// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
	"context"
	"sort"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
)

type branchDatabaseResult struct {
	table                           string
	sourceExists, destinationExists bool
	summary                         *dataBranchMergeSummary
}

// One owner transaction spans enumeration, authorization and every table.
// Table workers must never acquire/commit a separate background transaction.
func handleBranchDatabase(execCtx *ExecCtx, ses *Session, stmt *tree.DataBranchDatabase) (err error) {
	if stmt.Source.SchemaName != "" || stmt.Destination.SchemaName != "" {
		return moerr.NewInvalidInputNoCtx("DATA BRANCH DATABASE endpoints must be database names")
	}
	if len(stmt.Tables) > 0 && len(stmt.IgnoreTables) > 0 {
		return moerr.NewInvalidInputNoCtx("TABLES and IGNORE TABLES cannot be combined")
	}
	if stmt.Operation != "diff" {
		if dataBranchMergeTxnNotAllowed(ses) {
			return moerr.NewInternalError(execCtx.reqCtx, dataBranchMergePickTxnErrorInfo())
		}
		if stmt.Destination.AtTsExpr != nil {
			return moerr.NewInvalidInputNoCtx("DATA BRANCH DATABASE destination must be current")
		}
		if stmt.Source.ObjectName == stmt.Destination.ObjectName {
			return moerr.NewInvalidInputNoCtx("cannot merge a database into itself")
		}
	}
	bh, finish, err := getDataBranchMutationExecutor(execCtx.reqCtx, ses, false,
		&BackgroundExecOption{forcePessimisticRC: true})
	if err != nil {
		return err
	}
	defer finishDataBranchComponent(execCtx.reqCtx, finish, &err)
	ctx := execCtx.reqCtx
	source, err := branchDatabaseTables(ctx, ses, bh, stmt.Source)
	if err != nil {
		return err
	}
	destination, err := branchDatabaseTables(ctx, ses, bh, stmt.Destination)
	if err != nil {
		return err
	}
	names, err := selectBranchDatabaseTables(stmt, source, destination)
	if err != nil {
		return err
	}
	// Authorize the complete set before the first write. Historical endpoints
	// use the same snapshot-aware read authorization as table-level DIFF.
	for _, name := range names {
		src, dst := branchDatabaseTable(stmt.Source, name), branchDatabaseTable(stmt.Destination, name)
		if stmt.Operation == "merge" && (!source[name] || !destination[name]) {
			return moerr.NewInvalidInputNoCtxf("database merge requires table %q on both sides; reconcile schema or select TABLES first", name)
		}
		if !skipDataBranchPrivilegeCheck(ses) {
			if stmt.Operation == "merge" {
				if _, err = authenticateDataBranchReadWriteTables(ctx, ses, src, dst); err != nil {
					return err
				}
			} else {
				if source[name] {
					if _, err = requireBranchReadOnTableName(ctx, ses, src); err != nil {
						return err
					}
				}
				if destination[name] {
					if _, err = requireBranchReadOnTableName(ctx, ses, dst); err != nil {
						return err
					}
				}
			}
		}
	}
	results := make([]branchDatabaseResult, 0, len(names))
	for _, name := range names {
		if err = ctx.Err(); err != nil {
			return err
		}
		summary := newDataBranchMergeSummary()
		src, dst := branchDatabaseTable(stmt.Source, name), branchDatabaseTable(stmt.Destination, name)
		switch {
		case !source[name]:
			summary.destination[0], err = branchDatabaseRowCount(ctx, bh, dst)
		case !destination[name]:
			summary.source[0], err = branchDatabaseRowCount(ctx, bh, src)
		case stmt.Operation == "diff":
			diff := &tree.DataBranchDiff{TargetTable: src, BaseTable: dst}
			err = diffMergeWithExecutor(ses, execCtx, diff, bh, summary)
		default:
			policy := stmt.ConflictOpt
			if policy == nil {
				policy = &tree.ConflictOpt{Opt: tree.CONFLICT_FAIL}
			}
			merge := &tree.DataBranchMerge{SrcTable: src, DstTable: dst, ConflictOpt: policy, OutputSummary: true}
			err = diffMergeWithExecutor(ses, execCtx, merge, bh, summary)
		}
		summary.releaseKeys()
		if err != nil {
			return err
		}
		results = append(results, branchDatabaseResult{name, source[name], destination[name], summary})
	}
	return outputBranchDatabaseSummary(ctx, ses, results)
}

func branchDatabaseTable(database tree.TableName, name string) tree.TableName {
	return newQualifiedCloneTableName(database.ObjectName.String(), name, database.AtTsExpr)
}

func branchDatabaseTables(ctx context.Context, ses *Session, bh BackgroundExec, endpoint tree.TableName) (map[string]bool, error) {
	snapshot, err := resolveSnapshot(ses, endpoint.AtTsExpr)
	if err != nil {
		return nil, err
	}
	op := backgroundExecTxnOperator(bh)
	if snapshot != nil && snapshot.TS != nil {
		op = op.CloneSnapshotOp(*snapshot.TS)
	}
	name := endpoint.ObjectName.String()
	db, err := ses.proc.GetSessionInfo().StorageEngine.Database(ctx, name, op)
	if err != nil {
		return nil, err
	}
	if endpoint.AtTsExpr != nil && endpoint.AtTsExpr.Type == tree.ATTIMESTAMPSNAPSHOT {
		id, err := strconv.ParseUint(db.GetDatabaseId(ctx), 10, 64)
		if err != nil {
			return nil, err
		}
		if err = plan2.ValidateSnapshotDatabaseScope(snapshot, name, id); err != nil {
			return nil, err
		}
	}
	names, err := db.Relations(ctx)
	if err != nil {
		return nil, err
	}
	result := make(map[string]bool, len(names))
	for _, table := range names {
		if strings.HasPrefix(table, catalog.PrefixIndexTableName) {
			continue
		}
		rel, err := db.Relation(ctx, table, nil)
		if err != nil {
			return nil, err
		}
		def := rel.GetTableDef(ctx)
		if def.TableType == catalog.SystemOrdinaryRel || def.TableType == "" {
			result[table] = true
		}
	}
	return result, nil
}

func selectBranchDatabaseTables(stmt *tree.DataBranchDatabase, source, destination map[string]bool) ([]string, error) {
	selected := make(map[string]bool)
	if len(stmt.Tables) > 0 {
		for _, table := range stmt.Tables {
			name := table.String()
			if !source[name] && !destination[name] {
				return nil, moerr.NewInvalidInputNoCtxf("unknown data branch table %q", name)
			}
			if selected[name] {
				return nil, moerr.NewInvalidInputNoCtxf("duplicate data branch table %q", name)
			}
			selected[name] = true
		}
	} else {
		for name := range source {
			selected[name] = true
		}
		for name := range destination {
			selected[name] = true
		}
		for _, table := range stmt.IgnoreTables {
			delete(selected, table.String())
		}
	}
	names := make([]string, 0, len(selected))
	for name := range selected {
		names = append(names, name)
	}
	sort.Strings(names)
	return names, nil
}

func branchDatabaseRowCount(ctx context.Context, bh BackgroundExec, table tree.TableName) (uint64, error) {
	bh.ClearExecResultSet()
	sql := "select count(*) from " + tree.StringWithOpts(&table, dialect.MYSQL, tree.WithQuoteIdentifier(), tree.WithSingleQuoteString())
	if err := bh.Exec(ctx, sql); err != nil {
		return 0, err
	}
	results, err := getResultSet(ctx, bh)
	if err != nil {
		return 0, err
	}
	if !execResultArrayHasData(results) {
		return 0, moerr.NewInternalErrorNoCtx("missing database diff count")
	}
	return results[0].GetUint64(ctx, 0, 0)
}

func outputBranchDatabaseSummary(ctx context.Context, ses *Session, results []branchDatabaseResult) error {
	ses.ClearAllMysqlResultSet()
	ses.ClearResultBatches()
	mrs := ses.GetMysqlResultSet()
	for i, name := range []string{"table_name", "source_exists", "destination_exists",
		"source_inserted", "source_deleted", "source_updated", "destination_inserted", "destination_deleted", "destination_updated", "conflicts"} {
		col := new(MysqlColumn)
		col.SetName(name)
		if i == 0 {
			col.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
		} else {
			col.SetColumnType(defines.MYSQL_TYPE_LONGLONG)
		}
		mrs.AddColumn(col)
	}
	for _, result := range results {
		s, a, b := result.summary, uint64(0), uint64(0)
		if result.sourceExists {
			a = 1
		}
		if result.destinationExists {
			b = 1
		}
		mrs.AddRow([]any{result.table, a, b, s.source[0], s.source[1], s.source[2], s.destination[0], s.destination[1], s.destination[2], s.conflicts})
	}
	return trySaveQueryResult(ctx, ses, mrs)
}
