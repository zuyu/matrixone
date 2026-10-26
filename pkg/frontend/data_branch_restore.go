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
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"go.uber.org/zap"
)

type branchRestorePlan struct {
	destination             string
	columns, keys           []string
	rowsTable, deletesTable string
	rowsQuery, deletesQuery string
	insertQuery             string
	rowsEmpty, deletesEmpty bool
	deleteSubquery          bool
	rowsSubquery            bool
}

// RESET restores data without replacing relation identities. REVERT applies
// the inverse of after-vs-before to current rows, leaving other keys alone.
// Staging and apply remain inside one owner transaction and never return rows
// or primary keys to the client.
func handleBranchRestore(execCtx *ExecCtx, ses *Session, stmt *tree.DataBranchDatabase) (err error) {
	ctx := execCtx.reqCtx
	if dataBranchMergeTxnNotAllowed(ses) {
		return moerr.NewInternalError(ctx, dataBranchMergePickTxnErrorInfo())
	}
	if stmt.Source.SchemaName != "" || stmt.Destination.SchemaName != "" || stmt.Before.SchemaName != "" {
		return moerr.NewInvalidInputNoCtx("DATA BRANCH DATABASE endpoints must be database names")
	}
	if len(stmt.Tables) > 0 && len(stmt.IgnoreTables) > 0 {
		return moerr.NewInvalidInputNoCtx("TABLES and IGNORE TABLES cannot be combined")
	}
	if stmt.Destination.AtTsExpr != nil || stmt.Source.AtTsExpr == nil ||
		(stmt.Operation == "revert" && stmt.Before.AtTsExpr == nil) {
		return moerr.NewInvalidInputNoCtx("RESET/REVERT require historical source endpoints and a current destination")
	}
	// These are in-place DML operations, not bulk disaster-recovery replay.
	// MO_TS reads must retain normal statistics, duplicate checks and write
	// planning instead of triggering the background SQL restore heuristic.
	bh, finish, err := getDataBranchMutationExecutor(ctx, ses, false,
		&BackgroundExecOption{forcePessimisticRC: true, disableImplicitRestore: true})
	if err != nil {
		return err
	}
	defer finishDataBranchComponent(ctx, finish, &err)
	metadataStarted := time.Now()
	sourceCatalog, err := loadBranchDatabaseCatalog(ctx, ses, bh, stmt.Source, true)
	if err != nil {
		return err
	}
	destinationCatalog, err := loadBranchDatabaseCatalog(ctx, ses, bh, stmt.Destination, true)
	if err != nil {
		return err
	}
	beforeCatalog := sourceCatalog
	if stmt.Operation == "revert" {
		beforeCatalog, err = loadBranchDatabaseCatalog(ctx, ses, bh, stmt.Before, true)
		if err != nil {
			return err
		}
	}
	// These definitions belong only to this statement's planning phase;
	// no target writes have happened and nothing is retained on the session.
	source, destination, before := sourceCatalog.tables, destinationCatalog.tables, beforeCatalog.tables
	endpoints, snapshots := 2, 1
	relationOpens := sourceCatalog.relationOpens + destinationCatalog.relationOpens
	if stmt.Operation == "revert" {
		endpoints, snapshots = 3, 2
		relationOpens += beforeCatalog.relationOpens
	}
	logutil.Info("DataBranch-Restore-Metadata", zap.Int("schema_version", 1),
		zap.String("operation", stmt.Operation), zap.Int("database_opens", endpoints),
		zap.Int("snapshot_resolutions", snapshots), zap.Int("relation_opens", relationOpens),
		zap.Float64("catalog_ms", float64(time.Since(metadataStarted))/float64(time.Millisecond)))
	names, err := selectBranchDatabaseTables(stmt, source, destination)
	if err != nil {
		return err
	}
	if stmt.Operation == "revert" && len(stmt.Tables) == 0 {
		// Include before-only tables in validation; never silently ignore a
		// table deletion that this data-only operation cannot restore.
		union := make(map[string]bool)
		for name := range source {
			union[name] = true
		}
		for name := range before {
			union[name] = true
		}
		names, err = selectBranchDatabaseTables(stmt, union, destination)
		if err != nil {
			return err
		}
	}
	plans := make([]branchRestorePlan, 0, len(names))
	for _, name := range names {
		if !source[name] || !destination[name] || !before[name] {
			return moerr.NewInvalidInputNoCtxf("database %s requires table %q at every endpoint; reconcile schema or select TABLES first", stmt.Operation, name)
		}
		src, dst := branchDatabaseTable(stmt.Source, name), branchDatabaseTable(stmt.Destination, name)
		if !skipDataBranchPrivilegeCheck(ses) {
			if _, err = authenticateDataBranchReadWriteTables(ctx, ses, src, dst); err != nil {
				return err
			}
			if stmt.Operation == "revert" {
				if _, err = requireBranchReadOnTableName(ctx, ses, branchDatabaseTable(stmt.Before, name)); err != nil {
					return err
				}
			}
		}
		p, err := planBranchRestore(stmt, name, sourceCatalog, destinationCatalog, beforeCatalog)
		if err != nil {
			return err
		}
		plans = append(plans, p)
	}
	return executeBranchRestorePlans(ctx, bh, plans, stmt.Operation)
}

func branchRestoreTableSQL(table tree.TableName) string {
	return tree.StringWithOpts(&table, dialect.MYSQL, tree.WithQuoteIdentifier(), tree.WithSingleQuoteString())
}

func branchRestoreColumns(alias string, names []string) string {
	parts := make([]string, len(names))
	for i, name := range names {
		parts[i] = alias + "." + quoteIdentifierForSQL(name)
	}
	return strings.Join(parts, ",")
}

func branchRestoreJoin(left, right string, keys []string) string {
	parts := make([]string, len(keys))
	for i, name := range keys {
		q := quoteIdentifierForSQL(name)
		parts[i] = left + "." + q + "=" + right + "." + q
	}
	return strings.Join(parts, " and ")
}

func branchRestoreDifferent(left, right string, columns, keys []string) string {
	var parts []string
	for _, name := range columns {
		if slices.Contains(keys, name) {
			continue
		}
		q := quoteIdentifierForSQL(name)
		parts = append(parts, "not ("+left+"."+q+" <=> "+right+"."+q+")")
	}
	if len(parts) == 0 {
		return "false"
	}
	return "(" + strings.Join(parts, " or ") + ")"
}

func branchRestoreWritableColumns(def *plan.TableDef) []string {
	var names []string
	for _, col := range def.Cols {
		if isDataBranchWritableColumn(col) {
			names = append(names, col.Name)
		}
	}
	return names
}

// Validate selected definitions even though database scope was checked while
// loading the catalog. Timestamp-only hints have no named-object restriction.
func (c *branchDatabaseCatalog) restoreTable(endpoint tree.TableName, table string) (*plan.TableDef, error) {
	def := c.definitions[table]
	if def == nil {
		return nil, moerr.NewInvalidInputNoCtxf("missing restore definition for table %q", table)
	}
	if endpoint.AtTsExpr != nil && endpoint.AtTsExpr.Type == tree.ATTIMESTAMPSNAPSHOT {
		if err := plan2.ValidateSnapshotScope(c.snapshot, endpoint.ObjectName.String(), table,
			def.DbId, plan2.SnapshotTableID(def)); err != nil {
			return nil, err
		}
	}
	return def, nil
}

func planBranchRestore(stmt *tree.DataBranchDatabase, table string, source, destination, before *branchDatabaseCatalog) (branchRestorePlan, error) {
	p := branchRestorePlan{}
	srcName, dstName := branchDatabaseTable(stmt.Source, table), branchDatabaseTable(stmt.Destination, table)
	srcDef, err := source.restoreTable(stmt.Source, table)
	if err != nil {
		return p, err
	}
	dstDef, err := destination.restoreTable(stmt.Destination, table)
	if err != nil {
		return p, err
	}
	// The catalog resolved the named snapshot and validated its scope. Use
	// its timestamp for background DML and any remaining CTAS stages.
	if snapshot := source.snapshot; snapshot != nil && snapshot.TS != nil {
		srcName.AtTsExpr = newMoTimestampHint(snapshot.TS.PhysicalTime)
	}

	dstKind, dstKeys := dataBranchPrimaryKeyColumns(dstDef)
	srcKind, srcKeys := dataBranchPrimaryKeyColumns(srcDef)
	if dstKind != srcKind || !slices.Equal(dstKeys, srcKeys) {
		return p, moerr.NewInvalidInputNoCtxf("database %s primary keys differ for table %q", stmt.Operation, table)
	}
	p.columns = branchRestoreWritableColumns(dstDef)
	checkColumns := func(def *plan.TableDef) error {
		names := branchRestoreWritableColumns(def)
		if len(names) != len(p.columns) {
			return moerr.NewInvalidInputNoCtxf("database %s requires matching writable columns for table %q", stmt.Operation, table)
		}
		for _, name := range p.columns {
			if !slices.Contains(names, name) {
				return moerr.NewInvalidInputNoCtxf("database %s requires matching writable columns for table %q", stmt.Operation, table)
			}
		}
		return nil
	}
	if err = checkColumns(srcDef); err != nil {
		return p, err
	}
	if len(p.columns) == 0 {
		return p, moerr.NewNotSupportedNoCtxf("table %q has no writable columns", table)
	}
	// REVERT reads an immutable before/after delta. A RESET without FK
	// dependencies can re-evaluate its own current-vs-snapshot delta: DELETE
	// removes only non-snapshot keys, UPDATE restores matching rows, and the
	// following INSERT fills the remaining missing keys. FK-related RESETs
	// retain pre-write materialization across earlier table cascades.
	p.rowsSubquery = stmt.Operation == "revert" || (len(dstDef.Fkeys) == 0 && len(dstDef.RefChildTbls) == 0)
	p.destination = branchRestoreTableSQL(dstName)
	id := strings.ReplaceAll(uuid.NewString(), "-", "")
	p.rowsTable = qualifiedTableName(stmt.Destination.ObjectName.String(), "__mo_branch_restore_"+id+"_r")
	p.deletesTable = qualifiedTableName(stmt.Destination.ObjectName.String(), "__mo_branch_restore_"+id+"_d")
	if dstKind == fakeKind {
		if stmt.Operation == "revert" {
			return p, moerr.NewNotSupportedNoCtxf("database REVERT requires a primary key on table %q", table)
		}
		p.rowsQuery = "select " + branchRestoreColumns("s", p.columns) + " from " + branchRestoreTableSQL(srcName) + " s"
		return p, nil
	}
	p.keys = dstKeys
	p.deleteSubquery = p.rowsSubquery
	firstKey := quoteIdentifierForSQL(p.keys[0])
	if stmt.Operation == "reset" {
		source := branchRestoreTableSQL(srcName)
		join := branchRestoreJoin("s", "d", p.keys)
		p.rowsQuery = fmt.Sprintf("select %s from %s s left join %s d on %s where d.%s is null or %s",
			branchRestoreColumns("s", p.columns), source, p.destination, join, firstKey, branchRestoreDifferent("s", "d", p.columns, p.keys))
		p.deletesQuery = fmt.Sprintf("select %s from %s d left join %s s on %s where s.%s is null",
			branchRestoreColumns("d", p.keys), p.destination, source, join, firstKey)
		if p.rowsSubquery {
			// INSERT only needs missing keys. UPDATE has already restored
			// existing rows, so do not compute the row delta and join the
			// current target a second time.
			p.insertQuery = fmt.Sprintf("select %s from %s s left join %s d on %s where d.%s is null",
				branchRestoreColumns("s", p.columns), source, p.destination, join, firstKey)
		}
	} else {
		beforeName := branchDatabaseTable(stmt.Before, table)
		beforeDef, err := before.restoreTable(stmt.Before, table)
		if err != nil {
			return p, err
		}
		if snapshot := before.snapshot; snapshot != nil && snapshot.TS != nil {
			beforeName.AtTsExpr = newMoTimestampHint(snapshot.TS.PhysicalTime)
		}
		kind, keys := dataBranchPrimaryKeyColumns(beforeDef)
		if kind != dstKind || !slices.Equal(keys, p.keys) {
			return p, moerr.NewInvalidInputNoCtxf("database REVERT primary keys differ for table %q", table)
		}
		if err = checkColumns(beforeDef); err != nil {
			return p, err
		}
		afterSQL, beforeSQL := branchRestoreTableSQL(srcName), branchRestoreTableSQL(beforeName)
		join := branchRestoreJoin("a", "b", p.keys)
		p.rowsQuery = fmt.Sprintf("select %s from %s b left join %s a on %s where a.%s is null or %s",
			branchRestoreColumns("b", p.columns), beforeSQL, afterSQL, join, firstKey, branchRestoreDifferent("a", "b", p.columns, p.keys))
		p.deletesQuery = fmt.Sprintf("select %s from %s a left join %s b on %s where b.%s is null",
			branchRestoreColumns("a", p.keys), afterSQL, beforeSQL, join, firstKey)
		// Restrict INSERT to before-images absent from the current target
		// before comparing snapshots. Unchanged rows deleted after the
		// reverted commit must still be excluded by the after-image check.
		missing := fmt.Sprintf("select %s from %s b left join %s d on %s where d.%s is null",
			branchRestoreColumns("b", p.columns), beforeSQL, p.destination, branchRestoreJoin("b", "d", p.keys), firstKey)
		p.insertQuery = fmt.Sprintf("select %s from (%s) s left join %s a on %s where a.%s is null or %s",
			branchRestoreColumns("s", p.columns), missing, afterSQL, branchRestoreJoin("s", "a", p.keys), firstKey,
			branchRestoreDifferent("s", "a", p.columns, p.keys))
	}
	return p, nil
}

func (p branchRestorePlan) applySQL() []string {
	columns := joinQuotedColumnNames(p.columns)
	rowsSource := p.rowsTable
	if p.rowsSubquery {
		rowsSource = "(" + p.rowsQuery + ")"
	}
	if len(p.keys) == 0 {
		// A WHERE clause keeps this as row DML; an unrestricted DELETE is
		// optimized into TRUNCATE, which replaces identity and breaks lineage.
		sqls := []string{"delete from " + p.destination + " where true"}
		if p.rowsSubquery || !p.rowsEmpty {
			sqls = append(sqls, fmt.Sprintf("insert into %s (%s) select %s from %s s", p.destination, columns, branchRestoreColumns("s", p.columns), rowsSource))
		}
		return sqls
	}
	join := branchRestoreJoin("d", "s", p.keys)
	var sqls []string
	if p.deleteSubquery {
		// The derived-table boundary permits reading the target's historical
		// and current views in DELETE without MySQL's direct-target-subquery
		// restriction. Keep the delta in the SQL plan; no literal key lists.
		sqls = append(sqls, fmt.Sprintf("delete d from %s d inner join (%s) s on %s", p.destination, p.deletesQuery, join))
	} else if !p.deletesEmpty {
		sqls = append(sqls, fmt.Sprintf("delete d from %s d inner join %s s on %s", p.destination, p.deletesTable, join))
	}
	if p.rowsEmpty && !p.rowsSubquery {
		return sqls
	}
	var assignments []string
	for _, name := range p.columns {
		if slices.Contains(p.keys, name) {
			continue
		}
		q := quoteIdentifierForSQL(name)
		assignments = append(assignments, "d."+q+"=s."+q)
	}
	if len(assignments) > 0 {
		sqls = append(sqls, fmt.Sprintf("update %s d inner join %s s on %s set %s", p.destination, rowsSource, join, strings.Join(assignments, ",")))
	}
	if p.rowsSubquery && p.insertQuery != "" {
		sqls = append(sqls, fmt.Sprintf("insert into %s (%s) %s", p.destination, columns, p.insertQuery))
	} else {
		sqls = append(sqls, fmt.Sprintf("insert into %s (%s) select %s from %s s left join %s d on %s where d.%s is null",
			p.destination, columns, branchRestoreColumns("s", p.columns), rowsSource, p.destination, join, quoteIdentifierForSQL(p.keys[0])))
	}
	return sqls
}
