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
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"go.uber.org/zap"
)

// Counts cover SQL submitted by restore materialization, apply and cleanup.
// Nested CTAS INSERTs, catalog/authorization SQL and transaction finish are
// outside this work event. It must not be interpreted as request success.
type branchRestoreWork struct {
	operation                  string
	tables                     int
	started                    time.Time
	counts                     map[string]int
	phases                     map[string]time.Duration
	emptyStages, unknownStages int
	materializedRows           int64
}

func newBranchRestoreWork(operation string, tables int) *branchRestoreWork {
	w := &branchRestoreWork{operation: operation, tables: tables, started: time.Now(),
		counts: make(map[string]int), phases: make(map[string]time.Duration)}
	for _, phase := range []string{"stage_create", "delete", "update", "insert", "stage_drop"} {
		w.counts[phase] = 0
	}
	return w
}

func (w *branchRestoreWork) exec(ctx context.Context, bh BackgroundExec, phase, sql string) error {
	w.counts[phase]++
	started := time.Now()
	defer func() { w.phases[phase] += time.Since(started) }()
	return bh.Exec(ctx, sql)
}

func (w *branchRestoreWork) finish(err *error) {
	failure := recover()
	status := "ok"
	if failure != nil {
		status = "panic"
	} else if *err != nil {
		status = "error"
	}
	total := time.Since(w.started)
	phases := make(map[string]float64)
	var measured time.Duration
	statements := 0
	for phase, count := range w.counts {
		statements += count
		measured += w.phases[phase]
		phases[phase] = float64(w.phases[phase]) / float64(time.Millisecond)
	}
	phases["other"] = float64(max(time.Duration(0), total-measured)) / float64(time.Millisecond)
	logutil.Info("DataBranch-Restore-Work", zap.Int("schema_version", 1),
		zap.String("request_id", uuid.NewString()), zap.String("operation", w.operation),
		zap.String("status", status), zap.String("staging", "temporary"), zap.Int("table_count", w.tables),
		zap.Int("sql_statements", statements), zap.Any("sql_counts", w.counts),
		zap.Int("empty_stages", w.emptyStages), zap.Int("unknown_stages", w.unknownStages),
		zap.Int64("materialized_rows", w.materializedRows),
		zap.Float64("total_ms", float64(total)/float64(time.Millisecond)), zap.Any("phases_ms", phases))
	if failure != nil {
		panic(failure)
	}
}

func executeBranchRestorePlans(ctx context.Context, bh BackgroundExec, plans []branchRestorePlan, operation string) (err error) {
	work := newBranchRestoreWork(operation, len(plans))
	defer work.finish(&err)
	var stages []string
	defer func() {
		err = errors.Join(err, work.dropStages(ctx, bh, stages))
	}()
	// Background temporary DDL participates in this executor's transaction.
	// Its aliases use the upstream session's transaction/statement journal,
	// including rollback after cancellation or a failed cleanup. DROP still
	// removes the physical stages synchronously, without public-table catalog
	// cleanup such as publication/view invalidation and role-privilege deletion.
	// Preserve the original order: materialize every delta before any target
	// write. Stage SELECTs also avoid same-table DELETE snapshot limitations.
	for i := range plans {
		p := &plans[i]
		for _, stage := range []struct {
			table, query string
			empty        *bool
		}{
			{p.rowsTable, p.rowsQuery, &p.rowsEmpty},
			{p.deletesTable, p.deletesQuery, &p.deletesEmpty},
		} {
			if stage.query == "" {
				continue
			}
			// Reset first: an executor that cannot report this CTAS must not
			// accidentally reuse the previous statement's affected-row count.
			provider, hasCount := bh.(backgroundExecRowCount)
			if hasCount {
				provider.SetLastAffectedRows(-1)
			}
			if err = work.exec(ctx, bh, "stage_create", "create temporary table "+stage.table+" as "+stage.query); err != nil {
				return err
			}
			stages = append(stages, stage.table)
			rows := int64(-1)
			if hasCount {
				rows = provider.GetLastAffectedRows()
			}
			*stage.empty = rows == 0
			if rows < 0 {
				work.unknownStages++
			} else {
				work.materializedRows += rows
				if rows == 0 {
					work.emptyStages++
				}
			}
		}
	}
	for _, p := range plans {
		for _, sql := range p.applySQL() {
			phase, _, _ := strings.Cut(sql, " ")
			if err = work.exec(ctx, bh, phase, sql); err != nil {
				return err
			}
		}
	}
	return nil
}

// Bound the SQL text even for restores with thousands of tables. CTAS stages
// have no foreign keys; grouping their drops preserves the owner transaction.
const branchRestoreDropBatchSize = 128

func (w *branchRestoreWork) dropStages(ctx context.Context, bh BackgroundExec, stages []string) (err error) {
	for end := len(stages); end > 0; {
		start := max(0, end-branchRestoreDropBatchSize)
		group := make([]string, 0, end-start)
		for i := end - 1; i >= start; i-- {
			group = append(group, stages[i])
		}
		dropErr := w.exec(ctx, bh, "stage_drop", "drop temporary table if exists "+strings.Join(group, ","))
		err = errors.Join(err, dropErr)
		if dropErr != nil && len(group) > 1 {
			// Retain the batch failure so the caller rolls back, but still try
			// every stage, as the former per-table cleanup did on failure.
			for _, stage := range group {
				err = errors.Join(err, w.exec(ctx, bh, "stage_drop", "drop temporary table if exists "+stage))
			}
		}
		end = start
	}
	return err
}
