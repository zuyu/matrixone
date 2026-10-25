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
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

type restoreWorkExec struct {
	BackgroundExec
	run func(context.Context, string) error
}

func (e *restoreWorkExec) Exec(ctx context.Context, sql string) error { return e.run(ctx, sql) }

type restoreWorkCountExec struct {
	*restoreWorkExec
	rows int64
}

func (e *restoreWorkCountExec) GetLastAffectedRows() int64  { return e.rows }
func (e *restoreWorkCountExec) SetLastAffectedRows(n int64) { e.rows = n }

func restoreWorkPlan(name string) branchRestorePlan {
	return branchRestorePlan{destination: name, columns: []string{"id", "v"}, keys: []string{"id"},
		rowsTable: name + "_r", deletesTable: name + "_d", rowsQuery: "select rows", deletesQuery: "select keys"}
}

func TestBranchRestoreEmptyStages(t *testing.T) {
	for _, tc := range []struct {
		name          string
		rows, deletes int64
		want          []string
	}{
		{"noop", 0, 0, []string{"create", "create", "drop"}},
		{"delete only", 0, 3, []string{"create", "create", "delete", "drop"}},
		{"rows only", 3, 0, []string{"create", "create", "update", "insert", "drop"}},
		{"mixed", 3, 2, []string{"create", "create", "delete", "update", "insert", "drop"}},
		{"unknown", -1, -1, []string{"create", "create", "delete", "update", "insert", "drop"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			back := &restoreWorkCountExec{restoreWorkExec: &restoreWorkExec{}}
			var kinds []string
			back.run = func(_ context.Context, sql string) error {
				kind, _, _ := strings.Cut(sql, " ")
				kinds = append(kinds, kind)
				if strings.HasPrefix(sql, "create temporary table") {
					require.Equal(t, int64(-1), back.rows, "reset stale counts before each CTAS")
					if strings.Contains(sql, "t_r") {
						back.rows = tc.rows
					} else {
						back.rows = tc.deletes
					}
				}
				return nil
			}
			require.NoError(t, executeBranchRestorePlans(t.Context(), back, []branchRestorePlan{restoreWorkPlan("t")}, "reset"))
			require.Equal(t, tc.want, kinds)
		})
	}
}

func TestBranchRestoreUnknownCountsKeepDML(t *testing.T) {
	for _, provider := range []bool{false, true} {
		t.Run(fmt.Sprint(provider), func(t *testing.T) {
			base := &restoreWorkExec{}
			var back BackgroundExec = base
			if provider {
				back = &restoreWorkCountExec{restoreWorkExec: base, rows: 0}
			}
			var sqls []string
			base.run = func(_ context.Context, sql string) error { sqls = append(sqls, sql); return nil }
			p := restoreWorkPlan("t")
			// Re-evaluation must replace old empty-stage flags, too.
			p.rowsEmpty, p.deletesEmpty = true, true
			require.NoError(t, executeBranchRestorePlans(t.Context(), back, []branchRestorePlan{p}, "reset"))
			require.Len(t, sqls, 6)
			require.True(t, strings.HasPrefix(sqls[2], "delete"))
			require.True(t, strings.HasPrefix(sqls[3], "update"))
			require.True(t, strings.HasPrefix(sqls[4], "insert"))
		})
	}
}

func TestBranchRestoreMaterializesBeforeWriting(t *testing.T) {
	var sqls []string
	back := &restoreWorkExec{run: func(_ context.Context, sql string) error { sqls = append(sqls, sql); return nil }}
	require.NoError(t, executeBranchRestorePlans(t.Context(), back,
		[]branchRestorePlan{restoreWorkPlan("a"), restoreWorkPlan("b")}, "revert"))
	require.Len(t, sqls, 11)
	for _, sql := range sqls[:4] {
		require.True(t, strings.HasPrefix(sql, "create temporary table"))
	}
	require.Equal(t, "drop temporary table if exists b_d,b_r,a_d,a_r", sqls[10])
}

func TestBranchRestoreKeylessAndKeyOnly(t *testing.T) {
	p := restoreWorkPlan("heap")
	p.keys = nil
	p.rowsEmpty = true
	require.Equal(t, []string{"delete from heap where true"}, p.applySQL(), "empty snapshot must still clear a heap")
	p.rowsEmpty = false
	require.Len(t, p.applySQL(), 2)
	p = restoreWorkPlan("pk")
	p.columns = p.keys
	sqls := p.applySQL()
	require.Len(t, sqls, 2)
	require.True(t, strings.HasPrefix(sqls[0], "delete"))
	require.True(t, strings.HasPrefix(sqls[1], "insert"))
}

func TestBranchRestoreCleanupBoundsAndFailures(t *testing.T) {
	for _, count := range []int{0, 1, 128, 129, 257} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			stages := make([]string, count)
			for i := range stages {
				stages[i] = fmt.Sprintf("s%d", i)
			}
			var dropped []string
			back := &restoreWorkExec{run: func(_ context.Context, sql string) error {
				group := strings.Split(strings.TrimPrefix(sql, "drop temporary table if exists "), ",")
				require.LessOrEqual(t, len(group), 128)
				dropped = append(dropped, group...)
				return nil
			}}
			work := newBranchRestoreWork("reset", 1)
			require.NoError(t, work.dropStages(t.Context(), back, stages))
			require.Equal(t, (count+127)/128, work.counts["stage_drop"])
			require.Len(t, dropped, count)
			for i, name := range dropped {
				require.Equal(t, stages[count-i-1], name)
			}
		})
	}
	batchErr, singleErr := errors.New("batch failed"), errors.New("single failed")
	var sqls []string
	back := &restoreWorkExec{run: func(_ context.Context, sql string) error {
		sqls = append(sqls, sql)
		if strings.Contains(sql, ",") {
			return batchErr
		}
		if strings.HasSuffix(sql, "b") {
			return singleErr
		}
		return nil
	}}
	err := newBranchRestoreWork("reset", 1).dropStages(t.Context(), back, []string{"a", "b"})
	require.ErrorIs(t, err, batchErr)
	require.ErrorIs(t, err, singleErr)
	require.Equal(t, []string{"drop temporary table if exists b,a", "drop temporary table if exists b", "drop temporary table if exists a"}, sqls)
}

func TestBranchRestoreFailurePreservesCleanupAndRollback(t *testing.T) {
	for _, failure := range []string{"create", "apply", "cancel", "panic", "cleanup"} {
		t.Run(failure, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			original, dropErr := errors.New("restore failed"), errors.New("cleanup failed")
			var dropped []string
			back := &restoreWorkExec{run: func(_ context.Context, sql string) error {
				if strings.HasPrefix(sql, "drop") {
					dropped = append(dropped, sql)
					return dropErr
				}
				if failure == "create" && strings.HasPrefix(sql, "create temporary table t_d") {
					return original
				}
				if strings.HasPrefix(sql, "delete") {
					switch failure {
					case "apply":
						return original
					case "cancel":
						cancel()
						return ctx.Err()
					case "panic":
						panic(original)
					}
				}
				return nil
			}}
			finished := false
			run := func() (err error) {
				defer finishDataBranchComponent(ctx, func(cause error) error {
					finished = true
					require.Error(t, cause, "transaction owner must roll back")
					return cause
				}, &err)
				return executeBranchRestorePlans(ctx, back, []branchRestorePlan{restoreWorkPlan("t")}, "reset")
			}
			if failure == "panic" {
				require.PanicsWithValue(t, original, func() { _ = run() })
			} else {
				err := run()
				require.ErrorIs(t, err, dropErr)
				if failure == "create" || failure == "apply" {
					require.ErrorIs(t, err, original)
				}
				if failure == "cancel" {
					require.ErrorIs(t, err, context.Canceled)
				}
			}
			require.True(t, finished)
			if failure == "create" {
				require.Equal(t, []string{"drop temporary table if exists t_r"}, dropped)
			} else {
				require.Equal(t, []string{"drop temporary table if exists t_d,t_r", "drop temporary table if exists t_d", "drop temporary table if exists t_r"}, dropped)
			}
		})
	}
}
