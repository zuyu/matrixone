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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestBranchRestoreDeleteSubqueryEligibility(t *testing.T) {
	for _, operation := range []string{"reset", "revert"} {
		for _, fk := range []string{"none", "parent", "child"} {
			t.Run(operation+"/"+fk, func(t *testing.T) {
				ep := func(name string) tree.TableName {
					return *tree.NewTableName(tree.Identifier(name), tree.ObjectNamePrefix{}, nil)
				}
				stmt := &tree.DataBranchDatabase{Operation: operation, Source: ep("src"), Destination: ep("dst"), Before: ep("before")}
				src, dst, before := restoreMetadataCatalog(200, 1), restoreMetadataCatalog(0, 2), restoreMetadataCatalog(100, 3)
				dst.snapshot = nil
				if fk == "parent" {
					dst.definitions["t"].RefChildTbls = []uint64{123}
				}
				if fk == "child" {
					dst.definitions["t"].Fkeys = []*plan.ForeignKeyDef{{Name: "fk"}}
				}
				p, err := planBranchRestore(stmt, "t", src, dst, before)
				require.NoError(t, err)
				require.Equal(t, operation == "revert" || fk == "none", p.deleteSubquery)
				require.Equal(t, operation == "revert" || fk == "none", p.rowsSubquery)
				if operation == "reset" && fk == "none" {
					require.Contains(t, p.updateQuery, "`src`.`t`{MO_TS = 200}")
					require.NotContains(t, p.updateQuery, "`dst`")
					require.Equal(t, branchRestoreDifferent("s", "d", p.columns, p.keys), p.updatePredicate)
				} else {
					require.Empty(t, p.updateQuery)
				}
			})
		}
	}
}

func TestBranchRestoreDeleteSubqueryKeepsDeltaSQL(t *testing.T) {
	p := restoreWorkPlan("`db`.`t`")
	p.keys = []string{"a`b", "u"}
	p.deleteSubquery = true
	p.deletesQuery = "select a.`a``b`,a.u from `db`.`t`{MO_TS = 200} a left join `db`.`t`{MO_TS = 100} b on a.u=b.u where b.u is null"
	p.deletesEmpty = true // A stale materialized-stage flag cannot skip a direct delete.
	want := "delete d from `db`.`t` d inner join (" + p.deletesQuery + ") s on d.`a``b`=s.`a``b` and d.`u`=s.`u`"
	require.Equal(t, want, p.applySQL()[0])
	require.NotContains(t, want, " in (")
}

func TestBranchRestoreSubqueryMaterializationAndCleanup(t *testing.T) {
	for _, rows := range []int64{0, 3, -1} {
		plans := []branchRestorePlan{restoreWorkPlan("a"), restoreWorkPlan("b")}
		for i := range plans {
			plans[i].deleteSubquery = true
		}
		back := &restoreWorkCountExec{restoreWorkExec: &restoreWorkExec{}}
		var sqls []string
		back.run = func(_ context.Context, sql string) error {
			sqls = append(sqls, sql)
			if strings.HasPrefix(sql, "create") {
				back.rows = rows
			}
			return nil
		}
		require.NoError(t, executeBranchRestorePlans(t.Context(), back, plans, "reset"))
		require.True(t, strings.HasPrefix(sqls[0], "create temporary table a_r"))
		require.True(t, strings.HasPrefix(sqls[1], "create temporary table b_r"))
		require.Equal(t, "drop temporary table if exists b_r,a_r", sqls[len(sqls)-1])
		deletes := 0
		for _, sql := range sqls {
			require.NotContains(t, sql, "table a_d")
			require.NotContains(t, sql, "table b_d")
			require.False(t, strings.HasPrefix(sql, "select"), "no separate delete probe")
			if strings.HasPrefix(sql, "delete") {
				deletes++
				require.Contains(t, sql, "inner join (select keys) s")
			}
		}
		require.Equal(t, 2, deletes, "empty row stages do not imply empty delete deltas")
	}
}

func TestBranchRestoreDeleteSubqueryFailureRollsBack(t *testing.T) {
	for _, failure := range []string{"apply", "cancel", "panic"} {
		t.Run(failure, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			original := errors.New("delete failed")
			var sqls []string
			back := &restoreWorkExec{run: func(_ context.Context, sql string) error {
				sqls = append(sqls, sql)
				if strings.HasPrefix(sql, "delete") {
					if failure == "cancel" {
						cancel()
						return ctx.Err()
					}
					if failure == "panic" {
						panic(original)
					}
					return original
				}
				return nil
			}}
			p := restoreWorkPlan("t")
			p.deleteSubquery = true
			finished := false
			run := func() (err error) {
				defer finishDataBranchComponent(ctx, func(cause error) error { finished = true; require.Error(t, cause); return cause }, &err)
				return executeBranchRestorePlans(ctx, back, []branchRestorePlan{p}, "reset")
			}
			if failure == "panic" {
				require.PanicsWithValue(t, original, func() { _ = run() })
			} else {
				err := run()
				if failure == "cancel" {
					require.ErrorIs(t, err, context.Canceled)
				} else {
					require.ErrorIs(t, err, original)
				}
			}
			require.True(t, finished)
			require.Equal(t, "drop temporary table if exists t_r", sqls[len(sqls)-1])
		})
	}
}

func TestBranchRestoreRowsSubqueryUsesNoStages(t *testing.T) {
	for _, keys := range [][]string{{"id"}, nil} {
		p := restoreWorkPlan("`db`.`t`")
		p.keys = keys
		p.rowsSubquery = true
		p.deleteSubquery = len(keys) > 0
		if len(keys) == 0 {
			p.deletesQuery = ""
		}
		p.rowsEmpty, p.deletesEmpty = true, true // stale materialized-stage flags
		var sqls []string
		back := &restoreWorkExec{run: func(_ context.Context, sql string) error { sqls = append(sqls, sql); return nil }}
		require.NoError(t, executeBranchRestorePlans(t.Context(), back, []branchRestorePlan{p}, "reset"))
		if len(keys) > 0 {
			require.Len(t, sqls, 3)
		} else {
			require.Len(t, sqls, 2)
			require.Equal(t, "delete from `db`.`t` where true", sqls[0])
		}
		for _, sql := range sqls {
			require.False(t, strings.HasPrefix(sql, "create") || strings.HasPrefix(sql, "drop") || strings.HasPrefix(sql, "select"))
			if strings.HasPrefix(sql, "update") || strings.HasPrefix(sql, "insert") {
				require.Contains(t, sql, "(select rows) s")
			}
		}
	}
	// A key-only table needs DELETE/INSERT, without an empty UPDATE.
	p := restoreWorkPlan("keys_only")
	p.columns = p.keys
	p.rowsSubquery = true
	p.deleteSubquery = true
	require.Len(t, p.applySQL(), 2)
}

func TestBranchRestoreMixedStagesPrecedeDirectWrites(t *testing.T) {
	plans := []branchRestorePlan{restoreWorkPlan("plain"), restoreWorkPlan("fk")}
	plans[0].rowsSubquery, plans[0].deleteSubquery = true, true
	var sqls []string
	back := &restoreWorkExec{run: func(_ context.Context, sql string) error { sqls = append(sqls, sql); return nil }}
	require.NoError(t, executeBranchRestorePlans(t.Context(), back, plans, "reset"))
	require.True(t, strings.HasPrefix(sqls[0], "create temporary table fk_r"))
	require.True(t, strings.HasPrefix(sqls[1], "create temporary table fk_d"))
	require.True(t, strings.HasPrefix(sqls[2], "delete d from plain"))
	require.Equal(t, "drop temporary table if exists fk_d,fk_r", sqls[len(sqls)-1])
	for _, sql := range sqls[2:] {
		require.False(t, strings.HasPrefix(sql, "create"))
	}
}

func TestBranchRestoreWithoutStagesStillRollsBack(t *testing.T) {
	for _, failure := range []string{"delete", "update", "insert", "cancel", "panic"} {
		t.Run(failure, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			original := errors.New("apply failed")
			back := &restoreWorkExec{run: func(_ context.Context, sql string) error {
				require.False(t, strings.HasPrefix(sql, "create") || strings.HasPrefix(sql, "drop"))
				if strings.HasPrefix(sql, failure) {
					return original
				}
				if strings.HasPrefix(sql, "update") {
					if failure == "cancel" {
						cancel()
						return ctx.Err()
					}
					if failure == "panic" {
						panic(original)
					}
				}
				return nil
			}}
			p := restoreWorkPlan("t")
			p.rowsSubquery, p.deleteSubquery = true, true
			finished := false
			run := func() (err error) {
				defer finishDataBranchComponent(ctx, func(cause error) error { finished = true; require.Error(t, cause); return cause }, &err)
				return executeBranchRestorePlans(ctx, back, []branchRestorePlan{p}, "revert")
			}
			if failure == "panic" {
				require.PanicsWithValue(t, original, func() { _ = run() })
			} else {
				err := run()
				if failure == "cancel" {
					require.ErrorIs(t, err, context.Canceled)
				} else {
					require.ErrorIs(t, err, original)
				}
			}
			require.True(t, finished)
		})
	}
}

func TestBranchRestoreUpdateSkipsIdenticalCurrentRows(t *testing.T) {
	p := restoreWorkPlan("`db`.`t`")
	p.columns = []string{"tenant", "k`ey", "v`alue"}
	p.keys = []string{"tenant", "k`ey"}
	p.rowsSubquery, p.deleteSubquery = true, true
	p.updateQuery = "select * from `db`.`t`{MO_TS = 100}"
	p.updatePredicate = branchRestoreDifferent("d", "s", p.columns, p.keys)
	update := p.applySQL()[1]
	require.Contains(t, update, "inner join ("+p.updateQuery+") s")
	require.Equal(t, 2, strings.Count(update, "`db`.`t`"), "one current scan and one historical scan")
	require.Contains(t, update, "on d.`tenant`=s.`tenant` and d.`k``ey`=s.`k``ey`")
	require.Contains(t, update, "set d.`v``alue`=s.`v``alue` where (not (d.`v``alue` <=> s.`v``alue`))")
	// REVERT retains its before/after delta but filters current equality too.
	p.updateQuery = ""
	update = p.applySQL()[1]
	require.Contains(t, update, "inner join ("+p.rowsQuery+") s")
	require.Contains(t, update, " where (not (d.`v``alue` <=> s.`v``alue`))")
	// FK-related RESET still applies the materialized pre-cascade delta.
	p.rowsSubquery = false
	require.NotContains(t, p.applySQL()[1], " where ")
}

func TestBranchRestoreUpdateEqualityNeedsExactTypes(t *testing.T) {
	for _, typ := range []types.T{types.T_int32, types.T_decimal128, types.T_datetime,
		types.T_varchar, types.T_float64, types.T_json} {
		dst, before := restoreMetadataCatalog(0, 1).definitions["t"], restoreMetadataCatalog(1, 1).definitions["t"]
		dst.Cols[1].Typ = plan.Type{Id: int32(typ), Width: 20, Scale: 2}
		before.Cols[1].Typ = dst.Cols[1].Typ
		want := typ == types.T_int32 || typ == types.T_decimal128 || typ == types.T_datetime
		require.Equal(t, want, branchRestoreUpdateEqualitySafe(dst, before, []string{"id"}), typ.String())
		before.Cols[1].Typ.Scale++
		require.False(t, branchRestoreUpdateEqualitySafe(dst, before, []string{"id"}))
		before.Cols[1].Typ = dst.Cols[1].Typ
		before.Cols[1].Typ.Id = int32(types.T_int64)
		require.False(t, branchRestoreUpdateEqualitySafe(dst, before, []string{"id"}))
	}
}
