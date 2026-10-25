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
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/stretchr/testify/require"
)

func TestOnlineMergeCreatesOnlyConsumedStages(t *testing.T) {
	insert := batchWithKind{kind: diffInsert}
	del := batchWithKind{kind: diffDelete}
	update := batchWithKind{kind: diffInsert, fromUpdate: true}
	oldUpdate := batchWithKind{kind: diffDelete, fromUpdate: true}
	native := batchWithKind{kind: diffInsert, fromUpdate: true, requiresNativeUpdate: true}
	restore := batchWithKind{kind: diffInsert, fromUpdate: true, restoreMissing: true}
	for _, tc := range []struct {
		name      string
		ops       []batchWithKind
		want      []string
		fakePK    bool
		integer   bool
		rowsPerOp []int
		extraCols int
		failure   string
	}{
		{name: "empty"},
		{name: "inline_cells_limit", integer: true, extraCols: 2, ops: []batchWithKind{update}, rowsPerOp: []int{102}},
		{name: "inline_cells_above_limit", integer: true, extraCols: 2, ops: []batchWithKind{update}, rowsPerOp: []int{103}, want: []string{"upd"}},
		{name: "inline_limit", integer: true, ops: []batchWithKind{update}, rowsPerOp: []int{128}},
		{name: "inline_above_limit", integer: true, ops: []batchWithKind{update}, rowsPerOp: []int{129}, want: []string{"upd"}},
		{name: "inline_between_staged", integer: true, ops: []batchWithKind{update, insert, update, insert, update}, rowsPerOp: []int{129, 1, 1, 1, 129}, want: []string{"upd"}},
		{name: "inline_update", integer: true, ops: []batchWithKind{oldUpdate, update}},
		{name: "inline_repeated", integer: true, ops: []batchWithKind{update, insert, update}},
		{name: "inline_failure", integer: true, ops: []batchWithKind{update}, failure: "apply"},
		{name: "inline_cancel", integer: true, ops: []batchWithKind{update, insert}, failure: "cancel"},
		{name: "ignored_update_delete", ops: []batchWithKind{oldUpdate}},
		{name: "insert", ops: []batchWithKind{insert}},
		{name: "delete", ops: []batchWithKind{del}, want: []string{"del"}},
		{name: "update", ops: []batchWithKind{oldUpdate, update}, want: []string{"upd"}},
		{name: "native_update", ops: []batchWithKind{oldUpdate, native}},
		{name: "restore_deleted_target", ops: []batchWithKind{oldUpdate, restore}},
		{name: "repeated_flush", ops: []batchWithKind{insert, update, del, insert, update, del}, want: []string{"upd", "del"}},
		{name: "clear_update_failure", ops: []batchWithKind{update, insert, update}, want: []string{"upd"}, failure: "clear"},
		{name: "clear_delete_failure", ops: []batchWithKind{del, update, del}, want: []string{"del", "upd"}, failure: "clear"},
		{name: "keyless_insert", ops: []batchWithKind{insert}, fakePK: true},
		{name: "keyless_update", ops: []batchWithKind{oldUpdate, update}, want: []string{"del"}, fakePK: true},
		{name: "first_create_failure", ops: []batchWithKind{insert, update}, want: []string{"upd"}, failure: "create_first"},
		{name: "second_create_failure", ops: []batchWithKind{update, del}, want: []string{"upd", "del"}, failure: "create_second"},
		{name: "apply_failure", ops: []batchWithKind{insert, update}, failure: "apply"},
		{name: "cleanup_failure", ops: []batchWithKind{update}, want: []string{"upd"}, failure: "cleanup"},
		{name: "preserve_apply_error", ops: []batchWithKind{update, insert}, want: []string{"upd"}, failure: "apply_and_cleanup"},
		{name: "panic_after_flush", ops: []batchWithKind{update, insert}, want: []string{"upd"}, failure: "panic"},
		{name: "cancel_after_flush", ops: []batchWithKind{update, insert}, want: []string{"upd"}, failure: "cancel"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ses := newValidateSession(t)
			ctrl := gomock.NewController(t)
			tbl := newTestBranchTableStuff(ctrl)
			if tc.integer {
				tbl.def.colTypes[1] = types.T_int64.ToType()
			}
			for i := 0; i < tc.extraCols; i++ {
				idx := len(tbl.def.colNames)
				tbl.def.colNames = append(tbl.def.colNames, fmt.Sprintf("extra%d", i))
				tbl.def.colTypes = append(tbl.def.colTypes, types.T_int64.ToType())
				tbl.def.visibleIdxes = append(tbl.def.visibleIdxes, idx)
				tbl.def.writableIdxes = append(tbl.def.writableIdxes, idx)
				tbl.def.commonVisibleIdxes = append(tbl.def.commonVisibleIdxes, idx)
				tbl.def.commonWritableIdxes = append(tbl.def.commonWritableIdxes, idx)
			}
			if tc.fakePK {
				tbl = newFakePKBranchTableStuff(ctrl)
				tbl.retPool = &retBatchList{}
			}
			tbl.def.baseColNames = append([]string(nil), tbl.def.colNames...)
			defer tbl.retPool.freeAllRetBatches(ses.proc.Mp())
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			injected := errors.New("injected " + tc.failure)
			cleanupErr := errors.New("cleanup error")
			created, dropped, loads := map[string]int{}, map[string]int{}, map[string]int{}
			ready, dirty := map[string]bool{}, map[string]bool{}
			clears := map[string]int{}
			stageName := regexp.MustCompile("__mo_diff_(del|ins|upd)_[^` ]+")
			createCalls := 0
			bh := mock_frontend.NewMockBackgroundExec(ctrl)
			bh.EXPECT().Exec(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, sql string) error {
				match := stageName.FindStringSubmatch(sql)
				if match == nil {
					if strings.HasPrefix(sql, "update ") && strings.Contains(sql, "join (values ") {
						loads["inline"]++
						if tc.failure == "apply" {
							return injected
						}
					}
					if strings.HasPrefix(sql, "insert into") {
						loads["direct"]++
						if strings.HasPrefix(tc.failure, "apply") {
							return injected
						}
					}
					return nil
				} // direct row SQL uses no stage
				table, kind := match[0], match[1]
				switch {
				case strings.HasPrefix(sql, "create table"):
					createCalls++
					created[kind]++
					require.False(t, ready[table], "a stage must not be recreated across flushes")
					if tc.failure == "create_first" || tc.failure == "create_second" && createCalls == 2 {
						return injected
					}
					ready[table] = true
				case strings.HasPrefix(sql, "drop table"):
					dropped[kind]++
					delete(ready, table)
					delete(dirty, table)
					if dropped[kind] == 2 {
						if tc.failure == "cleanup" {
							return injected
						}
						if tc.failure == "apply_and_cleanup" {
							return cleanupErr
						}
					}
				default:
					require.True(t, ready[table], "stage DDL must precede DML: %s", sql)
					if strings.HasPrefix(sql, "delete from "+qualifiedTableName("db1", table)) {
						require.True(t, dirty[table], "fresh stages must not be cleared")
						clears[kind]++
						if tc.failure == "clear" {
							return injected
						}
						dirty[table] = false
					}
					if strings.HasPrefix(sql, "insert into "+qualifiedTableName("db1", table)) {
						require.False(t, dirty[table], "old rows must be cleared before the next load")
						dirty[table] = true
						loads[kind]++
						if strings.HasPrefix(tc.failure, "apply") {
							return injected
						}
					}
				}
				return nil
			}).AnyTimes()
			bh.EXPECT().GetExecResultSet().Return(nil).AnyTimes()
			bh.EXPECT().ClearExecResultSet().AnyTimes()
			produce := func(emit emitFunc) error {
				require.Zero(t, createCalls, "no stage DDL before production")
				for opIndex, wrapped := range tc.ops {
					bat := tbl.retPool.acquireRetBatch(tbl, false)
					rows := 1
					if tc.rowsPerOp != nil {
						rows = tc.rowsPerOp[opIndex]
					}
					for i := 0; i < rows; i++ {
						require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(i+1), false, ses.proc.Mp()))
						for _, vec := range bat.Vecs[1:] {
							if vec.GetType().Oid == types.T_int64 {
								require.NoError(t, vector.AppendFixed(vec, int64(2), false, ses.proc.Mp()))
							} else if vec.GetType().Oid == types.T_uint64 {
								require.NoError(t, vector.AppendFixed(vec, uint64(1), false, ses.proc.Mp()))
							} else {
								require.NoError(t, vector.AppendBytes(vec, []byte("row"), false, ses.proc.Mp()))
							}
						}
					}
					bat.SetRowCount(rows)
					wrapped.batch = bat
					if _, err := emitBatch(emit, wrapped, false, tbl.retPool); err != nil {
						return err
					}
				}
				if tc.failure == "panic" {
					panic(injected)
				}
				if tc.failure == "cancel" {
					cancel()
					return ctx.Err()
				}
				return nil
			}
			timing := &dataBranchMergeTiming{now: time.Now, phases: make(map[string]time.Duration)}
			run := func() error { return mergeDiffs(ctx, cancel, ses, bh, tbl, produce, timing) }
			switch tc.failure {
			case "":
				require.NoError(t, run())
			case "panic":
				require.PanicsWithValue(t, injected, func() { _ = run() })
			case "cancel":
				require.ErrorIs(t, run(), context.Canceled)
			default:
				require.ErrorIs(t, run(), injected)
			}
			require.Len(t, created, len(tc.want))
			require.Len(t, dropped, len(tc.want))
			for _, kind := range tc.want {
				require.Equal(t, 1, created[kind], kind)
				require.Equal(t, 2, dropped[kind], "one initial DROP and one cleanup for %s", kind)
			}
			require.Empty(t, ready)
			require.Empty(t, dirty)
			if tc.failure == "" {
				for _, kind := range tc.want {
					require.Equal(t, max(0, loads[kind]-1), clears[kind], "no final clear for %s", kind)
				}
			}
			if tc.failure == "clear" {
				kind := "upd"
				if tc.name == "clear_delete_failure" {
					kind = "del"
				}
				require.Equal(t, 1, clears[kind])
				require.Equal(t, 1, loads[kind], "a failed clear must prevent the next load")
			}
			if tc.name == "repeated_flush" {
				require.GreaterOrEqual(t, loads["direct"], 2)
				require.GreaterOrEqual(t, loads["upd"], 2)
				require.GreaterOrEqual(t, loads["del"], 2)
			}
			if len(tc.want) == 0 {
				require.Zero(t, timing.phases["stage_init"])
				require.Zero(t, timing.phases["stage_cleanup"])
			}
			if tc.name == "insert" || tc.name == "restore_deleted_target" || tc.name == "keyless_insert" {
				require.Equal(t, 1, loads["direct"], "flush must issue one target INSERT")
			}
			if tc.name == "inline_cells_limit" || tc.name == "inline_update" || tc.name == "inline_failure" || tc.name == "inline_limit" || tc.name == "inline_between_staged" {
				require.Equal(t, 1, loads["inline"])
			}
			if tc.name == "inline_above_limit" || tc.name == "inline_cells_above_limit" {
				require.Zero(t, loads["inline"])
			}
			if tc.name == "inline_between_staged" {
				require.Equal(t, 2, loads["upd"])
			}
			if tc.name == "inline_repeated" {
				require.Equal(t, 2, loads["inline"])
			}
			if tc.name == "empty" {
				require.Zero(t, timing.phases["apply"])
			}
		})
	}
}

func TestBranchMergeTimingExcludesStageInitializationFromApply(t *testing.T) {
	for _, failure := range []string{"", "error", "panic"} {
		t.Run(failure, func(t *testing.T) {
			now := time.Now()
			timing := testMergeTiming(&now)
			run := func() error {
				return timing.produce(func(emit emitFunc) error {
					now = now.Add(time.Millisecond)
					_, err := emit(batchWithKind{})
					return err
				}, func(batchWithKind) (bool, error) {
					err := timing.apply(func() error {
						now = now.Add(2 * time.Millisecond)
						return timing.measure("stage_init", func() error {
							now = now.Add(3 * time.Millisecond)
							if failure == "panic" {
								panic("init panic")
							}
							if failure == "error" {
								return errors.New("init error")
							}
							return nil
						})
					})
					return false, err
				})
			}
			switch failure {
			case "panic":
				require.PanicsWithValue(t, "init panic", func() { _ = run() })
			case "error":
				require.EqualError(t, run(), "init error")
			default:
				require.NoError(t, run())
			}
			phases := timing.account(6 * time.Millisecond)
			require.Equal(t, float64(1), phases["diff"])
			require.Equal(t, float64(2), phases["apply"])
			require.Equal(t, float64(3), phases["stage_init"])
			require.Zero(t, phases["other"])
		})
	}
}
