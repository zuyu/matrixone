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
	"math"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

type restoreKeysExec struct {
	*restoreWorkExec
	results []interface{}
}

func (e *restoreKeysExec) ClearExecResultSet()             { e.results = nil }
func (e *restoreKeysExec) GetExecResultSet() []interface{} { return e.results }
func (e *restoreKeysExec) ClearExecResultBatches()         {}

func TestBranchRestoreIntegerKeyEligibility(t *testing.T) {
	for _, typ := range []types.T{types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64, types.T_decimal128, types.T_varchar, types.T_float64} {
		def := &plan.TableDef{Cols: []*plan.ColDef{{Name: "id", Typ: plan.Type{Id: int32(typ)}}}}
		want := typ == types.T_int8 || typ == types.T_int16 || typ == types.T_int32 || typ == types.T_int64 ||
			typ == types.T_uint8 || typ == types.T_uint16 || typ == types.T_uint32 || typ == types.T_uint64
		got := branchRestoreIntegerKeys([]string{"id"}, def, def, def)
		require.Equal(t, want, len(got) == 1, "%v", typ)
		if want {
			require.Equal(t, typ == types.T_uint8 || typ == types.T_uint16 || typ == types.T_uint32 || typ == types.T_uint64, got[0])
		}
		other := &plan.TableDef{Cols: []*plan.ColDef{{Name: "id", Typ: plan.Type{Id: int32(types.T_bool)}}}}
		require.Nil(t, branchRestoreIntegerKeys([]string{"id"}, def, other))
		require.Nil(t, branchRestoreIntegerKeys([]string{"missing"}, def))
	}
	require.Nil(t, branchRestoreIntegerKeys(nil))
}

func TestBranchRestoreDeleteProbeBounds(t *testing.T) {
	for _, tc := range []struct {
		rows, keys int
		inline     bool
	}{
		{0, 1, true}, {128, 1, true}, {129, 1, false}, {102, 5, true}, {103, 5, false},
	} {
		t.Run(fmt.Sprintf("%d/%d", tc.rows, tc.keys), func(t *testing.T) {
			p := restoreWorkPlan("t")
			p.keys = make([]string, tc.keys)
			p.deleteKeyUnsigned = make([]bool, tc.keys)
			result := &MysqlResultSet{}
			for i := range p.keys {
				p.keys[i] = fmt.Sprintf("k%d", i)
				result.AddColumn(&MysqlColumn{})
			}
			for row := 0; row < tc.rows; row++ {
				values := make([]interface{}, tc.keys)
				for i := range values {
					values[i] = int64(row)
				}
				result.AddRow(values)
			}
			back := &restoreKeysExec{restoreWorkExec: &restoreWorkExec{}, results: []interface{}{"stale"}}
			back.run = func(_ context.Context, sql string) error {
				require.Empty(t, back.results)
				require.Equal(t, fmt.Sprintf("select keys limit %d", min(128, 512/tc.keys)+1), sql)
				back.results = []interface{}{result}
				return nil
			}
			p.deleteValues = "stale"
			w := newBranchRestoreWork("reset", 1)
			used, err := w.probeDeleteKeys(t.Context(), back, &p)
			require.NoError(t, err)
			require.Equal(t, tc.inline, used)
			require.Empty(t, back.results)
			require.Equal(t, 1, w.counts["delete_probe"])
			if used {
				require.Equal(t, 1, w.inlineDeletes)
				require.Equal(t, int64(tc.rows), w.materializedRows)
				require.Equal(t, tc.rows == 0, p.deletesEmpty)
			} else {
				require.Empty(t, p.deleteValues)
				require.Equal(t, 1, w.fallbackDeletes)
				require.Zero(t, w.materializedRows, "fallback CTAS owns row accounting")
			}
		})
	}
}

func TestBranchRestoreDeleteProbeExactIntegersAndErrors(t *testing.T) {
	for _, failure := range []string{"", "query", "shape", "null", "value", "cancel"} {
		t.Run(failure, func(t *testing.T) {
			p := restoreWorkPlan("`db`.`t`")
			p.keys, p.deleteKeyUnsigned = []string{"a`b", "u"}, []bool{false, true}
			result := &MysqlResultSet{}
			result.AddColumn(&MysqlColumn{})
			result.AddColumn(&MysqlColumn{})
			result.AddRow([]interface{}{int64(math.MinInt64), uint64(math.MaxUint64)})
			if failure == "null" {
				result.Data[0][0] = nil
			}
			if failure == "value" {
				result.Data[0][0] = "not an integer"
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			back := &restoreKeysExec{restoreWorkExec: &restoreWorkExec{}}
			back.run = func(context.Context, string) error {
				if failure == "query" {
					return errors.New("probe failed")
				}
				if failure == "shape" {
					result.AddColumn(&MysqlColumn{})
				}
				if failure == "cancel" {
					cancel()
				}
				back.results = []interface{}{result}
				return nil
			}
			w := newBranchRestoreWork("revert", 1)
			used, err := w.probeDeleteKeys(ctx, back, &p)
			require.Empty(t, back.results)
			if failure != "" {
				require.Error(t, err)
				require.False(t, used)
				require.Empty(t, p.deleteValues)
				require.Zero(t, w.inlineDeletes)
			} else {
				require.NoError(t, err)
				require.True(t, used)
				require.Equal(t, "(-9223372036854775808,18446744073709551615)", p.deleteValues)
				require.Equal(t, "delete from `db`.`t` where (`a``b`,`u`) in ((-9223372036854775808,18446744073709551615))", p.applySQL()[0])
			}
		})
	}
}

func TestBranchRestoreDeleteProbeEmptyPipeline(t *testing.T) {
	p := restoreWorkPlan("t")
	p.deleteKeyUnsigned = []bool{false}
	back := &restoreKeysExec{restoreWorkExec: &restoreWorkExec{run: func(context.Context, string) error { return nil }}}
	w := newBranchRestoreWork("reset", 1)
	used, err := w.probeDeleteKeys(t.Context(), back, &p)
	require.NoError(t, err)
	require.True(t, used)
	require.True(t, p.deletesEmpty)
	require.Equal(t, 1, w.inlineDeletes)
	require.Equal(t, 1, w.emptyStages)
}

func TestBranchRestoreDeleteProbeOrderingAndFallback(t *testing.T) {
	for _, rows := range []int{0, 1, 129} {
		t.Run(fmt.Sprint(rows), func(t *testing.T) {
			var sqls []string
			back := &restoreKeysExec{restoreWorkExec: &restoreWorkExec{}}
			back.run = func(_ context.Context, sql string) error {
				sqls = append(sqls, sql)
				if strings.HasPrefix(sql, "select") {
					r := &MysqlResultSet{}
					r.AddColumn(&MysqlColumn{})
					for i := 0; i < rows; i++ {
						r.AddRow([]interface{}{int64(i)})
					}
					back.results = []interface{}{r}
				}
				return nil
			}
			plans := []branchRestorePlan{restoreWorkPlan("a"), restoreWorkPlan("b")}
			for i := range plans {
				plans[i].deleteKeyUnsigned = []bool{false}
			}
			require.NoError(t, executeBranchRestorePlans(t.Context(), back, plans, "reset"))
			for i, sql := range sqls {
				if strings.HasPrefix(sql, "update") || strings.HasPrefix(sql, "delete") {
					for _, later := range sqls[i:] {
						require.False(t, strings.HasPrefix(later, "create") || strings.HasPrefix(later, "select"))
					}
					break
				}
			}
			if rows > 128 {
				require.Equal(t, "drop temporary table if exists b_d,b_r,a_d,a_r", sqls[len(sqls)-1])
			} else {
				require.Equal(t, "drop temporary table if exists b_r,a_r", sqls[len(sqls)-1])
				for _, sql := range sqls {
					require.NotContains(t, sql, "table a_d")
					require.NotContains(t, sql, "table b_d")
				}
			}
		})
	}
}

func TestBranchRestoreDeleteProbeFailureRollsBack(t *testing.T) {
	for _, failure := range []string{"probe", "apply", "cancel"} {
		t.Run(failure, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			original := errors.New("injected failure")
			var sqls []string
			back := &restoreKeysExec{restoreWorkExec: &restoreWorkExec{}}
			back.run = func(_ context.Context, sql string) error {
				sqls = append(sqls, sql)
				if strings.HasPrefix(sql, "select") {
					if failure == "probe" {
						return original
					}
					if failure == "cancel" {
						cancel()
						return ctx.Err()
					}
					r := &MysqlResultSet{}
					r.AddColumn(&MysqlColumn{})
					r.AddRow([]interface{}{int64(1)})
					back.results = []interface{}{r}
				}
				if strings.HasPrefix(sql, "delete from") {
					return original
				}
				return nil
			}
			p := restoreWorkPlan("t")
			p.deleteKeyUnsigned = []bool{false}
			finished := false
			run := func() (err error) {
				defer finishDataBranchComponent(ctx, func(cause error) error {
					finished = true
					require.Error(t, cause)
					return cause
				}, &err)
				return executeBranchRestorePlans(ctx, back, []branchRestorePlan{p}, "reset")
			}
			err := run()
			if failure == "cancel" {
				require.ErrorIs(t, err, context.Canceled)
			} else {
				require.ErrorIs(t, err, original)
			}
			require.True(t, finished)
			require.Empty(t, back.results)
			require.Equal(t, "drop temporary table if exists t_r", sqls[len(sqls)-1])
			for _, sql := range sqls {
				require.False(t, strings.HasPrefix(sql, "update") || strings.HasPrefix(sql, "insert"))
			}
		})
	}
}
