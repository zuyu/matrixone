// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	statspb "github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	"github.com/stretchr/testify/require"
)

func TestMissingPrimaryKeyNDVRuntimeFilter(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*QueryBuilder)
		want   bool
	}{
		{"new large table", func(b *QueryBuilder) {}, true},
		{"small table", func(b *QueryBuilder) { b.qry.Nodes[0].Stats.TableCnt = 100 }, false},
		{"default statistics", func(b *QueryBuilder) { b.qry.Nodes[0].Stats = DefaultStats() }, false},
		{"nonfinite statistics", func(b *QueryBuilder) { b.qry.Nodes[0].Stats.TableCnt = math.Inf(1) }, false},
		{"unknown statistics", func(b *QueryBuilder) { b.qry.Nodes[0].Stats.TableCnt = math.NaN() }, false},
		{"missing key", func(b *QueryBuilder) { b.qry.Nodes[0].TableDef.Pkey = nil }, false},
		{"fake key", func(b *QueryBuilder) { b.qry.Nodes[0].TableDef.Pkey.PkeyColName = catalog.FakePrimaryKeyColName }, false},
		{"partial composite key", func(b *QueryBuilder) {
			configureRuntimeFilterCompositePK(b)
			b.qry.Nodes[2].OnList = b.qry.Nodes[2].OnList[:1]
		}, false},
		{"preserved probe", func(b *QueryBuilder) { b.qry.Nodes[2].JoinType = pb.Node_LEFT }, false},
		{"local delivery join", func(b *QueryBuilder) { b.qry.Nodes[2].JoinType = pb.Node_RIGHT }, false},
		{"hint disabled", func(b *QueryBuilder) { b.optimizerHints = &OptimizerHints{runtimeFilter: 1} }, false},
		{"unselective build", func(b *QueryBuilder) {
			b.qry.Nodes[2].Stats.HashmapStats.HashmapSize = 100_000
			b.qry.Nodes[1].Stats.Outcnt = 100_000
		}, false},
		{"fallible probe filter", func(b *QueryBuilder) {
			b.qry.Nodes[0].FilterList = []*pb.Expr{makeMixedSideRuntimeFilterResidual(b.qry.Nodes[0].TableDef.Cols[0].Typ, 1, 1)}
		}, false},
		{"floating primary key", func(b *QueryBuilder) {
			for _, n := range b.qry.Nodes[:2] {
				n.TableDef.Cols[0].Typ.Id = int32(types.T_float64)
			}
			for _, a := range b.qry.Nodes[2].OnList[0].GetF().Args {
				a.Typ.Id = int32(types.T_float64)
			}
		}, false},
		{"known NDV retains cost gate", func(b *QueryBuilder) {
			probe := b.qry.Nodes[0]
			probe.TableDef.TblId = 41
			ctx := &statsCacheCompilerContext{MockCompilerContext: b.compCtx.(*MockCompilerContext), statsCache: NewStatsCache()}
			ctx.statsCache.Set(41, &statspb.StatsInfo{NdvMap: map[string]float64{"id": 1000}})
			b.compCtx = ctx
			b.tag2Table = map[int32]*TableDef{1: probe.TableDef}
		}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := newRuntimeFilterSingleTestBuilder(t, false)
			probe, build, join := b.qry.Nodes[0], b.qry.Nodes[1], b.qry.Nodes[2]
			join.JoinType = pb.Node_INNER
			join.Stats.HashmapStats.HashmapSize = 1000
			build.Stats.Outcnt = 1000
			join.OnList[0].GetF().Args[0].GetCol().Name = "id"
			tc.change(b)
			b.generateRuntimeFilters(2)
			require.Equal(t, tc.want, len(join.RuntimeFilterBuildList) == 1)
			require.Equal(t, tc.want, len(probe.RuntimeFilterProbeList) == 1)
			if tc.want {
				require.Equal(t, GetInFilterCardLimitOnPK(b.compCtx.GetProcess().GetService(), probe.Stats.TableCnt), join.RuntimeFilterBuildList[0].UpperLimit)
				require.False(t, probe.RuntimeFilterProbeList[0].NotOnPk)
			}
		})
	}
}

func TestMissingPrimaryKeyNDVRequiresExactBinding(t *testing.T) {
	b := newRuntimeFilterSingleTestBuilder(t, false)
	join, probe := b.qry.Nodes[2], b.qry.Nodes[0]
	join.JoinType = pb.Node_INNER
	expr := join.OnList[0].GetF().Args[0]
	require.Equal(t, probe.Stats.TableCnt, runtimeFilterMissingProbeNDV(join, probe, expr))
	expr.GetCol().RelPos = 2
	require.Equal(t, float64(100), runtimeFilterMissingProbeNDV(join, probe, expr))
	expr.GetCol().RelPos = 1
	expr.Typ.Id = int32(types.T_datetime)
	require.Equal(t, float64(100), runtimeFilterMissingProbeNDV(join, probe, expr))
}
