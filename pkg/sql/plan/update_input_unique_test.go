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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestUpdateInputUniquePlan(t *testing.T) {
	table := "constraint_test.t_on_update_gen"
	for _, tc := range []struct {
		name, sql string
		window    bool
	}{
		{"primary key", "update " + table + " d join " + table + " s on d.id=s.id set d.val=s.val", false},
		{"filtered projected source", "update " + table + " d join (select id,val from " + table + " where val>0) s on d.id=s.id set d.val=s.val", false},
		{"revert shape", "update " + table + " d join (select b.id,b.val from " + table + " b left join " + table + " a on a.id=b.id where a.id is null or not(a.val<=>b.val)) s on d.id=s.id set d.val=s.val where not(d.val<=>s.val)", false},
		{"preserved outer target", "update " + table + " d left join " + table + " s on d.id=s.id set d.val=s.val", false},
		{"nullable outer target", "update " + table + " d left join " + table + " s on d.id=s.id set s.val=d.val", true},
		{"nonunique source", "update " + table + " d join " + table + " s on d.val=s.val set d.val=s.val+1", true},
		{"union all source", "update " + table + " d join (select id,val from " + table + " union all select id,val from " + table + ") s on d.id=s.id set d.val=s.val", true},
		{"computed key", "update " + table + " d join " + table + " s on d.id=abs(s.id) set d.val=s.val", true},
		{"multiple targets", "update " + table + " d join " + table + " s on d.id=s.id set d.val=s.val,s.val=d.val", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(false, newPlanTestProcess(t))
			p, err := runOneStmt(mock, t, tc.sql)
			require.NoError(t, err)
			hasWindow := false
			for _, node := range p.GetQuery().Nodes {
				hasWindow = hasWindow || node.NodeType == pb.Node_WINDOW
			}
			require.Equal(t, tc.window, hasWindow)
		})
	}
}

func updateProofFixture(composite bool) *QueryBuilder {
	intType := pb.Type{Id: int32(types.T_int64), NotNullable: true}
	var nodes []*pb.Node
	for tag := int32(1); tag <= 2; tag++ {
		pk := &pb.PrimaryKeyDef{PkeyColName: "id", Names: []string{"id"}}
		if composite {
			pk = &pb.PrimaryKeyDef{PkeyColName: catalog.CPrimaryKeyColName, Names: []string{"id", "tenant"}}
		}
		nodes = append(nodes, &pb.Node{NodeType: pb.Node_TABLE_SCAN, BindingTags: []int32{tag}, TableDef: &pb.TableDef{
			Cols: []*pb.ColDef{{Name: "id", Typ: intType}, {Name: "tenant", Typ: intType}, {Name: catalog.Row_ID, Typ: pb.Type{Id: int32(types.T_Rowid)}}}, Pkey: pk,
		}})
	}
	nodes = append(nodes, &pb.Node{NodeType: pb.Node_JOIN, JoinType: pb.Node_INNER, Children: []int32{0, 1}, OnList: []*pb.Expr{updateProofEquality(0, intType)}})
	return &QueryBuilder{qry: &pb.Query{Nodes: nodes}}
}

func updateProofEquality(pos int32, typ pb.Type) *pb.Expr {
	return &pb.Expr{Expr: &pb.Expr_F{F: &pb.Function{Func: &pb.ObjectRef{ObjName: "="}, Args: []*pb.Expr{
		{Typ: typ, Expr: &pb.Expr_Col{Col: &pb.ColRef{RelPos: 1, ColPos: pos}}},
		{Typ: typ, Expr: &pb.Expr_Col{Col: &pb.ColRef{RelPos: 2, ColPos: pos}}},
	}}}}
}

func TestUpdateInputUniqueProofBoundaries(t *testing.T) {
	target := map[[2]int32]bool{{1, 2}: true}
	for _, tc := range []struct {
		name   string
		change func(*QueryBuilder)
		want   bool
	}{
		{"direct key", func(b *QueryBuilder) {}, true},
		{"reversed equality", func(b *QueryBuilder) {
			args := b.qry.Nodes[2].OnList[0].GetF().Args
			args[0], args[1] = args[1], args[0]
		}, true},
		{"missing key", func(b *QueryBuilder) { b.qry.Nodes[1].TableDef.Pkey = nil }, false},
		{"cross join", func(b *QueryBuilder) { b.qry.Nodes[2].OnList = nil }, false},
		{"same side equality", func(b *QueryBuilder) { b.qry.Nodes[2].OnList[0].GetF().Args[1].GetCol().RelPos = 1 }, false},
		{"null safe equality", func(b *QueryBuilder) { b.qry.Nodes[2].OnList[0].GetF().Func.ObjName = "<=>" }, false},
		{"floating key", func(b *QueryBuilder) {
			for _, n := range b.qry.Nodes[:2] {
				n.TableDef.Cols[0].Typ.Id = int32(types.T_float64)
			}
			for _, a := range b.qry.Nodes[2].OnList[0].GetF().Args {
				a.Typ.Id = int32(types.T_float64)
			}
		}, false},
		{"collated key", func(b *QueryBuilder) {
			for _, n := range b.qry.Nodes[:2] {
				n.TableDef.Cols[0].Typ = pb.Type{Id: int32(types.T_varchar), Charset: uint32(types.CharsetUTF8MB4UnicodeCI)}
			}
			for _, a := range b.qry.Nodes[2].OnList[0].GetF().Args {
				a.Typ = b.qry.Nodes[0].TableDef.Cols[0].Typ
			}
		}, false},
		{"concrete type mismatch", func(b *QueryBuilder) { b.qry.Nodes[1].TableDef.Cols[0].Typ.Id = int32(types.T_datetime) }, false},
		{"left join preserves target", func(b *QueryBuilder) { b.qry.Nodes[2].JoinType = pb.Node_LEFT }, true},
		{"full join", func(b *QueryBuilder) { b.qry.Nodes[2].JoinType = pb.Node_OUTER }, false},
		{"duplicate binding", func(b *QueryBuilder) { b.qry.Nodes[1].BindingTags[0] = 1 }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := updateProofFixture(false)
			tc.change(b)
			require.Equal(t, tc.want, b.updateInputUniqueProof(2).containsKey(target))
		})
	}
	b := updateProofFixture(true)
	require.False(t, b.updateInputUniqueProof(2).containsKey(target), "incomplete composite equality")
	b.qry.Nodes[2].OnList = append(b.qry.Nodes[2].OnList, updateProofEquality(1, b.qry.Nodes[0].TableDef.Cols[1].Typ))
	require.True(t, b.updateInputUniqueProof(2).containsKey(target))
	b.qry.Nodes[2].JoinType = pb.Node_LEFT
	require.False(t, b.updateInputUniqueProof(2).containsKey(map[[2]int32]bool{{2, 2}: true}), "nullable-side rowids are not unique")
}
