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
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// updateUniqueProof describes only exact column identities and keys that remain
// unique in this input. Estimates and table names are not uniqueness evidence.
type updateUniqueProof struct {
	columns map[[2]int32]pb.Type
	keys    [][][2]int32
}

func (p updateUniqueProof) containsKey(columns map[[2]int32]bool) bool {
	for _, key := range p.keys {
		complete := len(key) > 0
		for _, col := range key {
			complete = complete && columns[col]
		}
		if complete {
			return true
		}
	}
	return false
}

// updateInputUniqueProof runs before DML appends its row-number deduplication.
// Only scans, direct projections, filters/sorts and key-preserving INNER/LEFT
// joins are admitted. Unsupported nodes fail closed. In particular, no key is
// propagated from the nullable side of an outer join.
func (builder *QueryBuilder) updateInputUniqueProof(nodeID int32) updateUniqueProof {
	result := updateUniqueProof{columns: make(map[[2]int32]pb.Type)}
	if nodeID < 0 || int(nodeID) >= len(builder.qry.Nodes) {
		return result
	}
	node := builder.qry.Nodes[nodeID]
	if node == nil {
		return result
	}
	switch node.NodeType {
	case pb.Node_TABLE_SCAN:
		if len(node.Children) != 0 || len(node.BindingTags) != 1 || node.TableDef == nil || len(node.ProjectList) != 0 {
			return result
		}
		tag := node.BindingTags[0]
		for i, col := range node.TableDef.Cols {
			if col == nil {
				continue
			}
			ref := [2]int32{tag, int32(i)}
			result.columns[ref] = col.Typ
			if col.Name == catalog.Row_ID && col.Typ.Id == int32(types.T_Rowid) {
				result.keys = append(result.keys, [][2]int32{ref})
			}
		}
		if positions, ok := sqlEqualityCompatiblePrimaryKeyColumnPositions(node.TableDef); ok {
			key := make([][2]int32, len(positions))
			for i, pos := range positions {
				key[i] = [2]int32{tag, pos}
			}
			result.keys = append(result.keys, key)
		}

	case pb.Node_FILTER, pb.Node_SORT:
		if len(node.Children) != 1 || len(node.BindingTags) != 0 {
			return result
		}
		// DML also inserts filters with an explicit positional identity
		// projection. These keep the child's binding, unlike a PROJECT node.
		if len(node.ProjectList) > 0 {
			child := builder.qry.Nodes[node.Children[0]]
			if child == nil || len(node.ProjectList) != len(child.ProjectList) {
				return result
			}
			for i, expr := range node.ProjectList {
				col := expr.GetCol()
				if col == nil || col.RelPos != 0 || col.ColPos != int32(i) ||
					!sqlEqualityJoinUsesOneIdentityDomain(expr.Typ, child.ProjectList[i].Typ) {
					return result
				}
			}
		}
		return builder.updateInputUniqueProof(node.Children[0])

	case pb.Node_PROJECT:
		if len(node.Children) != 1 || len(node.BindingTags) != 1 {
			return result
		}
		child := builder.updateInputUniqueProof(node.Children[0])
		mapped := make(map[[2]int32][2]int32)
		for i, expr := range node.ProjectList {
			col := expr.GetCol()
			if col == nil {
				continue
			}
			old := [2]int32{col.RelPos, col.ColPos}
			typ, ok := child.columns[old]
			if !ok || !sqlEqualityJoinUsesOneIdentityDomain(expr.Typ, typ) {
				continue
			}
			ref := [2]int32{node.BindingTags[0], int32(i)}
			result.columns[ref] = typ
			// One direct alias is enough for a proof. Additional aliases may
			// cause a conservative miss, never a false uniqueness claim.
			if _, exists := mapped[old]; !exists {
				mapped[old] = ref
			}
		}
		for _, key := range child.keys {
			projected := make([][2]int32, 0, len(key))
			for _, col := range key {
				if ref, ok := mapped[col]; ok {
					projected = append(projected, ref)
				}
			}
			if len(projected) == len(key) {
				result.keys = append(result.keys, projected)
			}
		}

	case pb.Node_JOIN:
		if len(node.Children) != 2 || len(node.ProjectList) != 0 ||
			(node.JoinType != pb.Node_INNER && node.JoinType != pb.Node_LEFT) {
			return result
		}
		left := builder.updateInputUniqueProof(node.Children[0])
		right := builder.updateInputUniqueProof(node.Children[1])
		for ref, typ := range left.columns {
			result.columns[ref] = typ
		}
		for ref, typ := range right.columns {
			if _, duplicate := result.columns[ref]; duplicate {
				return updateUniqueProof{}
			}
			result.columns[ref] = typ
		}
		leftEqual, rightEqual := make(map[[2]int32]bool), make(map[[2]int32]bool)
		for _, cond := range splitPlanConjunctions(node.OnList) {
			fn := cond.GetF()
			if fn == nil || fn.Func == nil || fn.Func.ObjName != "=" || len(fn.Args) != 2 {
				continue
			}
			l, r := fn.Args[0], fn.Args[1]
			lc, rc := l.GetCol(), r.GetCol()
			if lc == nil || rc == nil {
				continue
			}
			lk, rk := [2]int32{lc.RelPos, lc.ColPos}, [2]int32{rc.RelPos, rc.ColPos}
			if _, reversed := right.columns[lk]; reversed {
				lk, rk, l, r = rk, lk, r, l
			}
			lt, lok := left.columns[lk]
			rt, rok := right.columns[rk]
			if !lok || !rok || !sqlEqualityJoinUsesOneIdentityDomain(l.Typ, r.Typ) ||
				!sqlEqualityJoinUsesOneIdentityDomain(l.Typ, lt) || !sqlEqualityJoinUsesOneIdentityDomain(r.Typ, rt) {
				continue
			}
			leftEqual[lk], rightEqual[rk] = true, true
		}
		if right.containsKey(rightEqual) {
			result.keys = append(result.keys, left.keys...)
		}
		if node.JoinType == pb.Node_INNER && left.containsKey(leftEqual) {
			result.keys = append(result.keys, right.keys...)
		}
	}
	return result
}
