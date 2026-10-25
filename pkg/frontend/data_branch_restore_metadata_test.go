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
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/stretchr/testify/require"
)

func TestBranchRestoreCatalogFilteringAndRefresh(t *testing.T) {
	ctrl := gomock.NewController(t)
	db := mock_frontend.NewMockDatabase(ctrl)
	hidden := catalog.PrefixIndexTableName + "idx"
	db.EXPECT().Relations(gomock.Any()).Return([]string{"t", "view", hidden}, nil).Times(3)
	old := &plan.TableDef{DbId: 10, TblId: 11, TableType: catalog.SystemOrdinaryRel}
	fresh := &plan.TableDef{DbId: 10, TblId: 12, Cols: []*plan.ColDef{{Name: "added"}}}
	view := &plan.TableDef{TableType: catalog.SystemViewRel}
	for _, def := range []*plan.TableDef{old, fresh, fresh} {
		rel := mock_frontend.NewMockRelation(ctrl)
		db.EXPECT().Relation(gomock.Any(), "t", nil).Return(rel, nil)
		rel.EXPECT().GetTableDef(gomock.Any()).Return(def)
		v := mock_frontend.NewMockRelation(ctrl)
		db.EXPECT().Relation(gomock.Any(), "view", nil).Return(v, nil)
		v.EXPECT().GetTableDef(gomock.Any()).Return(view)
	}
	first, err := scanBranchDatabaseCatalog(t.Context(), db, nil, true)
	require.NoError(t, err)
	require.Equal(t, map[string]bool{"t": true}, first.tables)
	require.Equal(t, map[string]*plan.TableDef{"t": old}, first.definitions)
	require.Equal(t, 2, first.relationOpens)
	second, err := scanBranchDatabaseCatalog(t.Context(), db, nil, true)
	require.NoError(t, err)
	require.Same(t, old, first.definitions["t"])
	require.Same(t, fresh, second.definitions["t"])
	namesOnly, err := scanBranchDatabaseCatalog(t.Context(), db, nil, false)
	require.NoError(t, err)
	require.Equal(t, second.tables, namesOnly.tables)
	require.Nil(t, namesOnly.definitions, "DIFF/MERGE must not retain extra table definitions")
}

func TestBranchRestoreCatalogFailure(t *testing.T) {
	for _, phase := range []string{"enumerate", "relation", "cancel"} {
		t.Run(phase, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			db := mock_frontend.NewMockDatabase(ctrl)
			injected := errors.New("metadata failed")
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if phase == "enumerate" {
				db.EXPECT().Relations(gomock.Any()).Return(nil, injected)
			} else {
				db.EXPECT().Relations(gomock.Any()).Return([]string{"t"}, nil)
				if phase == "cancel" {
					cancel()
					injected = context.Canceled
				} else {
					db.EXPECT().Relation(gomock.Any(), "t", nil).Return(nil, injected)
				}
			}
			result, err := scanBranchDatabaseCatalog(ctx, db, nil, true)
			require.ErrorIs(t, err, injected)
			require.Nil(t, result, "never publish partial metadata")
		})
	}
}

func restoreMetadataCatalog(ts int64, databaseID uint64) *branchDatabaseCatalog {
	return &branchDatabaseCatalog{snapshot: &plan2.Snapshot{TS: &timestamp.Timestamp{PhysicalTime: ts},
		ExtraInfo: &plan.SnapshotExtraInfo{Name: "snap", Level: tree.SNAPSHOTLEVELDATABASE.String(), ObjId: databaseID}},
		definitions: map[string]*plan.TableDef{"t": {DbId: databaseID, TblId: 20,
			Pkey: &plan.PrimaryKeyDef{PkeyColName: "id", Names: []string{"id"}},
			Cols: []*plan.ColDef{{Name: "id"}, {Name: "v"}}}}}
}

func TestBranchRestoreCatalogPlanKeepsEndpointSnapshots(t *testing.T) {
	named := &tree.AtTimeStamp{Type: tree.ATTIMESTAMPSNAPSHOT, SnapshotName: "snap"}
	endpoint := func(name string) tree.TableName {
		return *tree.NewTableName(tree.Identifier(name), tree.ObjectNamePrefix{}, named)
	}
	stmt := &tree.DataBranchDatabase{Operation: "revert", Source: endpoint("src"), Before: endpoint("before"), Destination: endpoint("dst")}
	stmt.Destination.AtTsExpr = nil
	source, destination, before := restoreMetadataCatalog(200, 1), restoreMetadataCatalog(0, 2), restoreMetadataCatalog(100, 3)
	destination.snapshot = nil
	p, err := planBranchRestore(stmt, "t", source, destination, before)
	require.NoError(t, err)
	require.Contains(t, p.rowsQuery, "200")
	require.Contains(t, p.rowsQuery, "100")
	require.Contains(t, p.deletesQuery, "200")
	require.Contains(t, p.deletesQuery, "100")
	require.NotContains(t, p.rowsQuery, "snapshot")
	require.Same(t, named, stmt.Source.AtTsExpr, "planning must not rewrite the caller's AST")
	require.Same(t, named, stmt.Before.AtTsExpr)
	require.Equal(t, "snap", named.SnapshotName)

	// A mismatching definition from any historical endpoint still fails scope
	// validation, even though database enumeration already checked the scope.
	for _, c := range []*branchDatabaseCatalog{source, before} {
		id := c.definitions["t"].DbId
		c.definitions["t"].DbId = 99
		_, err = planBranchRestore(stmt, "t", source, destination, before)
		require.ErrorContains(t, err, "does not belong")
		c.definitions["t"].DbId = id
	}
	before.definitions["t"].Cols = append(before.definitions["t"].Cols, &plan.ColDef{Name: "new"})
	_, err = planBranchRestore(stmt, "t", source, destination, before)
	require.ErrorContains(t, err, "matching writable columns")
	delete(before.definitions, "t")
	_, err = planBranchRestore(stmt, "t", source, destination, before)
	require.ErrorContains(t, err, "missing restore definition")
}
