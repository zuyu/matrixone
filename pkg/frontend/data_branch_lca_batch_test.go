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
	"regexp"
	"strconv"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/require"
)

func TestBranchLCAProbeCoalescesSmallSerialShards(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		rows, limit, queries int
		failure              string
	}{
		{name: "empty", limit: 4096},
		{name: "small", rows: 100, limit: 4096, queries: 1},
		{name: "cap", rows: 1024, limit: 4096, queries: 1},
		{name: "above_cap", rows: 1025, limit: 4096, queries: 10},
		{name: "memory_limit", rows: 100, limit: 8, queries: -1},
		{name: "query_failure", rows: 100, limit: 4096, queries: 1, failure: "query"},
		{name: "delivery_failure", rows: 100, limit: 4096, queries: 1, failure: "delivery"},
		{name: "cancelled", rows: 100, limit: 4096, failure: "cancel"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ses := newValidateSession(t)
			ctrl := gomock.NewController(t)
			tbl := newTestBranchTableStuff(ctrl)
			defer tbl.retPool.freeAllRetBatches(ses.proc.Mp())
			worker, err := ants.NewPool(1)
			require.NoError(t, err)
			defer worker.Release()
			tbl.worker = worker
			tbl.maxTombstoneBatchCnt = tc.limit
			tbl.hashmapAllocator = newBranchHashmapAllocator(dataBranchHashmapLimitRate)
			lca := mock_frontend.NewMockRelation(ctrl)
			lca.EXPECT().GetTableDef(gomock.Any()).Return(newTestBranchTableDef("lca_tbl", "name")).AnyTimes()
			lca.EXPECT().GetTableID(gomock.Any()).Return(uint64(77)).AnyTimes()
			tbl.lcaRel = lca

			var dataRows, tombRows [][]any
			var dataIDs, tombIDs []types.Rowid
			wantUpdates, wantDeletes, wantRemaining := map[int64]bool{}, map[int64]bool{}, map[int64]bool{}
			for i := 1; i <= tc.rows; i++ {
				id := int64(i)
				tombIDs = append(tombIDs, buildHashDiffRowID(t, i))
				tombRows = append(tombRows, []any{id, commitTSBytes(types.BuildTS(20, 0))})
				if i%4 != 3 {
					dataIDs = append(dataIDs, buildHashDiffRowID(t, i+tc.rows))
					dataRows = append(dataRows, []any{id, "new", "hidden", commitTSBytes(types.BuildTS(20, 0))})
				}
				switch i % 4 {
				case 0:
					wantRemaining[id] = true // no LCA row; the new rowid must survive
				case 1: // endpoint equals its ancestor: no net change
				case 2:
					wantUpdates[id] = true
				case 3:
					wantDeletes[id] = true
				}
			}
			responses := []stubEngineChangesHandleResponse{}
			if tc.rows > 0 {
				responses = append(responses, stubEngineChangesHandleResponse{
					data:      buildHashDiffDataBatchWithRowIDs(t, ses.proc.Mp(), dataIDs, dataRows),
					tombstone: buildHashDiffTombstoneBatchWithRowIDs(t, ses.proc.Mp(), tombIDs, tombRows),
				})
			}
			data, tombs, err := buildHashmapForTable(t.Context(), ses.proc.Mp(), &tbl,
				[]engine.ChangesHandle{&stubEngineChangesHandle{responses: responses}}, "target", nil)
			require.NoError(t, err)
			defer data.Close()
			defer tombs.Close()
			require.Equal(t, 10, tombs.ShardCount())

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if tc.failure == "cancel" {
				cancel()
			}
			injected := errors.New("injected " + tc.failure)
			bh := mock_frontend.NewMockBackgroundExec(ctrl)
			queryCount := 0
			var result *MysqlResultSet
			rowPattern := regexp.MustCompile(`row\((\d+),(\d+)\)`)
			bh.EXPECT().Exec(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, sql string) error {
				queryCount++
				if tc.failure == "query" {
					return injected
				}
				require.Contains(t, sql, "{MO_TS=10}")
				keys := rowPattern.FindAllStringSubmatch(sql, -1)
				require.NotEmpty(t, keys)
				require.LessOrEqual(t, len(keys), tc.limit)
				var rows [][]interface{}
				for _, key := range keys {
					ordinal, err := strconv.ParseInt(key[1], 10, 64)
					require.NoError(t, err)
					id, err := strconv.ParseInt(key[2], 10, 64)
					require.NoError(t, err)
					if id%4 == 0 {
						rows = append(rows, []interface{}{ordinal, nil, nil, nil})
						continue
					}
					name := "old"
					if id%4 == 1 {
						name = "new"
					}
					rows = append(rows, []interface{}{ordinal, id, name, "hidden"})
				}
				result = buildLCAProbeResultSetWithRows(rows...)
				return nil
			}).AnyTimes()
			bh.EXPECT().GetExecResultSet().DoAndReturn(func() []interface{} { return []interface{}{result} }).AnyTimes()
			bh.EXPECT().ClearExecResultSet().AnyTimes()
			updates, deletes, oldUpdates := map[int64]bool{}, map[int64]bool{}, map[int64]bool{}
			logicalUpdates, logicalDeletes := 0, 0
			err = findDeleteAndUpdateBat(ctx, ses, bh, tbl, "target", diffSideTarget, func(w batchWithKind) error {
				defer tbl.retPool.releaseRetBatch(w.batch, false)
				if tc.failure == "delivery" {
					return injected
				}
				for _, row := range decodeCapturedRows(t, w.batch, tbl.def.colTypes) {
					id := row[0].(int64)
					dst := deletes
					if w.fromUpdate {
						dst = oldUpdates
						if w.kind == diffInsert {
							dst = updates
						}
					}
					require.False(t, dst[id], "a row must be delivered only once per logical piece")
					dst[id] = true
				}
				return nil
			}, types.BuildTS(10, 0), true, data, tombs, 1, func(w batchWithKind) error {
				if w.kind == diffUpdate {
					logicalUpdates += w.batch.RowCount()
				} else {
					logicalDeletes += w.batch.RowCount()
				}
				return nil
			})
			switch tc.failure {
			case "":
				require.NoError(t, err)
			case "cancel":
				require.ErrorIs(t, err, context.Canceled)
			default:
				require.ErrorIs(t, err, injected)
			}
			if tc.queries >= 0 {
				require.Equal(t, tc.queries, queryCount)
			} else {
				require.Greater(t, queryCount, 1)
			}
			if tc.failure != "" {
				return
			}
			require.Equal(t, wantUpdates, updates)
			require.Equal(t, wantUpdates, oldUpdates)
			require.Equal(t, wantDeletes, deletes)
			require.Equal(t, len(wantUpdates), logicalUpdates)
			require.Equal(t, len(wantDeletes), logicalDeletes)
			require.Equal(t, int64(len(wantRemaining)), data.ItemCount())
		})
	}
}
