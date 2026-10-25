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
	"sync/atomic"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/malloc"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/require"
)

// Make released entries unreadable immediately, independently of allocator
// reuse, batch sizes, hash shard scheduling, and background storage compaction.
type poisoningBranchAllocator struct {
	outstanding atomic.Int64
}

func (a *poisoningBranchAllocator) Allocate(size uint64, _ malloc.Hints) ([]byte, malloc.Deallocator, error) {
	buf := make([]byte, size)
	a.outstanding.Add(int64(size))
	return buf, malloc.FuncDeallocator(func() {
		clear(buf)
		a.outstanding.Add(-int64(size))
	}), nil
}

func TestBuildHashmapForTablePreservesBorrowedKeysDuringCompaction(t *testing.T) {
	for _, tc := range []struct {
		name              string
		inserted, deleted bool
		updates           int
	}{
		{name: "update_twice", updates: 2},
		{name: "update_three_times", updates: 3},
		{name: "update_then_delete", updates: 1, deleted: true},
		{name: "insert_then_update", inserted: true, updates: 1},
		{name: "insert_update_delete", inserted: true, updates: 1, deleted: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ses := newValidateSession(t)
			ctrl := gomock.NewController(t)
			allocator := &poisoningBranchAllocator{}
			defer func() { require.Zero(t, allocator.outstanding.Load()) }()
			tblStuff := newTestBranchTableStuff(ctrl)
			tblStuff.hashmapAllocator = &branchHashmapAllocator{upstream: allocator}
			tblStuff.maxTombstoneBatchCnt = 1
			worker, err := ants.NewPool(1)
			require.NoError(t, err)
			defer worker.Release()
			tblStuff.worker = worker

			var responses []stubEngineChangesHandleResponse
			// Separate batches allocate the old and new row versions in
			// separate blocks. Removing the old entry releases its key too.
			appendChange := func(version int, insert, remove bool) {
				ts := commitTSBytes(types.BuildTS(int64(version+10), 0))
				response := stubEngineChangesHandleResponse{}
				if insert {
					response.data = buildHashDiffDataBatchWithRowIDs(t, ses.proc.Mp(),
						[]types.Rowid{buildHashDiffRowID(t, version)},
						[][]any{{int64(7), "value", "hidden", ts}})
				}
				if remove {
					response.tombstone = buildHashDiffTombstoneBatchWithRowIDs(t, ses.proc.Mp(),
						[]types.Rowid{buildHashDiffRowID(t, version-1)},
						[][]any{{int64(7), ts}})
				}
				responses = append(responses, response)
			}
			if tc.inserted {
				appendChange(0, true, false)
			}
			for version := 1; version <= tc.updates; version++ {
				appendChange(version, true, true)
			}
			if tc.deleted {
				appendChange(tc.updates+1, false, true)
			}

			data, tombstones, err := buildHashmapForTable(context.Background(), ses.proc.Mp(), &tblStuff,
				[]engine.ChangesHandle{&stubEngineChangesHandle{responses: responses}}, "target", nil)
			require.NoError(t, err)
			defer data.Close()
			defer tombstones.Close()
			wantData, wantTombstones := int64(1), int64(1)
			if tc.deleted {
				wantData = 0
			}
			if tc.inserted {
				wantTombstones = 0
			}
			require.Equal(t, wantData, data.ItemCount())
			require.Equal(t, wantTombstones, tombstones.ItemCount(), "an LCA deletion must survive intermediate-version compaction")
		})
	}
}
