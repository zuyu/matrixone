// Copyright 2025 Matrix Origin
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

package frontend

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
)

// dataBranchMergeSummary describes the logical pre-merge differences, using
// the same source/destination orientation as DIFF source AGAINST destination.
// Counts include conflicts, independent of SKIP/ACCEPT. Equal changes and
// rows deleted on both sides are not conflicts. Only PKs, never row values,
// are retained while joining the two sides. Apply runs after analysis using
// the already resolved endpoints; plain MERGE does not pay for analysis.
type dataBranchMergeSummary struct {
	source      [3]uint64
	destination [3]uint64
	conflicts   uint64
	keys        map[string]uint8
}

func newDataBranchMergeSummary() *dataBranchMergeSummary {
	return &dataBranchMergeSummary{keys: make(map[string]uint8)}
}

func (s *dataBranchMergeSummary) observe(ses *Session, tables tableStuff, b batchWithKind) error {
	var kind int
	switch b.kind {
	case diffInsert:
		kind = 0
	case diffDelete:
		kind = 1
	case diffUpdate:
		kind = 2
	default:
		return moerr.NewInternalErrorNoCtxf("unknown merge summary kind %q", b.kind)
	}
	switch b.side {
	case diffSideTarget:
		s.source[kind] += uint64(b.batch.RowCount())
	case diffSideBase:
		s.destination[kind] += uint64(b.batch.RowCount())
	default:
		return moerr.NewInternalErrorNoCtxf("unknown merge summary side %d", b.side)
	}
	if tables.def.pkKind == fakeKind {
		return nil
	}
	for row := range b.batch.RowCount() {
		key, err := extractPKAsString(ses, tables, b.batch, row)
		if err != nil {
			return err
		}
		s.observeKey(key, b.side, b.kind)
	}
	return nil
}

func (s *dataBranchMergeSummary) observeKey(key string, side int, kind string) {
	// Bits 0/1 mark the two sides; bits 2/3 mark a deletion on that side.
	bit := uint8(1)
	if side == diffSideBase {
		bit = 2
	}
	old := s.keys[key]
	now := old | bit
	if kind == diffDelete {
		now |= bit << 2
	}
	if old&3 != 3 && now&3 == 3 && now&12 != 12 {
		s.conflicts++
	}
	s.keys[key] = now
}

func (s *dataBranchMergeSummary) releaseKeys() { s.keys = nil }

func (s *dataBranchMergeSummary) output(ctx context.Context, ses *Session) error {
	ses.ClearAllMysqlResultSet()
	ses.ClearResultBatches()
	mrs := ses.GetMysqlResultSet()
	for _, name := range []string{
		"source_inserted", "source_deleted", "source_updated",
		"destination_inserted", "destination_deleted", "destination_updated", "conflicts",
	} {
		col := new(MysqlColumn)
		col.SetName(name)
		col.SetColumnType(defines.MYSQL_TYPE_LONGLONG)
		mrs.AddColumn(col)
	}
	mrs.AddRow([]any{s.source[0], s.source[1], s.source[2],
		s.destination[0], s.destination[1], s.destination[2], s.conflicts})
	return trySaveQueryResult(ctx, ses, mrs)
}
