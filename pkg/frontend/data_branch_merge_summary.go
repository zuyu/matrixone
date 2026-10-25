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
	"bytes"
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/frontend/databranchutils"
)

// dataBranchMergeSummary describes the logical pre-merge differences, using
// the same source/destination orientation as DIFF source AGAINST destination.
// Counts include conflicts, independent of SKIP/ACCEPT. Equal changes and
// rows deleted on both sides are not conflicts. Observe logical changes
// before update expansion and conflict filtering, within the existing apply
// traversal. Reuse retained destination batches to exclude equal updates.
// Only unmatched PKs and pending equal-change markers are retained here;
// row values stay owned by the matcher. Plain MERGE does not collect counts.
type dataBranchMergeSummary struct {
	source             [3]uint64
	destination        [3]uint64
	conflicts          uint64
	keys               map[string]uint8
	equalPending       int
	failOnEqualOverlap bool
	equalConflict      error
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

	if now&3 == 3 {
		if now&12 != 12 {
			s.conflicts++
		}
		delete(s.keys, key)
	} else {
		s.keys[key] = now
	}
}

const mergeSummaryEqual = 16

// Destination batches are sorted and retained by the merge matcher. Inspect
// them before delivery of the source's expanded delete/insert can consume them.
func (s *dataBranchMergeSummary) observeSourceChanges(
	ctx context.Context, ses *Session, tables tableStuff, w batchWithKind,
	deletes, updates []batchWithKind,
) error {
	candidates, index := updates, 2
	if w.kind == diffDelete {
		candidates, index = deletes, 1
	}
	if len(candidates) == 0 || w.batch.RowCount() == 0 {
		return s.observe(ses, tables, w)
	}
	if err := sortDataBranchBatchByPrimaryKey(w.batch, tables.def.pkColIdx, ses.proc.Mp()); err != nil {
		return err
	}
	s.source[index] += uint64(w.batch.RowCount())
	equal := make([]bool, w.batch.RowCount())
	// Walk sorted batches rather than probing every destination batch for
	// each source row. Equal rows need no conflict-key insertion/probe.
	for _, base := range candidates {
		if w.kind == diffDelete && base.fromUpdate {
			continue
		}
		srcVec, dstVec := w.batch.Vecs[tables.def.pkColIdx], base.batch.Vecs[tables.def.pkColIdx]
		i, j := 0, 0
		for i < srcVec.Length() && j < dstVec.Length() {
			cmp, err := compareDataBranchPrimaryKeyInVectors(ctx, ses, i, j, srcVec, dstVec)
			if err != nil {
				return err
			}
			if cmp < 0 {
				i++
				continue
			}
			if cmp > 0 {
				j++
				continue
			}
			if !equal[i] {
				base.kind = w.kind // the retained update is physically an INSERT
				cmp, err = compareRowInWrappedBatches(ctx, ses, tables, i, j, true, w, base)
				if err != nil {
					return err
				}
				if cmp == 0 {
					key, err := extractPKAsString(ses, tables, w.batch, i)
					if err != nil {
						return err
					}
					s.source[index]--
					s.destination[index]--
					s.keys[key] = mergeSummaryEqual
					s.equalPending++
					equal[i] = true
					// Preserve the previous no-op success under FAIL;
					// otherwise plain MERGE's equal-overlap error applies.
					if s.failOnEqualOverlap && s.equalConflict == nil {
						s.equalConflict = moerr.NewInternalErrorNoCtxf(
							"conflict: %s %s and %s %s on pk(%v)", w.name, w.kind, base.name, w.kind, key)
					}
				}
			}
			i++
			j++
		}
	}
	if tables.def.pkKind != fakeKind {
		for row := range w.batch.RowCount() {
			if equal[row] {
				continue
			}
			key, err := extractPKAsString(ses, tables, w.batch, row)
			if err != nil {
				return err
			}
			s.observeKey(key, diffSideTarget, w.kind)
		}
	}
	return nil
}

// An equal logical UPDATE has two physical pieces. Keep its marker until
// both have passed; an equal DELETE only has one. No row values are copied.
func (s *dataBranchMergeSummary) filterEqual(ses *Session, tables tableStuff, w batchWithKind) error {
	if s.equalPending == 0 {
		return nil
	}
	var remove []int64
	for row := range w.batch.RowCount() {
		key, err := extractPKAsString(ses, tables, w.batch, row)
		if err != nil {
			return err
		}
		if s.keys[key] != mergeSummaryEqual {
			continue
		}
		remove = append(remove, int64(row))
		if w.kind != diffDelete || !w.fromUpdate {
			delete(s.keys, key)
			s.equalPending--
		}
	}
	if len(remove) != 0 {
		w.batch.Shrink(remove, true)
	}
	return nil
}

func mergeSummaryTupleKey(ses *Session, tables tableStuff, tuple types.Tuple) (string, error) {
	var buf bytes.Buffer
	for i, idx := range tables.def.pkColIdxes {
		if i > 0 {
			buf.WriteByte(',')
		}
		value, err := getTupleColumnValue(tuple, tables, idx)
		if err != nil {
			return "", err
		}
		if value == nil {
			buf.WriteString("NULL")
			continue
		}
		if err := formatValIntoString(ses, value, tables.def.colTypes[idx], &buf); err != nil {
			return "", err
		}
	}
	return buf.String(), nil
}

func (s *dataBranchMergeSummary) observeInsert(ses *Session, tables tableStuff, tuple types.Tuple, side int) error {
	if side == diffSideTarget {
		s.source[0]++
	} else {
		s.destination[0]++
	}
	if tables.def.pkKind == fakeKind {
		return nil
	}
	key, err := mergeSummaryTupleKey(ses, tables, tuple)
	if err != nil {
		return err
	}
	s.observeKey(key, side, diffInsert)
	return nil
}

func (s *dataBranchMergeSummary) observeDestinationInserts(
	ctx context.Context, ses *Session, tables tableStuff, data databranchutils.BranchHashmap,
) error {
	s.destination[0] += uint64(data.ItemCount())
	if tables.def.pkKind == fakeKind || s.source == [3]uint64{} {
		return nil
	}
	// These rows cannot match another INSERT: the source loop already
	// consumed those. Only a source UPDATE/DELETE can still conflict.
	return data.ForEachShardParallel(func(cursor databranchutils.ShardCursor) error {
		return cursor.ForEach(func(_ []byte, row []byte) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			tuple, _, err := data.DecodeRow(row)
			if err != nil {
				return err
			}
			key, err := mergeSummaryTupleKey(ses, tables, tuple)
			if err != nil {
				return err
			}
			if s.keys[key]&1 != 0 {
				s.conflicts++
				delete(s.keys, key)
			}
			return nil
		})
	}, 1)
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
