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
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// Match the integer-only MERGE fast path. Requiring the same key type at every
// endpoint avoids changing CTAS coercion, signedness or collation semantics.
func branchRestoreIntegerKeys(keys []string, defs ...*plan.TableDef) []bool {
	if len(keys) == 0 || len(keys) > maxInlineDeleteCells || len(defs) == 0 {
		return nil
	}
	unsigned := make([]bool, len(keys))
	for i, key := range keys {
		var first int32
		for d, def := range defs {
			var col *plan.ColDef
			for _, c := range def.Cols {
				if c.Name == key {
					col = c
					break
				}
			}
			if col == nil || (d > 0 && col.Typ.Id != first) {
				return nil
			}
			first = col.Typ.Id
			switch types.T(first) {
			case types.T_int8, types.T_int16, types.T_int32, types.T_int64:
			case types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64:
				unsigned[i] = true
			default:
				return nil
			}
		}
	}
	return unsigned
}

// Materialize at most 128 rows / 512 integer cells per table in server memory.
// The extra row detects overflow; a larger delta keeps the existing temporary
// CTAS path. All probes and CTASs finish before any target write. This avoids
// same-table snapshot DELETE limitations and never sends keys to the client.
func (w *branchRestoreWork) probeDeleteKeys(ctx context.Context, bh BackgroundExec, p *branchRestorePlan) (bool, error) {
	p.deleteValues = ""
	limit := min(maxInlineDeleteRows, maxInlineDeleteCells/len(p.deleteKeyUnsigned))
	bh.ClearExecResultSet()
	defer bh.ClearExecResultSet()
	bh.ClearExecResultBatches()
	defer bh.ClearExecResultBatches()
	if err := w.exec(ctx, bh, "delete_probe", p.deletesQuery+" limit "+strconv.Itoa(limit+1)); err != nil {
		return false, err
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	results, err := getResultSet(ctx, bh)
	if err != nil {
		return false, err
	}
	if len(results) > 1 || (len(results) == 1 && results[0].GetColumnCount() != uint64(len(p.keys))) {
		return false, moerr.NewInternalError(ctx, "unexpected restore delete probe result")
	}
	// The background row callback may publish no result set for an empty
	// pipeline. A successful SELECT with no result is an empty delta.
	var result ExecResult
	var rows uint64
	if len(results) == 1 {
		result = results[0]
		rows = result.GetRowCount()
	}
	if rows > uint64(limit) {
		w.fallbackDeletes++
		return false, nil
	}
	var values strings.Builder
	for row := uint64(0); row < rows; row++ {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		if row > 0 {
			values.WriteByte(',')
		}
		if len(p.keys) > 1 {
			values.WriteByte('(')
		}
		for col, unsigned := range p.deleteKeyUnsigned {
			if col > 0 {
				values.WriteByte(',')
			}
			null, err := result.ColumnIsNull(ctx, row, uint64(col))
			if err != nil {
				return false, err
			}
			if null {
				return false, moerr.NewInternalError(ctx, "NULL restore primary key")
			}
			if unsigned {
				v, err := result.GetUint64(ctx, row, uint64(col))
				if err != nil {
					return false, err
				}
				values.WriteString(strconv.FormatUint(v, 10))
			} else {
				v, err := result.GetInt64(ctx, row, uint64(col))
				if err != nil {
					return false, err
				}
				values.WriteString(strconv.FormatInt(v, 10))
			}
		}
		if len(p.keys) > 1 {
			values.WriteByte(')')
		}
	}
	p.deleteValues = values.String()
	p.deletesEmpty = rows == 0
	w.inlineDeletes++
	w.materializedRows += int64(rows)
	if rows == 0 {
		w.emptyStages++
	}
	return true, nil
}
