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

	"github.com/matrixorigin/matrixone/pkg/frontend/databranchutils"
)

// A database DIFF/MERGE's private owner holds the lineage lifecycle gate
// throughout the statement. Its table workers change data and ordinary stages,
// not branch edges, so one immutable DAG serves the serial table loop. Never
// retain this cache on a session or reuse it for clone, ALTER, DROP or restore.
// Explicit transactions do not hold that gate here; keep their existing reads.
type dataBranchDAGCache struct {
	dag *databranchutils.DataBranchDAG
}

func newDataBranchDAGCache(explicitTxn bool) *dataBranchDAGCache {
	if explicitTxn {
		return nil
	}
	return &dataBranchDAGCache{}
}

func (cache *dataBranchDAGCache) get(ctx context.Context, ses *Session, bh BackgroundExec) (*databranchutils.DataBranchDAG, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if cache != nil && cache.dag != nil {
		return cache.dag, nil
	}
	// Lazy loading preserves the no-query path for empty databases, one-sided
	// DIFF tables, or endpoint/worker validation errors. Failed loads are never
	// cached; the caller still aborts the operation on the original error.
	dag, err := constructBranchDAG(ctx, ses, bh)
	if err == nil && cache != nil {
		cache.dag = dag
	}
	return dag, err
}
