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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

type branchDAGReadExecutor struct {
	executor.SQLExecutor
	read func(context.Context, string) (executor.Result, error)
}

func (e *branchDAGReadExecutor) Exec(ctx context.Context, sql string, _ executor.Options) (executor.Result, error) {
	return e.read(ctx, sql)
}

// Use the real internal-executor path: mo_branch_metadata includes BOOL,
// which the generic MysqlResultSet test fallback cannot convert to a vector.
func branchDAGTestResult(t *testing.T, mp *mpool.MPool, rows ...[]interface{}) executor.Result {
	t.Helper()
	bat := batch.NewWithSize(5)
	for i, typ := range []types.T{types.T_uint64, types.T_int64, types.T_uint64, types.T_varchar, types.T_bool} {
		bat.Vecs[i] = vector.NewVec(typ.ToType())
	}
	for _, row := range rows {
		for i, value := range row {
			var err error
			switch v := value.(type) {
			case uint64:
				err = vector.AppendFixed(bat.Vecs[i], v, false, mp)
			case int64:
				err = vector.AppendFixed(bat.Vecs[i], v, false, mp)
			case bool:
				err = vector.AppendFixed(bat.Vecs[i], v, false, mp)
			case string:
				err = vector.AppendBytes(bat.Vecs[i], []byte(v), false, mp)
			default:
				t.Fatalf("unexpected DAG value %T", value)
			}
			require.NoError(t, err)
		}
	}
	bat.SetRowCount(len(rows))
	return executor.Result{Mp: mp, Batches: []*batch.Batch{bat}}
}

func TestBranchDAGCacheStatementScope(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		for _, empty := range []bool{false, true} {
			name := "private"
			if explicit {
				name = "explicit"
			}
			if empty {
				name += "_empty"
			}
			t.Run(name, func(t *testing.T) {
				ses := newValidateSession(t)
				oldCtx := ses.proc.Ctx
				ctx := defines.AttachAccountId(t.Context(), 42)
				queries := 0
				var rows [][]interface{}
				if !empty {
					rows = [][]interface{}{{uint64(2), int64(10), uint64(1), "table", false}}
				}
				read := &branchDAGReadExecutor{read: func(ctx context.Context, sql string) (executor.Result, error) {
					require.Equal(t, branchDAGSelectSQL(false), sql)
					account, err := defines.GetAccountId(ctx)
					require.NoError(t, err)
					require.Equal(t, uint32(catalog.System_Account), account)
					queries++
					return branchDAGTestResult(t, ses.proc.Mp(), rows...), nil
				}}
				bh := newPickStreamingBackExecForTest(t, ses, read)
				cache := newDataBranchDAGCache(explicit)
				if explicit {
					require.Nil(t, cache)
				}
				first, err := cache.get(ctx, ses, bh)
				require.NoError(t, err)
				second, err := cache.get(ctx, ses, bh)
				require.NoError(t, err)
				require.Equal(t, oldCtx, ses.proc.Ctx, "system-account context must be restored")
				if explicit {
					require.Equal(t, 2, queries)
					require.NotSame(t, first, second)
				} else {
					require.Equal(t, 1, queries)
					require.Same(t, first, second)
				}
				// New metadata after the operation must be observed by the next
				// request, even on the same session/executor and after an empty DAG.
				rows = [][]interface{}{{uint64(3), int64(20), uint64(2), "table", false}}
				fresh, err := newDataBranchDAGCache(explicit).get(ctx, ses, bh)
				require.NoError(t, err)
				require.True(t, fresh.HasParent(3))
				require.False(t, first.Exists(3))
				require.NotSame(t, first, fresh)
				want := 2
				if explicit {
					want = 3
				}
				require.Equal(t, want, queries)
			})
		}
	}
}

func TestBranchDAGCacheFailureAndCancellation(t *testing.T) {
	ses := newValidateSession(t)
	oldCtx := ses.proc.Ctx
	injected := errors.New("DAG read failed")
	queries := 0
	read := &branchDAGReadExecutor{read: func(_ context.Context, sql string) (executor.Result, error) {
		require.Equal(t, branchDAGSelectSQL(false), sql)
		queries++
		if queries == 1 {
			return executor.Result{}, injected
		}
		return branchDAGTestResult(t, ses.proc.Mp()), nil
	}}
	bh := newPickStreamingBackExecForTest(t, ses, read)
	cache := newDataBranchDAGCache(false)
	dag, err := cache.get(t.Context(), ses, bh)
	require.ErrorIs(t, err, injected)
	require.Nil(t, dag)
	require.Nil(t, cache.dag)
	require.Equal(t, oldCtx, ses.proc.Ctx)
	dag, err = cache.get(t.Context(), ses, bh)
	require.NoError(t, err)
	require.NotNil(t, dag)
	require.Equal(t, 2, queries)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	for _, candidate := range []*dataBranchDAGCache{cache, newDataBranchDAGCache(false), nil} {
		dag, err = candidate.get(ctx, ses, bh)
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, dag)
	}
	require.Equal(t, 2, queries, "cancellation must not load or return a cached DAG")
}
