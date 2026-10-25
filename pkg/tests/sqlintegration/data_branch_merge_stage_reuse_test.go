// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sqlintegration

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

func TestDataBranchMergeStageReuse(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
		defer cancel()
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(1)
		exec := func(q string) { execSQLRequire(t, ctx, db, q) }
		exec("create database stage_reuse_dst")
		defer cleanupSQLIntegration(t, cn, "drop database if exists stage_reuse_dst")
		// Both change sets exceed the 81,920-row SQL flush limit, exercising
		// reuse of each stage before its final nonempty DROP.
		const n, deleted = 90000, 85000
		for _, name := range []string{"u", "d"} {
			exec("create table stage_reuse_dst." + name + "(id int primary key,v int)")
			exec(fmt.Sprintf("insert into stage_reuse_dst.%s select result,result from generate_series(1,%d) g", name, n))
		}
		exec("data branch create database stage_reuse_src from stage_reuse_dst")
		defer cleanupSQLIntegration(t, cn, "drop database if exists stage_reuse_src")
		exec("update stage_reuse_src.u set v=v+1")
		exec(fmt.Sprintf("delete from stage_reuse_src.d where id<=%d", deleted))

		merge := func(tables string) map[string][7]uint64 {
			rows, err := db.QueryContext(ctx, "data branch merge database stage_reuse_src into stage_reuse_dst tables("+tables+") when conflict accept output summary")
			require.NoError(t, err)
			defer rows.Close()
			out := map[string][7]uint64{}
			for rows.Next() {
				var name string
				var existsA, existsB uint64
				var counts [7]uint64
				dests := []any{&name, &existsA, &existsB}
				for i := range counts {
					dests = append(dests, &counts[i])
				}
				require.NoError(t, rows.Scan(dests...))
				out[name] = counts
			}
			require.NoError(t, rows.Err())
			return out
		}
		checkMerged := func() {
			require.Equal(t, map[string][7]uint64{
				"u": {0, 0, n, 0, 0, 0, 0}, "d": {0, deleted, 0, 0, 0, 0, 0},
			}, merge("u,d"))
			var count, sum, minID, maxID int64
			require.NoError(t, db.QueryRowContext(ctx, "select count(*),sum(v) from stage_reuse_dst.u").Scan(&count, &sum))
			require.Equal(t, int64(n), count)
			require.Equal(t, int64(n*(n+1)/2+n), sum)
			require.NoError(t, db.QueryRowContext(ctx, "select count(*),sum(v),min(id),max(id) from stage_reuse_dst.d").Scan(&count, &sum, &minID, &maxID))
			require.Equal(t, int64(n-deleted), count)
			require.Equal(t, int64(n*(n+1)/2-deleted*(deleted+1)/2), sum)
			require.Equal(t, int64(deleted+1), minID)
			require.Equal(t, int64(n), maxID)
		}

		// Reusing and dropping populated stages must stay in the owner's
		// transaction: both tables return to their original rows on rollback.
		exec("begin")
		defer db.ExecContext(context.Background(), "rollback")
		checkMerged()
		exec("rollback")
		for _, name := range []string{"u", "d"} {
			var count, sum int64
			require.NoError(t, db.QueryRowContext(ctx, "select count(*),sum(v) from stage_reuse_dst."+name).Scan(&count, &sum))
			require.Equal(t, int64(n), count)
			require.Equal(t, int64(n*(n+1)/2), sum)
		}
		checkMerged()
		rows, err := db.QueryContext(ctx, "show tables from stage_reuse_dst like '__mo_diff_%'")
		require.NoError(t, err)
		defer rows.Close()
		require.False(t, rows.Next(), "no stage may survive a completed merge")
		require.NoError(t, rows.Err())
	})
}
