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
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestSnapshotJoinUsesIndependentReadViews(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(1)
		for _, q := range []string{
			"create database snapshot_join", "use snapshot_join",
			"create table t(id int primary key,v int)", "insert into t values(1,10),(2,20)",
			"create snapshot snapshot_join_before for database snapshot_join",
			"update t set v=11 where id=1", "delete from t where id=2", "insert into t values(3,30)",
			"create snapshot snapshot_join_after for database snapshot_join",
		} {
			execSQLRequire(t, ctx, db, q)
		}
		defer cleanupSQLIntegration(t, cn, "drop database if exists snapshot_join")
		defer execSQLRequire(t, ctx, db, "drop snapshot snapshot_join_after")
		defer execSQLRequire(t, ctx, db, "drop snapshot snapshot_join_before")
		for _, hints := range [][2]string{{"snapshot='snapshot_join_before'", "snapshot='snapshot_join_after'"}} {
			before, after := "t{"+hints[0]+"}", "t{"+hints[1]+"}"
			for _, q := range []string{
				"select count(*) from " + before + " b left join " + after + " a on a.id=b.id where a.id is null or not(a.v <=> b.v)",
				"select count(*) from " + after + " a right join " + before + " b on a.id=b.id where a.id is null or not(a.v <=> b.v)",
			} {
				var count int
				require.NoError(t, db.QueryRowContext(ctx, q).Scan(&count))
				require.Equal(t, 2, count)
			}
			var added int
			require.NoError(t, db.QueryRowContext(ctx, "select count(*) from "+after+" a left join "+before+" b on a.id=b.id where b.id is null").Scan(&added))
			require.Equal(t, 1, added)
			// DML must retain both historical scan timestamps too.
			execSQLRequire(t, ctx, db, "create table delta(id int primary key,v int)")
			execSQLRequire(t, ctx, db, "insert into delta select b.* from "+before+" b left join "+after+" a on a.id=b.id where a.id is null or not(a.v <=> b.v)")
			var count, sum int
			require.NoError(t, db.QueryRowContext(ctx, "select count(*),sum(v) from delta").Scan(&count, &sum))
			require.Equal(t, 2, count)
			require.Equal(t, 30, sum)
		}
	})
}
