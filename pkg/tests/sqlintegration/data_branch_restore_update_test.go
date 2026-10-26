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

func TestDataBranchRestoreUpdateCompositeNullAndRepeat(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
		defer cancel()
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(1)
		for _, op := range []string{"reset", "revert"} {
			t.Run(op, func(t *testing.T) {
				name := "restore_update_" + op
				q := func(sql string) { execSQLRequire(t, ctx, db, sql) }
				q("create database " + name)
				defer cleanupSQLIntegration(t, cn, "drop database if exists "+name)
				q("create table " + name + ".t(tenant int,id bigint,v int,primary key(tenant,id))")
				q("insert into " + name + ".t values(1,1,NULL),(1,2,20),(2,1,30),(2,2,40),(3,1,50),(3,2,60)")
				q("create snapshot " + name + "_before for database " + name)
				defer q("drop snapshot " + name + "_before")
				q("update " + name + ".t set v=10 where tenant=1 and id=1")
				q("update " + name + ".t set v=NULL where tenant=1 and id=2")
				q("update " + name + ".t set v=31 where tenant=2 and id=1")
				q("delete from " + name + ".t where tenant=2 and id=2")
				q("insert into " + name + ".t values(9,9,99)")
				q("create snapshot " + name + "_after for database " + name)
				defer q("drop snapshot " + name + "_after")
				defer db.ExecContext(context.Background(), "rollback")
				// One candidate is already restored. REVERT must preserve later
				// edits/deletions outside the before/after delta, including on repeat.
				q("update " + name + ".t set v=30 where tenant=2 and id=1")
				q("delete from " + name + ".t where tenant=3 and id=1")
				q("update " + name + ".t set v=600 where tenant=3 and id=2")
				read := func() []string {
					rows, err := db.QueryContext(ctx, "select concat(tenant,':',id,':',ifnull(cast(v as char),'NULL')) from "+name+".t order by tenant,id")
					require.NoError(t, err)
					defer rows.Close()
					var values []string
					for rows.Next() {
						var v string
						require.NoError(t, rows.Scan(&v))
						values = append(values, v)
					}
					require.NoError(t, rows.Err())
					return values
				}
				original := read()
				restore := fmt.Sprintf("data branch reset database %[1]s from %[1]s{snapshot='%[1]s_before'}", name)
				want := []string{"1:1:NULL", "1:2:20", "2:1:30", "2:2:40", "3:1:50", "3:2:60"}
				if op == "revert" {
					restore = fmt.Sprintf("data branch revert database %[1]s from %[1]s{snapshot='%[1]s_after'} against %[1]s{snapshot='%[1]s_before'}", name)
					want = []string{"1:1:NULL", "1:2:20", "2:1:30", "2:2:40", "3:2:600"}
				}
				q("begin")
				q(restore)
				require.Equal(t, want, read())
				q(restore)
				require.Equal(t, want, read())
				q("rollback")
				require.Equal(t, original, read())
				q(restore)
				require.Equal(t, want, read())
				q(restore)
				require.Equal(t, want, read())
			})
		}
	})
}

func TestDataBranchRestoreUpdatePreservesCollatedBeforeImage(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
		defer cancel()
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(1)
		name := "restore_update_collation"
		q := func(sql string) { execSQLRequire(t, ctx, db, sql) }
		q("create database " + name)
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+name)
		q("create table " + name + ".t(id int primary key,v varchar(20) collate utf8mb4_unicode_ci)")
		q("insert into " + name + ".t values(1,'Alpha'),(2,'stable')")
		q("create snapshot " + name + "_before for database " + name)
		defer q("drop snapshot " + name + "_before")
		q("update " + name + ".t set v='Beta' where id=1")
		q("create snapshot " + name + "_after for database " + name)
		defer q("drop snapshot " + name + "_after")
		defer db.ExecContext(context.Background(), "rollback")
		q("update " + name + ".t set v='alpha' where id=1")
		q("update " + name + ".t set v='later' where id=2")
		// Equality under this collation cannot replace assigning the exact
		// before-image: alpha compares equal but must be restored to Alpha.
		var equal bool
		require.NoError(t, db.QueryRowContext(ctx, "select v <=> 'Alpha' from "+name+".t where id=1").Scan(&equal))
		require.True(t, equal)
		verify := func(want string) {
			var first, second string
			require.NoError(t, db.QueryRowContext(ctx, "select v from "+name+".t where id=1").Scan(&first))
			require.NoError(t, db.QueryRowContext(ctx, "select v from "+name+".t where id=2").Scan(&second))
			require.Equal(t, want, first)
			require.Equal(t, "later", second)
		}
		restore := fmt.Sprintf("data branch revert database %[1]s from %[1]s{snapshot='%[1]s_after'} against %[1]s{snapshot='%[1]s_before'}", name)
		q("begin")
		q(restore)
		verify("Alpha")
		q(restore)
		verify("Alpha")
		q("rollback")
		verify("alpha")
		q(restore)
		verify("Alpha")
		q(restore)
		verify("Alpha")
	})
}
