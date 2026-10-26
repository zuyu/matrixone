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

func TestUpdateUniqueSourceDedupAndConstraints(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
		defer cancel()
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(1)
		name := "update_unique_source"
		q := func(sql string) { execSQLRequire(t, ctx, db, sql) }
		q("create database " + name)
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+name)
		q("create table " + name + ".t(tenant int,id int,v int,primary key(tenant,id),unique key uv(v))")
		q("create table " + name + ".s(tenant int,id int,v int,primary key(tenant,id))")
		q("insert into " + name + ".t values(1,1,10),(1,2,20),(2,1,30)")
		q("insert into " + name + ".s values(1,1,11),(1,2,NULL),(2,1,31)")
		defer db.ExecContext(context.Background(), "rollback")
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
		affected := func(sql string, want int64) {
			res, err := db.ExecContext(ctx, sql)
			require.NoError(t, err)
			n, err := res.RowsAffected()
			require.NoError(t, err)
			require.Equal(t, want, n)
		}
		original := read()
		update := "update " + name + ".t d join " + name + ".s s on d.tenant=s.tenant and d.id=s.id set d.v=s.v"
		q("begin")
		affected(update, 3)
		require.Equal(t, []string{"1:1:11", "1:2:NULL", "2:1:31"}, read())
		affected(update, 0)
		q("rollback")
		require.Equal(t, original, read())
		// A unique source does not imply unique newly assigned values.
		q("update " + name + ".s set v=99")
		_, err = db.ExecContext(ctx, update)
		require.Error(t, err)
		require.Equal(t, original, read())
		for _, join := range []string{
			name + ".s s on d.id=s.id", // missing one component of the source PK
			"(select tenant,id from " + name + ".s union all select tenant,id from " + name + ".s) s on d.tenant=s.tenant and d.id=s.id",
		} {
			q("begin")
			affected("update "+name+".t d join "+join+" set d.v=d.v+100", 3)
			require.Equal(t, []string{"1:1:110", "1:2:120", "2:1:130"}, read())
			q("rollback")
			require.Equal(t, original, read())
		}
		q("insert into " + name + ".s values(9,9,99)")
		q("begin")
		affected("update "+name+".s s left join "+name+".t d on d.tenant=s.tenant and d.id=s.id set d.v=d.v+1", 3)
		require.Equal(t, []string{"1:1:11", "1:2:21", "2:1:31"}, read())
		q("rollback")
		require.Equal(t, original, read())
	})
}
