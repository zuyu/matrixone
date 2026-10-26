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
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

func TestDataBranchRestoreDeleteSingleCurrentScan(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
		defer cancel()
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(1)
		for _, kind := range []string{"single", "composite", "empty"} {
			t.Run(kind, func(t *testing.T) {
				name := "restore_delete_scan_" + kind
				q := func(sql string) { execSQLRequire(t, ctx, db, sql) }
				q("create database " + name)
				defer cleanupSQLIntegration(t, cn, "drop database if exists "+name)
				keys, join, marker := "id", "d.id=s.id", "s.id"
				initial := "(1,1,10),(1,2,NULL),(2,3,30),(2,4,40)"
				if kind == "composite" {
					keys, join, marker = "tenant,id", "d.tenant=s.tenant and d.id=s.id", "s.tenant"
					initial = "(1,1,10),(1,2,NULL),(2,1,30),(2,2,40)"
				}
				q("create table " + name + ".t(tenant int,id bigint,v int,primary key(" + keys + "),unique key u(v))")
				if kind != "empty" {
					q("insert into " + name + ".t values" + initial)
				}
				read := func() []string {
					rows, err := db.QueryContext(ctx, "select concat(tenant,':',id,':',ifnull(cast(v as char),'NULL')) from "+name+".t order by tenant,id")
					require.NoError(t, err)
					defer rows.Close()
					var result []string
					for rows.Next() {
						var value string
						require.NoError(t, rows.Scan(&value))
						result = append(result, value)
					}
					require.NoError(t, rows.Err())
					return result
				}
				before := read()
				q("create snapshot " + name + "_snap for database " + name)
				defer q("drop snapshot " + name + "_snap")
				q("update " + name + ".t set v=11 where tenant=1 and id=1")
				q("delete from " + name + ".t where tenant=1 and id=2")
				kept := read()
				q("insert into " + name + ".t values(9,9,90),(9,10,NULL)")
				original := read()
				var ts string
				require.NoError(t, db.QueryRowContext(ctx, "select ts from mo_catalog.mo_snapshots where sname='"+name+"_snap'").Scan(&ts))
				deleteSQL := fmt.Sprintf("delete d from %[1]s.t d left join (select %[2]s from %[1]s.t{MO_TS=%[3]s}) s on %[4]s where %[5]s is null", name, keys, ts, join, marker)
				rows, err := db.QueryContext(ctx, "explain verbose "+deleteSQL)
				require.NoError(t, err)
				var explain strings.Builder
				baseScans := 0
				for rows.Next() {
					var line string
					require.NoError(t, rows.Scan(&line))
					explain.WriteString(line + "\n")
					if strings.Contains(line, "Table Scan[") && strings.Contains(line, " on "+name+".t ") {
						baseScans++
					}
				}
				require.NoError(t, rows.Err())
				require.NoError(t, rows.Close())
				require.Equal(t, 2, baseScans, explain.String())
				require.Contains(t, explain.String(), "Join Type: ANTI")
				defer db.ExecContext(context.Background(), "rollback")
				q("begin")
				for _, want := range []int64{2, 0} {
					res, err := db.ExecContext(ctx, deleteSQL)
					require.NoError(t, err)
					affected, err := res.RowsAffected()
					require.NoError(t, err)
					require.Equal(t, want, affected)
					require.Equal(t, kept, read(), "DELETE must retain current values and leave missing keys for INSERT")
				}
				q("rollback")
				require.Equal(t, original, read())
				restore := fmt.Sprintf("data branch reset database %[1]s from %[1]s{snapshot='%[1]s_snap'} tables(t)", name)
				q("begin")
				q(restore)
				require.Equal(t, before, read())
				q(restore)
				require.Equal(t, before, read())
				q("rollback")
				require.Equal(t, original, read())
				q(restore)
				require.Equal(t, before, read())
			})
		}
	})
}
