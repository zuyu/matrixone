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

func TestDataBranchRestoreRowsSubqueryReevaluatesTarget(t *testing.T) {
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
				name := "restore_rows_" + op
				q := func(sql string) { execSQLRequire(t, ctx, db, sql) }
				q("create database " + name)
				defer cleanupSQLIntegration(t, cn, "drop database if exists "+name)
				q("create table " + name + ".t(id int primary key,v int)")
				q("insert into " + name + ".t values(1,10),(2,20),(3,NULL),(4,40)")
				q("create snapshot " + name + "_before for database " + name)
				defer q("drop snapshot " + name + "_before")
				q("update " + name + ".t set v=11 where id=1")
				q("delete from " + name + ".t where id=2")
				q("insert into " + name + ".t values(5,50)")
				q("create snapshot " + name + "_after for database " + name)
				defer q("drop snapshot " + name + "_after")
				defer db.ExecContext(context.Background(), "rollback")
				// Current membership differs from both snapshots. UPDATE must repair
				// id=2, INSERT must restore id=1, and REVERT must preserve ids=4/6
				// and the later deletion of unchanged id=3.
				q("delete from " + name + ".t where id=1 or id=3")
				q("insert into " + name + ".t values(2,222),(6,60)")
				q("update " + name + ".t set v=400 where id=4")
				read := func() []string {
					rows, err := db.QueryContext(ctx, "select concat(id,':',ifnull(cast(v as char),'NULL')) from "+name+".t order by id")
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
				want := []string{"1:10", "2:20", "3:NULL", "4:40"}
				if op == "revert" {
					restore = fmt.Sprintf("data branch revert database %[1]s from %[1]s{snapshot='%[1]s_after'} against %[1]s{snapshot='%[1]s_before'}", name)
					want = []string{"1:10", "2:20", "4:400", "6:60"}
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

func TestDataBranchRestoreRowsAfterForeignKeyCascade(t *testing.T) {
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
				name := "restore_rows_fk_" + op
				q := func(sql string) { execSQLRequire(t, ctx, db, sql) }
				q("create database " + name)
				defer cleanupSQLIntegration(t, cn, "drop database if exists "+name)
				q("create table " + name + ".a_parent(id int primary key)")
				q("create table " + name + ".z_child(id int primary key,pid int,foreign key(pid) references " + name + ".a_parent(id) on update cascade on delete cascade)")
				q("insert into " + name + ".a_parent values(1)")
				q("insert into " + name + ".z_child values(10,1)")
				q("create snapshot " + name + "_before for database " + name)
				defer q("drop snapshot " + name + "_before")
				q("update " + name + ".a_parent set id=2 where id=1")
				q("create snapshot " + name + "_after for database " + name)
				defer q("drop snapshot " + name + "_after")
				defer db.ExecContext(context.Background(), "rollback")
				restore := fmt.Sprintf("data branch reset database %[1]s from %[1]s{snapshot='%[1]s_before'}", name)
				if op == "revert" {
					restore = fmt.Sprintf("data branch revert database %[1]s from %[1]s{snapshot='%[1]s_after'} against %[1]s{snapshot='%[1]s_before'}", name)
				}
				verify := func(want int) {
					var parent, id, pid int
					require.NoError(t, db.QueryRowContext(ctx, "select id from "+name+".a_parent").Scan(&parent))
					require.NoError(t, db.QueryRowContext(ctx, "select id,pid from "+name+".z_child").Scan(&id, &pid))
					require.Equal(t, want, parent)
					require.Equal(t, 10, id)
					require.Equal(t, want, pid)
					var stages int
					require.NoError(t, db.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_tables where reldatabase=? and relname like '%__mo_branch_restore_%'", name).Scan(&stages))
					require.Zero(t, stages)
				}
				// Deleting the new parent cascades away the current child. Its
				// before-image must still be inserted when the later table applies.
				q("begin")
				q(restore)
				verify(1)
				q("rollback")
				verify(2)
				q(restore)
				verify(1)
				q(restore)
				verify(1)
			})
		}
	})
}
