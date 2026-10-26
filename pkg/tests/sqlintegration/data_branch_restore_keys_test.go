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
	"math"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

func TestDataBranchRestoreDeleteSubqueryKeys(t *testing.T) {
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
				name := "restore_keys_" + op
				q := func(s string) { execSQLRequire(t, ctx, db, s) }
				q("create database " + name)
				defer cleanupSQLIntegration(t, cn, "drop database if exists "+name)
				cases := []struct {
					name      string
					rows      int
					composite bool
				}{
					{"empty", 0, false}, {"rows_128", 128, false}, {"rows_129", 129, false}, {"rows_300", 300, false},
					{"composite_102", 102, true}, {"composite_103", 103, true}, {"composite_300", 300, true},
				}
				ids := map[string]uint64{}
				for _, tc := range cases {
					def := "(id bigint unsigned primary key,v int)"
					if tc.composite {
						def = "(a bigint,b bigint unsigned,c smallint,d tinyint,e int,v int,primary key(a,b,c,d,e))"
					}
					q("create table " + name + "." + tc.name + def)
					var id uint64
					require.NoError(t, db.QueryRowContext(ctx, "select rel_id from mo_catalog.mo_tables where reldatabase=? and relname=?", name, tc.name).Scan(&id))
					ids[tc.name] = id
				}
				q("create snapshot " + name + "_before for database " + name)
				defer q("drop snapshot " + name + "_before")
				for _, tc := range cases {
					var values []string
					for i := 0; i < tc.rows; i++ {
						v := fmt.Sprintf("(%d,%d)", uint64(math.MaxUint64)-uint64(i), i)
						if tc.composite {
							v = fmt.Sprintf("(%d,%d,1,2,3,%d)", int64(math.MinInt64)+int64(i), uint64(math.MaxUint64)-uint64(i), i)
						}
						values = append(values, v)
					}
					if len(values) > 0 {
						q("insert into " + name + "." + tc.name + " values " + strings.Join(values, ","))
					}
				}
				q("create snapshot " + name + "_after for database " + name)
				defer q("drop snapshot " + name + "_after")
				restore := fmt.Sprintf("data branch reset database %[1]s from %[1]s{snapshot='%[1]s_before'}", name)
				if op == "revert" {
					restore = fmt.Sprintf("data branch revert database %[1]s from %[1]s{snapshot='%[1]s_after'} against %[1]s{snapshot='%[1]s_before'}", name)
					for _, tc := range cases {
						v := "(1,99)"
						if tc.composite {
							v = "(1,1,1,2,3,99)"
						}
						q("insert into " + name + "." + tc.name + " values " + v)
					}
				}
				verify := func(restored bool) {
					for _, tc := range cases {
						want := 0
						if op == "revert" {
							want++
						}
						if !restored {
							want += tc.rows
						}
						var count int
						require.NoError(t, db.QueryRowContext(ctx, "select count(*) from "+name+"."+tc.name).Scan(&count))
						require.Equal(t, want, count, tc.name)
						if restored && op == "revert" {
							var v int
							require.NoError(t, db.QueryRowContext(ctx, "select v from "+name+"."+tc.name).Scan(&v))
							require.Equal(t, 99, v)
						}
						var id uint64
						require.NoError(t, db.QueryRowContext(ctx, "select rel_id from mo_catalog.mo_tables where reldatabase=? and relname=?", name, tc.name).Scan(&id))
						require.Equal(t, ids[tc.name], id)
					}
					var stages int
					require.NoError(t, db.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_tables where reldatabase=? and relname like '%__mo_branch_restore_%'", name).Scan(&stages))
					require.Zero(t, stages)
				}
				execSQLRequire(t, ctx, db, "begin")
				q(restore)
				verify(true)
				execSQLRequire(t, ctx, db, "rollback")
				verify(false)
				q(restore)
				verify(true)
				q(restore)
				verify(true)
			})
		}
	})
}

func TestDataBranchRestoreSubqueryAtomicityAndForeignKeys(t *testing.T) {
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
				name := "restore_subquery_" + op
				q := func(statement string) { execSQLRequire(t, ctx, db, fmt.Sprintf(statement, name)) }
				q("create database %[1]s")
				defer cleanupSQLIntegration(t, cn, "drop database if exists "+name)
				for _, statement := range []string{
					"create table %[1]s.a_plain(id int primary key,v int)",
					"insert into %[1]s.a_plain values(1,10)",
					"create table %[1]s.p_parent(id int primary key)",
					"create table %[1]s.q_child(id int primary key,pid int,foreign key(pid) references %[1]s.p_parent(id) on delete cascade on update cascade)",
					"insert into %[1]s.p_parent values(1)",
					"insert into %[1]s.q_child values(1,1)",
					"create table %[1]s.z_unique(id int primary key,v int)",
					"insert into %[1]s.z_unique values(1,10),(2,10)",
					"create snapshot %[1]s_before for database %[1]s",
				} {
					q(statement)
				}
				defer q("drop snapshot %[1]s_before")
				for _, statement := range []string{
					"insert into %[1]s.a_plain values(2,20)",
					"insert into %[1]s.p_parent values(2)",
					"insert into %[1]s.q_child values(2,2)",
					"update %[1]s.z_unique set v=20 where id=2",
					"create unique index uq_restore on %[1]s.z_unique(v)",
					"create snapshot %[1]s_after for database %[1]s",
				} {
					q(statement)
				}
				defer q("drop snapshot %[1]s_after")
				defer db.ExecContext(context.Background(), "rollback")
				restore := fmt.Sprintf("data branch reset database %[1]s from %[1]s{snapshot='%[1]s_before'}", name)
				if op == "revert" {
					restore = fmt.Sprintf("data branch revert database %[1]s from %[1]s{snapshot='%[1]s_after'} against %[1]s{snapshot='%[1]s_before'}", name)
				}
				count := func(table string) int {
					var n int
					require.NoError(t, db.QueryRowContext(ctx, "select count(*) from "+name+"."+table).Scan(&n))
					return n
				}
				// A later unique failure must undo the earlier subquery DELETE.
				_, err = db.ExecContext(ctx, restore+" tables(a_plain,z_unique)")
				require.Error(t, err)
				require.Equal(t, 2, count("a_plain"))
				var sum int
				require.NoError(t, db.QueryRowContext(ctx, "select sum(v) from "+name+".z_unique").Scan(&sum))
				require.Equal(t, 30, sum)
				// Parent deletion cascades into a later selected child table.
				execSQLRequire(t, ctx, db, "begin")
				execSQLRequire(t, ctx, db, restore+" tables(p_parent,q_child)")
				require.Equal(t, 1, count("p_parent"))
				require.Equal(t, 1, count("q_child"))
				execSQLRequire(t, ctx, db, "rollback")
				require.Equal(t, 2, count("p_parent"))
				require.Equal(t, 2, count("q_child"))
				execSQLRequire(t, ctx, db, restore+" tables(p_parent,q_child)")
				require.Equal(t, 1, count("p_parent"))
				require.Equal(t, 1, count("q_child"))
				var stages int
				require.NoError(t, db.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_tables where reldatabase=? and relname like '%__mo_branch_restore_%'", name).Scan(&stages))
				require.Zero(t, stages)
			})
		}
	})
}
