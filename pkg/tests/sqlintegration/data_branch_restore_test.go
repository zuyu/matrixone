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

func TestDataBranchDatabaseResetRevert(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
		defer cancel()
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(1)
		for _, q := range []string{
			"create database dbrestore",
			"create table dbrestore.a(id int primary key,v int)",
			"insert into dbrestore.a values(1,10),(2,20),(3,NULL),(4,40)",
			"create table dbrestore.z(id int primary key,v int)",
			"insert into dbrestore.z values(1,10),(2,20)",
			"create table dbrestore.heap(v int)",
			"insert into dbrestore.heap values(1),(1),(2)",
			"create table dbrestore.composite(a int,b varchar(20),v varchar(20),primary key(a,b))",
			"insert into dbrestore.composite values(1,'quote''key','old'),(2,'b',NULL)",
			"create snapshot dbrestore_before for database dbrestore",
		} {
			execSQLRequire(t, ctx, db, q)
		}
		defer cleanupSQLIntegration(t, cn, "drop database if exists dbrestore")
		defer execSQLRequire(t, ctx, db, "drop snapshot dbrestore_before")
		tableID := func() uint64 {
			var id uint64
			require.NoError(t, db.QueryRowContext(ctx, "select rel_id from mo_catalog.mo_tables where reldatabase='dbrestore' and relname='a'").Scan(&id))
			return id
		}
		id := tableID()
		var heapID uint64
		require.NoError(t, db.QueryRowContext(ctx, "select rel_id from mo_catalog.mo_tables where reldatabase='dbrestore' and relname='heap'").Scan(&heapID))
		values := func(query string) []string {
			rows, err := db.QueryContext(ctx, query)
			require.NoError(t, err)
			defer rows.Close()
			var out []string
			for rows.Next() {
				var v string
				require.NoError(t, rows.Scan(&v))
				out = append(out, v)
			}
			require.NoError(t, rows.Err())
			return out
		}
		readA := func() []string {
			return values("select concat(id,':',ifnull(cast(v as char),'NULL')) from dbrestore.a order by id")
		}
		original := readA()
		for _, q := range []string{
			"update dbrestore.a set v=11 where id=1", "delete from dbrestore.a where id=2", "insert into dbrestore.a values(5,50)",
			"update dbrestore.z set v=21 where id=1",
			"update dbrestore.composite set v='new' where a=1",
			"create snapshot dbrestore_after for database dbrestore",
			"update dbrestore.a set v=100 where id=1", "update dbrestore.a set v=400 where id=4", "insert into dbrestore.a values(6,60)",
		} {
			execSQLRequire(t, ctx, db, q)
		}
		defer execSQLRequire(t, ctx, db, "drop snapshot dbrestore_after")
		execSQLRequire(t, ctx, db, "data branch revert database dbrestore from dbrestore{snapshot='dbrestore_after'} against dbrestore{snapshot='dbrestore_before'} tables (a,z,composite)")
		require.Equal(t, []string{"1:10", "2:20", "3:NULL", "4:400", "6:60"}, readA())
		require.Equal(t, []string{"old"}, values("select v from dbrestore.composite where a=1"))
		require.Equal(t, id, tableID())
		for _, q := range []string{"delete from dbrestore.heap where v=1", "insert into dbrestore.heap values(3),(3)"} {
			execSQLRequire(t, ctx, db, q)
		}
		execSQLRequire(t, ctx, db, "data branch reset database dbrestore from dbrestore{snapshot='dbrestore_before'}")
		require.Equal(t, original, readA())
		require.Equal(t, []string{"1", "1", "2"}, values("select cast(v as char) from dbrestore.heap order by v"))
		require.Equal(t, id, tableID())
		var restoredHeapID uint64
		require.NoError(t, db.QueryRowContext(ctx, "select rel_id from mo_catalog.mo_tables where reldatabase='dbrestore' and relname='heap'").Scan(&restoredHeapID))
		require.Equal(t, heapID, restoredHeapID)
		execSQLRequire(t, ctx, db, "data branch create database dbrestore_child from dbrestore")
		defer cleanupSQLIntegration(t, cn, "drop database if exists dbrestore_child")
		execSQLRequire(t, ctx, db, "insert into dbrestore_child.heap values(99)")
		execSQLRequire(t, ctx, db, "data branch reset database dbrestore_child from dbrestore{snapshot='dbrestore_before'} tables(a,heap)")
		require.Equal(t, []string{"1", "1", "2"}, values("select cast(v as char) from dbrestore_child.heap order by v"))

		execSQLRequire(t, ctx, db, "update dbrestore.a set v=88 where id=1")
		execSQLRequire(t, ctx, db, "begin")
		execSQLRequire(t, ctx, db, "data branch reset database dbrestore from dbrestore{snapshot='dbrestore_before'} tables (a)")
		require.Equal(t, original, readA())
		execSQLRequire(t, ctx, db, "rollback")
		require.Equal(t, "1:88", readA()[0])
		_, err = db.ExecContext(ctx, "data branch revert database dbrestore from dbrestore{snapshot='dbrestore_after'} against dbrestore{snapshot='dbrestore_before'} tables (a,heap)")
		require.ErrorContains(t, err, "requires a primary key")
		require.Equal(t, "1:88", readA()[0])

		// A late constraint failure must roll back earlier table changes.
		for _, q := range []string{
			"update dbrestore.z set v=10", "create snapshot dbrestore_duplicate for database dbrestore",
			"update dbrestore.z set v=20 where id=2", "create unique index restore_unique on dbrestore.z(v)",
			"update dbrestore.a set v=99 where id=1",
		} {
			execSQLRequire(t, ctx, db, q)
		}
		defer execSQLRequire(t, ctx, db, "drop snapshot dbrestore_duplicate")
		_, err = db.ExecContext(ctx, "data branch reset database dbrestore from dbrestore{snapshot='dbrestore_duplicate'} tables(a,z)")
		require.Error(t, err)
		require.Equal(t, "1:99", readA()[0])
		require.Equal(t, []string{"10", "20"}, values("select cast(v as char) from dbrestore.z order by id"))
		var stages int
		require.NoError(t, db.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_tables where reldatabase='dbrestore' and relname like '__mo_branch_restore_%'").Scan(&stages))
		require.Zero(t, stages)
	})
}

func TestDataBranchRestoreEmptyStages(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
		defer cancel()
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(1)
		for _, operation := range []string{"reset", "revert"} {
			t.Run(operation, func(t *testing.T) {
				name := "restore_empty_" + operation
				q := func(sql string) { execSQLRequire(t, ctx, db, fmt.Sprintf(sql, name)) }
				q("create database %[1]s")
				defer cleanupSQLIntegration(t, cn, "drop database if exists "+name)
				q("create table %[1]s.heap(v int)")
				q("create table %[1]s.keys_only(id decimal(38,0) primary key)")
				q("insert into %[1]s.keys_only values(123456789012345678901234567890)")
				q("create table %[1]s.typed(a int,b varchar(20),v decimal(38,0),raw varbinary(10),n varchar(20),primary key(a,b))")
				q("insert into %[1]s.typed values(1,'quote''key',123456789012345678901234567890,x'00ff',NULL),(2,'b',NULL,x'abcd','keep')")
				q("create snapshot %[1]s_before for database %[1]s")
				defer q("drop snapshot %[1]s_before")
				var heapID uint64
				require.NoError(t, db.QueryRowContext(ctx, "select rel_id from mo_catalog.mo_tables where reldatabase=? and relname='heap'", name).Scan(&heapID))
				q("insert into %[1]s.heap values(1),(1)")
				q("delete from %[1]s.keys_only where true")
				q("insert into %[1]s.keys_only values(2)")
				q("update %[1]s.typed set v=NULL,raw=x'00',n='new' where a=1")
				q("delete from %[1]s.typed where a=2")
				q("insert into %[1]s.typed values(3,'c',3,x'33','new')")
				q("create snapshot %[1]s_after for database %[1]s")
				defer q("drop snapshot %[1]s_after")
				restore := fmt.Sprintf("data branch reset database %[1]s from %[1]s{snapshot='%[1]s_before'}", name)
				if operation == "revert" {
					restore = fmt.Sprintf("data branch revert database %[1]s from %[1]s{snapshot='%[1]s_after'} against %[1]s{snapshot='%[1]s_before'} tables(keys_only,typed)", name)
					q("insert into %[1]s.typed values(4,'later',4,x'44','later')")
				}
				verify := func(restored bool) {
					var key string
					require.NoError(t, db.QueryRowContext(ctx, "select cast(id as char) from "+name+".keys_only").Scan(&key))
					if restored {
						require.Equal(t, "123456789012345678901234567890", key)
					} else {
						require.Equal(t, "2", key)
					}
					var value, raw, nullable sql.NullString
					require.NoError(t, db.QueryRowContext(ctx, "select cast(v as char),hex(raw),n from "+name+".typed where a=1").Scan(&value, &raw, &nullable))
					if restored {
						require.Equal(t, sql.NullString{String: "123456789012345678901234567890", Valid: true}, value)
						require.Equal(t, "00FF", raw.String)
						require.False(t, nullable.Valid)
					} else {
						require.False(t, value.Valid)
						require.Equal(t, "new", nullable.String)
					}
					var count, heapCount int
					require.NoError(t, db.QueryRowContext(ctx, "select count(*) from "+name+".typed").Scan(&count))
					if operation == "revert" {
						require.Equal(t, 3, count)
					} else {
						require.Equal(t, 2, count)
					}
					require.NoError(t, db.QueryRowContext(ctx, "select count(*) from "+name+".heap").Scan(&heapCount))
					if restored && operation == "reset" {
						require.Zero(t, heapCount)
					} else {
						require.Equal(t, 2, heapCount)
					}
					var id uint64
					require.NoError(t, db.QueryRowContext(ctx, "select rel_id from mo_catalog.mo_tables where reldatabase=? and relname='heap'", name).Scan(&id))
					require.Equal(t, heapID, id)
					require.NoError(t, db.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_tables where reldatabase=? and relname like '__mo_branch_restore_%'", name).Scan(&count))
					require.Zero(t, count)
				}
				execSQLRequire(t, ctx, db, "begin")
				execSQLRequire(t, ctx, db, restore)
				verify(true)
				execSQLRequire(t, ctx, db, "rollback")
				verify(false)
				execSQLRequire(t, ctx, db, restore)
				verify(true)
				// Repeated RESET has empty stages; repeated REVERT is idempotent.
				execSQLRequire(t, ctx, db, restore)
				verify(true)
				if operation == "revert" {
					execSQLRequire(t, ctx, db, fmt.Sprintf("data branch revert database %[1]s from %[1]s{snapshot='%[1]s_before'} against %[1]s{snapshot='%[1]s_before'} tables(keys_only,typed)", name))
					verify(true)
				}
			})
		}
	})
}
