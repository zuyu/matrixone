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

func TestDataBranchMergeInlineDelete(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
		defer cancel()
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(1)
		exec := func(q string) { execSQLRequire(t, ctx, db, q) }
		for _, name := range []string{"merge_delete_dst", "merge_delete_src"} {
			exec("create database " + name)
			defer cleanupSQLIntegration(t, cn, "drop database if exists "+name)
		}
		read := func(q string) []string {
			rows, err := db.QueryContext(ctx, q)
			require.NoError(t, err)
			defer rows.Close()
			cols, err := rows.Columns()
			require.NoError(t, err)
			var out []string
			for rows.Next() {
				cells := make([]sql.NullString, len(cols))
				ptrs := make([]any, len(cols))
				for i := range cells {
					ptrs[i] = &cells[i]
				}
				require.NoError(t, rows.Scan(ptrs...))
				out = append(out, fmt.Sprint(cells))
			}
			require.NoError(t, rows.Err())
			return out
		}
		mergeSQL := func(mode, name, policy string) string {
			if mode == "database" {
				return "data branch merge database merge_delete_src into merge_delete_dst tables (" + name + ") when conflict " + policy + " output summary"
			}
			q := "data branch merge merge_delete_src." + name + " into merge_delete_dst." + name + " when conflict " + policy
			if mode == "summary" {
				q += " output summary"
			}
			return q
		}
		merge := func(mode, name string, deleted uint64) {
			q := mergeSQL(mode, name, "accept")
			if mode == "plain" {
				exec(q)
				return
			}
			var counts [7]uint64
			var table string
			var srcExists, dstExists uint64
			var dests []any
			if mode == "database" {
				dests = append(dests, &table, &srcExists, &dstExists)
			}
			for i := range counts {
				dests = append(dests, &counts[i])
			}
			require.NoError(t, db.QueryRowContext(ctx, q).Scan(dests...))
			require.Equal(t, [7]uint64{0, deleted, 0, 0, 0, 0, 0}, counts)
		}
		clone := func(name string) {
			exec("data branch create table merge_delete_src." + name + " from merge_delete_dst." + name)
		}
		for _, mode := range []string{"plain", "summary", "database"} {
			for _, tc := range []struct {
				name, ddl, values, predicate string
				deleted                      uint64
			}{
				{"signed", "id bigint primary key,v varchar(20)",
					"(-9223372036854775808,NULL),(-9223372036854775807,'keep'),(0,'zero'),(9223372036854775806,'keep'),(9223372036854775807,'max')",
					"id in (-9223372036854775808,0,9223372036854775807)", 3},
				{"unsigned", "id bigint unsigned primary key,v decimal(20,0)",
					"(0,NULL),(1,1),(18446744073709551614,2),(18446744073709551615,3)",
					"id in (0,18446744073709551615)", 2},
				{"composite", "id bigint unsigned,k bigint,v varchar(20),primary key(id,k)",
					"(18446744073709551615,-9223372036854775808,NULL),(18446744073709551615,-9223372036854775807,'keep'),(1,-9223372036854775808,'keep'),(1,9223372036854775807,'del'),(18446744073709551615,9223372036854775807,'keep')",
					"(id=18446744073709551615 and k=-9223372036854775808) or (id=1 and k=9223372036854775807)", 2},
				{"small", "id tinyint,a tinyint unsigned,b smallint,c smallint unsigned,d int,e int unsigned,v varchar(20),primary key(id,a,b,c,d,e)",
					"(-128,255,-32768,65535,-2147483648,4294967295,'del'),(127,0,32767,0,2147483647,0,'del'),(0,1,0,1,0,1,NULL)",
					"id<>0", 2},
				{"quoted", "`id``key` int primary key,v varchar(20)", "(1,'delete'),(2,NULL)", "`id``key`=1", 1},
			} {
				name := tc.name + "_" + mode
				dst, src := "merge_delete_dst."+name, "merge_delete_src."+name
				exec("create table " + dst + "(" + tc.ddl + ")")
				exec("insert into " + dst + " values" + tc.values)
				clone(name)
				exec("delete from " + src + " where " + tc.predicate)
				rowsOf := func(table string) []string { return read("select * from " + table + " order by 1,2") }
				before := rowsOf(dst)
				exec("begin")
				merge(mode, name, tc.deleted)
				require.Equal(t, rowsOf(src), rowsOf(dst))
				exec("rollback")
				require.Equal(t, before, rowsOf(dst), "direct DELETE must belong to the owner transaction")
				merge(mode, name, tc.deleted)
				require.Equal(t, rowsOf(src), rowsOf(dst))
				merge(mode, name, 0)
				require.Equal(t, rowsOf(src), rowsOf(dst))
			}
			// A destination update conflicts with one source deletion. Both
			// policies must preserve their semantics on the direct apply path.
			for _, policy := range []string{"skip", "accept"} {
				name := "conflict_" + mode + "_" + policy
				dst, src := "merge_delete_dst."+name, "merge_delete_src."+name
				exec("create table " + dst + "(id int primary key,v varchar(20))")
				exec("insert into " + dst + " values(1,'old'),(2,'old'),(3,NULL)")
				clone(name)
				exec("delete from " + src + " where id<3")
				exec("update " + dst + " set v='target' where id=1")
				exec(mergeSQL(mode, name, policy))
				expected := "select * from " + src + " order by id"
				if policy == "skip" {
					expected = "select 1 as id,'target' as v union all select 3,NULL"
				}
				require.Equal(t, read(expected), read("select * from "+dst+" order by id"))
			}
		}
		// A later table's constraint failure must roll back an earlier direct DELETE.
		for _, name := range []string{"atomic_a", "atomic_z"} {
			exec("create table merge_delete_dst." + name + "(id int primary key,v int unique)")
			exec("insert into merge_delete_dst." + name + " values(1,10)")
			clone(name)
		}
		exec("delete from merge_delete_src.atomic_a")
		exec("update merge_delete_src.atomic_z set v=20")
		exec("insert into merge_delete_dst.atomic_z values(2,20)")
		beforeA, beforeZ := read("select * from merge_delete_dst.atomic_a"), read("select * from merge_delete_dst.atomic_z order by id")
		_, err = db.ExecContext(ctx, "data branch merge database merge_delete_src into merge_delete_dst tables(atomic_a,atomic_z) when conflict accept output summary")
		require.Error(t, err)
		require.Contains(t, strings.ToLower(err.Error()), "duplicate")
		require.Equal(t, beforeA, read("select * from merge_delete_dst.atomic_a"))
		require.Equal(t, beforeZ, read("select * from merge_delete_dst.atomic_z order by id"))
		require.Empty(t, read("show tables from merge_delete_dst like '__mo_diff_%'"))
	})
}
