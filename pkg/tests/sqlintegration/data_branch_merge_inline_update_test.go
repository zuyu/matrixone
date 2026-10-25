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

func TestDataBranchMergeInlineUpdate(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
		defer cancel()
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(1)
		for _, name := range []string{"merge_update_dst", "merge_update_src"} {
			execSQLRequire(t, ctx, db, "create database "+name)
			defer cleanupSQLIntegration(t, cn, "drop database if exists "+name)
		}
		exec := func(q string) { execSQLRequire(t, ctx, db, q) }
		read := func(q string) []string {
			rows, err := db.QueryContext(ctx, q)
			require.NoError(t, err)
			defer rows.Close()
			columns, err := rows.Columns()
			require.NoError(t, err)
			var out []string
			for rows.Next() {
				cells := make([]sql.NullString, len(columns))
				ptrs := make([]any, len(cells))
				for i := range cells {
					ptrs[i] = &cells[i]
				}
				require.NoError(t, rows.Scan(ptrs...))
				out = append(out, fmt.Sprint(cells))
			}
			require.NoError(t, rows.Err())
			return out
		}
		mergeSQL := func(mode, name string) string {
			if mode == "database" {
				return "data branch merge database merge_update_src into merge_update_dst tables (" + name + ") when conflict accept output summary"
			}
			q := "data branch merge merge_update_src." + name + " into merge_update_dst." + name + " when conflict accept"
			if mode == "summary" {
				q += " output summary"
			}
			return q
		}
		merge := func(mode, name string, updated uint64) {
			q := mergeSQL(mode, name)
			if mode == "plain" {
				exec(q)
				return
			}
			var counts [7]uint64
			dests := make([]any, 0, 10)
			var table string
			var srcExists, dstExists uint64
			if mode == "database" {
				dests = append(dests, &table, &srcExists, &dstExists)
			}
			for i := range counts {
				dests = append(dests, &counts[i])
			}
			require.NoError(t, db.QueryRowContext(ctx, q).Scan(dests...))
			require.Equal(t, [7]uint64{0, 0, updated, 0, 0, 0, 0}, counts)
		}
		clone := func(name string) {
			exec("data branch create table merge_update_src." + name + " from merge_update_dst." + name)
		}
		rowsOf := func(endpoint string) []string { return read("select * from " + endpoint + " order by id") }

		for _, mode := range []string{"plain", "summary", "database"} {
			name := "typed_" + mode
			dst, src := "merge_update_dst."+name, "merge_update_src."+name
			exec("create table " + dst + "(id bigint unsigned,k bigint,s tinyint,u tinyint unsigned,sm smallint,um smallint unsigned,i int,ui int unsigned,v bigint,uv bigint unsigned,n int,primary key(id,k))")
			exec("insert into " + dst + " values(18446744073709551615,-9223372036854775808,0,0,0,0,0,0,0,0,1),(1,2,0,0,0,0,0,0,0,0,2)")
			clone(name)
			exec("update " + src + " set s=-128,u=255,sm=-32768,um=65535,i=-2147483648,ui=4294967295,v=-9223372036854775808,uv=18446744073709551615,n=NULL")
			before := rowsOf(dst)
			exec("begin")
			merge(mode, name, 2)
			require.Equal(t, rowsOf(src), rowsOf(dst))
			exec("rollback")
			require.Equal(t, before, rowsOf(dst))
			merge(mode, name, 2)
			require.Equal(t, rowsOf(src), rowsOf(dst))

			name = "generated_" + mode
			dst, src = "merge_update_dst."+name, "merge_update_src."+name
			exec("create table " + dst + "(id int primary key,v int,g int generated always as(v*2) stored,shared_default int default 99)")
			exec("insert into " + dst + "(id,v) values(1,10),(2,20)")
			clone(name)
			exec("alter table " + src + " add column src_only int")
			exec("update " + src + " set v=v+1,src_only=123")
			merge(mode, name, 2)
			require.Equal(t, read("select id,v,g,shared_default from "+src+" order by id"), rowsOf(dst))
			require.Len(t, rowsOf(dst), 2)

			name = "unique_" + mode
			dst, src = "merge_update_dst."+name, "merge_update_src."+name
			exec("create table " + dst + "(id int primary key,v int unique)")
			exec("insert into " + dst + " values(1,10),(2,20)")
			clone(name)
			exec("update " + src + " set v=v+1")
			exec("insert into " + dst + " values(3,21)")
			before = rowsOf(dst)
			_, err := db.ExecContext(ctx, mergeSQL(mode, name))
			require.Error(t, err)
			require.Contains(t, strings.ToLower(err.Error()), "duplicate")
			require.Equal(t, before, rowsOf(dst), "failed inline UPDATE must not retain a partial batch")
		}

		for _, name := range []string{"atomic_a", "atomic_z"} {
			exec("create table merge_update_dst." + name + "(id int primary key,v int unique)")
			exec("insert into merge_update_dst." + name + " values(1,10)")
			clone(name)
			exec("update merge_update_src." + name + " set v=20")
		}
		exec("insert into merge_update_dst.atomic_z values(2,20)")
		beforeA, beforeZ := rowsOf("merge_update_dst.atomic_a"), rowsOf("merge_update_dst.atomic_z")
		_, err = db.ExecContext(ctx, "data branch merge database merge_update_src into merge_update_dst tables(atomic_a,atomic_z) when conflict accept output summary")
		require.Error(t, err)
		require.Contains(t, strings.ToLower(err.Error()), "duplicate")
		require.Equal(t, beforeA, rowsOf("merge_update_dst.atomic_a"), "later failure must roll back earlier inline UPDATE")
		require.Equal(t, beforeZ, rowsOf("merge_update_dst.atomic_z"))
		require.Empty(t, read("show tables from merge_update_dst like '__mo_diff_%'"))
	})
}
