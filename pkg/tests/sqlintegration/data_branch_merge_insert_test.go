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

// Direct VALUES must retain the staged path's type conversions, destination
// defaults/constraints, and transaction boundary, including later-table errors.
func TestDataBranchMergeDirectInsert(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
		defer cancel()
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(1)
		for _, name := range []string{"merge_insert_dst", "merge_insert_src"} {
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
				return "data branch merge database merge_insert_src into merge_insert_dst tables (" + name + ") when conflict accept output summary"
			}
			q := "data branch merge merge_insert_src." + name + " into merge_insert_dst." + name + " when conflict accept"
			if mode == "summary" {
				q += " output summary"
			}
			return q
		}
		merge := func(mode, name string, inserted uint64) {
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
			require.Equal(t, [7]uint64{inserted, 0, 0, 0, 0, 0, 0}, counts)
		}
		clone := func(name string) {
			exec("data branch create table merge_insert_src." + name + " from merge_insert_dst." + name)
		}
		rowsOf := func(endpoint string) []string { return read("select * from " + endpoint + " order by id") }

		for _, mode := range []string{"plain", "summary", "database"} {
			name := "typed_" + mode
			dst, src := "merge_insert_dst."+name, "merge_insert_src."+name
			exec("create table " + dst + "(id bigint primary key,amount decimal(38,6),payload varbinary(16),tag enum('a','b','c'),note varchar(100),v int default 7,g int generated always as(v*2) stored,unique key uq(note))")
			exec("insert into " + dst + "(id,amount,payload,tag,note) values(1,1.25,x'ABCD','a','base')")
			clone(name)
			exec("insert into " + src + "(id,amount,payload,tag,note,v) values(2,123456789012345678901234567890.123456,x'00FF','b','quote''slash\\tail',9),(3,NULL,NULL,'c',NULL,NULL)")
			merge(mode, name, 2)
			require.Equal(t, rowsOf(src), rowsOf(dst))
			require.Len(t, rowsOf(dst), 3)

			name = "schema_" + mode
			dst, src = "merge_insert_dst."+name, "merge_insert_src."+name
			exec("create table " + dst + "(id int primary key,v int,shared_default int default 99)")
			exec("insert into " + dst + "(id,v) values(1,10)")
			clone(name)
			// The engine permits source-only additions; destination-only
			// additions require the adapter's schema reconciliation.
			exec("alter table " + src + " add column src_only int")
			exec("insert into " + src + "(id,v,src_only) values(2,20,123),(3,NULL,456)")
			merge(mode, name, 2)
			require.Equal(t, read("select id,v from "+src+" order by id"), read("select id,v from "+dst+" order by id"))
			var defaults int
			require.NoError(t, db.QueryRowContext(ctx, "select count(*) from "+dst+" where shared_default=99").Scan(&defaults))
			require.Equal(t, 3, defaults)

			name = "unique_" + mode
			dst, src = "merge_insert_dst."+name, "merge_insert_src."+name
			exec("create table " + dst + "(id int primary key,v int unique)")
			exec("insert into " + dst + " values(1,10)")
			clone(name)
			exec("insert into " + src + " values(2,20),(3,30)")
			exec("insert into " + dst + " values(4,30)")
			before := rowsOf(dst)
			_, err := db.ExecContext(ctx, mergeSQL(mode, name))
			require.Error(t, err)
			require.Contains(t, strings.ToLower(err.Error()), "duplicate")
			require.Equal(t, before, rowsOf(dst), "failed VALUES must not retain a partially inserted batch")

			name = "txn_" + mode
			dst, src = "merge_insert_dst."+name, "merge_insert_src."+name
			exec("create table " + dst + "(id int primary key,v int)")
			exec("insert into " + dst + " values(1,10)")
			clone(name)
			exec("insert into " + src + " values(2,20),(3,30)")
			before = rowsOf(dst)
			exec("begin")
			merge(mode, name, 2)
			require.Equal(t, rowsOf(src), rowsOf(dst))
			exec("rollback")
			require.Equal(t, before, rowsOf(dst), "MERGE must not commit the caller's transaction")
		}

		for _, name := range []string{"atomic_a", "atomic_z"} {
			exec("create table merge_insert_dst." + name + "(id int primary key,v int unique)")
			exec("insert into merge_insert_dst." + name + " values(1,10)")
			clone(name)
			exec("insert into merge_insert_src." + name + " values(2,20)")
		}
		exec("insert into merge_insert_dst.atomic_z values(3,20)")
		beforeA, beforeZ := rowsOf("merge_insert_dst.atomic_a"), rowsOf("merge_insert_dst.atomic_z")
		_, err = db.ExecContext(ctx, "data branch merge database merge_insert_src into merge_insert_dst tables(atomic_a,atomic_z) when conflict accept output summary")
		require.Error(t, err)
		require.Contains(t, strings.ToLower(err.Error()), "duplicate")
		require.Equal(t, beforeA, rowsOf("merge_insert_dst.atomic_a"), "a later table failure must roll back earlier direct inserts")
		require.Equal(t, beforeZ, rowsOf("merge_insert_dst.atomic_z"))
		require.Empty(t, read("show tables from merge_insert_dst like '__mo_diff_%'"))
	})
}
