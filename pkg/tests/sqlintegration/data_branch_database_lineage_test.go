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

func TestDataBranchDatabaseRefreshesLineagePerStatement(t *testing.T) {
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
		for _, name := range []string{"dag_dst", "dag_src"} {
			exec("create database " + name)
			defer cleanupSQLIntegration(t, cn, "drop database if exists "+name)
		}
		// Two different roots and opposite fork directions in a single request.
		exec("create table dag_dst.a(id int primary key,v int)")
		exec("insert into dag_dst.a values(1,10)")
		exec("data branch create table dag_src.a from dag_dst.a")
		exec("create table dag_src.b(id int primary key,v int)")
		exec("insert into dag_src.b values(1,20)")
		exec("data branch create table dag_dst.b from dag_src.b")
		exec("update dag_src.a set v=11")
		exec("update dag_src.b set v=21")
		exec("update dag_dst.b set v=22")
		read := func(q string) map[string][9]uint64 {
			rows, err := db.QueryContext(ctx, q)
			require.NoError(t, err)
			defer rows.Close()
			out := map[string][9]uint64{}
			for rows.Next() {
				var name string
				var counts [9]uint64
				dests := []any{&name}
				for i := range counts {
					dests = append(dests, &counts[i])
				}
				require.NoError(t, rows.Scan(dests...))
				out[name] = counts
			}
			require.NoError(t, rows.Err())
			return out
		}
		value := func(table string) int {
			var v int
			require.NoError(t, db.QueryRowContext(ctx, "select v from "+table).Scan(&v))
			return v
		}
		diff := "data branch diff database dag_src against dag_dst output summary"
		merge := "data branch merge database dag_src into dag_dst when conflict "
		first := map[string][9]uint64{"a": {1, 1, 0, 0, 1, 0, 0, 0, 0}, "b": {1, 1, 0, 0, 1, 0, 0, 1, 1}}
		require.Equal(t, first, read(diff))
		require.Equal(t, first, read(merge+"accept output summary"))
		require.Equal(t, 11, value("dag_dst.a"))
		require.Equal(t, 21, value("dag_dst.b"))

		// Replace lineage between requests on this connection: one ALTER
		// generation and one new clone edge. Neither may reuse the old graph.
		exec("alter table dag_src.a add column extra int default 7")
		exec("drop table dag_src.b")
		exec("data branch create table dag_src.b from dag_dst.b")
		exec("update dag_src.a set v=31")
		exec("update dag_dst.a set v=32")
		exec("update dag_src.b set v=41")
		exec("update dag_dst.b set v=42")
		expected := map[string][9]uint64{"a": {1, 1, 0, 0, 1, 0, 0, 1, 1}, "b": {1, 1, 0, 0, 1, 0, 0, 1, 1}}
		require.Equal(t, expected, read(diff))
		require.Equal(t, expected, read(merge+"skip output summary"))
		require.Equal(t, 32, value("dag_dst.a"))
		require.Equal(t, 42, value("dag_dst.b"))
		exec("begin")
		require.Equal(t, expected, read(merge+"accept output summary"))
		require.Equal(t, 31, value("dag_dst.a"))
		require.Equal(t, 41, value("dag_dst.b"))
		exec("rollback")
		require.Equal(t, 32, value("dag_dst.a"))
		require.Equal(t, 42, value("dag_dst.b"))
		require.Equal(t, expected, read(merge+"accept output summary"))
		zero := map[string][9]uint64{"a": {1, 1}, "b": {1, 1}}
		require.Equal(t, zero, read(diff))
		require.Equal(t, zero, read(merge+"accept output summary"))
	})
}
