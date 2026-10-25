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

// A coalesced probe delivers restored and ordinary updates in the same batch.
// Keeping the wrong row indexes used to silently discard the ordinary updates.
func TestDataBranchMergeRestoresAndUpdatesSameBatch(t *testing.T) {
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
		for _, name := range []string{"merge_restore_src", "merge_restore_dst"} {
			exec("create database " + name)
			defer cleanupSQLIntegration(t, cn, "drop database if exists "+name)
		}
		read := func(q string) []string {
			rows, err := db.QueryContext(ctx, q)
			require.NoError(t, err)
			defer rows.Close()
			cols, err := rows.Columns()
			require.NoError(t, err)
			var result []string
			for rows.Next() {
				cells := make([]string, len(cols))
				ptrs := make([]any, len(cols))
				for i := range cells {
					ptrs[i] = &cells[i]
				}
				require.NoError(t, rows.Scan(ptrs...))
				result = append(result, strings.Join(cells, ","))
			}
			require.NoError(t, rows.Err())
			return result
		}
		for _, mode := range []string{"plain", "summary", "database"} {
			for _, valueType := range []string{"int", "decimal(20,0)"} {
				name := mode + "_integer"
				if valueType != "int" {
					name = mode + "_decimal"
				}
				dst, src := "merge_restore_dst."+name, "merge_restore_src."+name
				exec("create table " + dst + "(id int,k int,v " + valueType + ",primary key(id,k))")
				exec("insert into " + dst + " values(1,1,10),(2,1,20),(3,1,30),(4,1,40),(5,1,50)")
				exec("data branch create table " + src + " from " + dst)
				exec("update " + src + " set v=v+100")
				exec("delete from " + dst + " where id in(2,4)")
				exec("update " + dst + " set v=1010 where id=1")
				exec("update " + dst + " set v=150 where id=5") // equal updates are suppressed
				rowsOf := func(table string) []string { return read("select id,k,v from " + table + " order by id,k") }
				before, want := rowsOf(dst), rowsOf(src)
				merge := func(counts string) {
					q := "data branch merge " + src + " into " + dst
					prefix := ""
					if mode == "database" {
						q = "data branch merge database merge_restore_src into merge_restore_dst tables(" + name + ")"
						prefix = name + ",1,1,"
					}
					q += " when conflict accept"
					if mode == "plain" {
						exec(q)
					} else {
						require.Equal(t, []string{prefix + counts}, read(q+" output summary"))
					}
					require.Equal(t, want, rowsOf(dst), mode+" "+valueType)
				}
				exec("begin")
				merge("0,0,4,0,2,1,3")
				exec("rollback")
				require.Equal(t, before, rowsOf(dst))
				merge("0,0,4,0,2,1,3")
				merge("0,0,0,0,0,0,0")
			}
		}
		require.Empty(t, read("show tables from merge_restore_dst like '__mo_diff_%'"))
	})
}
