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

func TestDataBranchRestoreSparseRuntimeFilter(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
		defer cancel()
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(1)
		name := "restore_sparse_filter"
		q := func(sql string) { execSQLRequire(t, ctx, db, sql) }
		q("create database " + name)
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+name)
		q("create table " + name + ".t(id bigint primary key,v bigint)")
		q("insert into " + name + ".t select result,result*10 from generate_series(0,9999) g")
		q("create snapshot " + name + "_before for database " + name)
		defer q("drop snapshot " + name + "_before")
		q("update " + name + ".t set v=v+1 where id%100=0")
		q("create snapshot " + name + "_after for database " + name)
		defer q("drop snapshot " + name + "_after")
		defer db.ExecContext(context.Background(), "rollback")
		q("update " + name + ".t set v=v+10000 where id=9999")
		q("delete from " + name + ".t where id=9998")
		q("insert into " + name + ".t values(20000,99)")
		read := func() [][2]int64 {
			rows, err := db.QueryContext(ctx, "select id,v from "+name+".t order by id")
			require.NoError(t, err)
			defer rows.Close()
			var result [][2]int64
			for rows.Next() {
				var row [2]int64
				require.NoError(t, rows.Scan(&row[0], &row[1]))
				result = append(result, row)
			}
			require.NoError(t, rows.Err())
			return result
		}
		var want [][2]int64
		for id := int64(0); id < 10000; id++ {
			if id == 9998 {
				continue
			}
			v := id * 10
			if id == 9999 {
				v += 10000
			}
			want = append(want, [2]int64{id, v})
		}
		want = append(want, [2]int64{20000, 99})
		original := read()
		update := fmt.Sprintf("update %[1]s.t d join (select b.id,b.v from %[1]s.t{snapshot='%[1]s_before'} b left join %[1]s.t{snapshot='%[1]s_after'} a on a.id=b.id where a.id is null or not(a.v<=>b.v)) s on d.id=s.id set d.v=s.v where not(d.v<=>s.v)", name)
		rows, err := db.QueryContext(ctx, "explain verbose "+update)
		require.NoError(t, err)
		var explain strings.Builder
		for rows.Next() {
			var line string
			require.NoError(t, rows.Scan(&line))
			explain.WriteString(line)
			explain.WriteByte('\n')
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())
		require.Contains(t, explain.String(), "Runtime Filter Probe: d.id")
		q("begin")
		for _, affected := range []int64{100, 0} {
			res, err := db.ExecContext(ctx, update)
			require.NoError(t, err)
			got, err := res.RowsAffected()
			require.NoError(t, err)
			require.Equal(t, affected, got)
			require.Equal(t, want, read())
		}
		q("rollback")
		require.Equal(t, original, read())
		restore := fmt.Sprintf("data branch revert database %[1]s from %[1]s{snapshot='%[1]s_after'} against %[1]s{snapshot='%[1]s_before'} tables(t)", name)
		q(restore)
		require.Equal(t, want, read())
		q(restore)
		require.Equal(t, want, read())
		// An empty build must not discard or modify any target rows.
		q("update " + name + ".t d join (select id,v from " + name + ".t{snapshot='" + name + "_before'} where id<0) s on d.id=s.id set d.v=s.v")
		require.Equal(t, want, read())
	})
}
