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

func TestDataBranchDatabaseDiffMerge(t *testing.T) {
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
			"create database dbmerge_dst",
			"create table dbmerge_dst.a(id int primary key,v int)",
			"create table dbmerge_dst.b(id int primary key,v int)",
			"create table dbmerge_dst._bb_commits(id int primary key)",
			"insert into dbmerge_dst.a values(1,10)",
			"insert into dbmerge_dst.b values(1,10)",
			"data branch create database dbmerge_src from dbmerge_dst",
			"update dbmerge_src.a set v=11",
			"update dbmerge_src.b set v=12",
			"update dbmerge_dst.b set v=13",
		} {
			execSQLRequire(t, ctx, db, q)
		}
		defer cleanupSQLIntegration(t, cn, "drop database if exists dbmerge_src")
		defer cleanupSQLIntegration(t, cn, "drop database if exists dbmerge_dst")
		read := func(q string) map[string][9]uint64 {
			rows, err := db.QueryContext(ctx, q)
			require.NoError(t, err)
			defer rows.Close()
			out := make(map[string][9]uint64)
			for rows.Next() {
				var name string
				var v [9]uint64
				require.NoError(t, rows.Scan(&name, &v[0], &v[1], &v[2], &v[3], &v[4], &v[5], &v[6], &v[7], &v[8]))
				out[name] = v
			}
			require.NoError(t, rows.Err())
			return out
		}
		value := func(table string) int {
			var v int
			require.NoError(t, db.QueryRowContext(ctx, "select v from "+table).Scan(&v))
			return v
		}
		expected := map[string][9]uint64{
			"a": {1, 1, 0, 0, 1, 0, 0, 0, 0},
			"b": {1, 1, 0, 0, 1, 0, 0, 1, 1},
		}
		// A role allowed to modify only the first table cannot partially
		// merge the database when the later table has no write permission.
		for _, q := range []string{
			"create role dbmerge_role",
			"grant select on table dbmerge_src.a to dbmerge_role",
			"grant select on table dbmerge_src.b to dbmerge_role",
			"grant select,insert,update,delete on table dbmerge_dst.a to dbmerge_role",
			"grant select on table dbmerge_dst.b to dbmerge_role",
			"create user dbmerge_user identified by '111' default role dbmerge_role",
		} {
			execSQLRequire(t, ctx, db, q)
		}
		defer execSQLRequire(t, ctx, db, "drop role dbmerge_role")
		defer execSQLRequire(t, ctx, db, "drop user dbmerge_user")
		limited, err := sql.Open("mysql", fmt.Sprintf("dbmerge_user:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer limited.Close()
		_, err = limited.ExecContext(ctx, "data branch merge database dbmerge_src into dbmerge_dst tables (a,b) when conflict accept output summary")
		require.Error(t, err)
		require.Contains(t, err.Error(), "privilege")
		require.Equal(t, 10, value("dbmerge_dst.a"))
		require.Equal(t, expected, read("data branch diff database dbmerge_src against dbmerge_dst ignore tables (_bb_commits) output summary"))
		// The second table fails after the first has been processed: all data
		// from this statement must roll back, including the first table.
		_, err = db.ExecContext(ctx, "data branch merge database dbmerge_src into dbmerge_dst ignore tables (_bb_commits) when conflict fail output summary")
		require.ErrorContains(t, err, "conflict")
		require.Equal(t, 10, value("dbmerge_dst.a"))
		require.Equal(t, 13, value("dbmerge_dst.b"))
		require.Equal(t, expected, read("data branch merge database dbmerge_src into dbmerge_dst ignore tables (_bb_commits) when conflict skip output summary"))
		require.Equal(t, 11, value("dbmerge_dst.a"))
		require.Equal(t, 13, value("dbmerge_dst.b"))
		execSQLRequire(t, ctx, db, "begin")
		read("data branch merge database dbmerge_src into dbmerge_dst tables (b) when conflict accept output summary")
		require.Equal(t, 12, value("dbmerge_dst.b"))
		execSQLRequire(t, ctx, db, "rollback")
		require.Equal(t, 13, value("dbmerge_dst.b"))

		// Enumerate the snapshot's tables, even after one is dropped at head.
		execSQLRequire(t, ctx, db, "create snapshot dbmerge_old for database dbmerge_src")
		defer execSQLRequire(t, ctx, db, "drop snapshot dbmerge_old")
		execSQLRequire(t, ctx, db, "drop table dbmerge_src.a")
		old := read("data branch diff database dbmerge_src{snapshot='dbmerge_old'} against dbmerge_dst tables (a) output summary")
		require.Equal(t, [9]uint64{1, 1, 0, 0, 0, 0, 0, 0, 0}, old["a"])
		missing := read("data branch diff database dbmerge_src against dbmerge_dst tables (a) output summary")
		require.Equal(t, [9]uint64{0, 1, 0, 0, 0, 1, 0, 0, 0}, missing["a"])
		_, err = db.ExecContext(ctx, "data branch merge database dbmerge_src into dbmerge_dst output summary")
		require.ErrorContains(t, err, "requires table")
		_, err = db.QueryContext(ctx, "data branch diff database dbmerge_dst{snapshot='dbmerge_old'} against dbmerge_dst output summary")
		require.ErrorContains(t, err, "does not belong")
		_, err = db.QueryContext(ctx, "data branch diff database dbmerge_src against dbmerge_dst tables (missing) output summary")
		require.ErrorContains(t, err, "unknown data branch table")
	})
}
