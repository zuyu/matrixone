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

func TestDataBranchRestoreInsertUsesOrdinaryConstraints(t *testing.T) {
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
				name := "restore_insert_check_" + op
				q := func(sql string) { execSQLRequire(t, ctx, db, sql) }
				q("create database " + name)
				defer cleanupSQLIntegration(t, cn, "drop database if exists "+name)
				q("create table " + name + ".a_plain(id int primary key)")
				q("insert into " + name + ".a_plain values(1)")
				q("create table " + name + ".z_unique(id int primary key,v int)")
				q("insert into " + name + ".z_unique values(1,10),(2,10)")
				q("create snapshot " + name + "_before for database " + name)
				defer q("drop snapshot " + name + "_before")
				q("delete from " + name + ".z_unique where id=1")
				q("create unique index uv on " + name + ".z_unique(v)")
				q("insert into " + name + ".a_plain values(2)")
				q("create snapshot " + name + "_after for database " + name)
				defer q("drop snapshot " + name + "_after")
				defer db.ExecContext(context.Background(), "rollback")
				restore := fmt.Sprintf("data branch reset database %[1]s from %[1]s{snapshot='%[1]s_before'}", name)
				if op == "revert" {
					restore = fmt.Sprintf("data branch revert database %[1]s from %[1]s{snapshot='%[1]s_after'} against %[1]s{snapshot='%[1]s_before'}", name)
				}
				verify := func(plain, rows, sumID int) {
					var gotPlain, gotRows, gotSum int
					require.NoError(t, db.QueryRowContext(ctx, "select count(*) from "+name+".a_plain").Scan(&gotPlain))
					require.NoError(t, db.QueryRowContext(ctx, "select count(*),sum(id) from "+name+".z_unique").Scan(&gotRows, &gotSum))
					require.Equal(t, plain, gotPlain)
					require.Equal(t, rows, gotRows)
					require.Equal(t, sumID, gotSum)
				}
				for _, explicit := range []bool{false, true} {
					if explicit {
						q("begin")
					}
					// INSERT of the missing id=1 violates the current unique
					// index. Reject the statement itself, including inside
					// BEGIN; do not defer detection to a bulk-restore commit.
					_, err := db.ExecContext(ctx, restore)
					require.Error(t, err)
					if explicit {
						q("rollback")
					}
					// The earlier a_plain DELETE must be undone as well.
					verify(2, 1, 2)
				}
				q("drop index uv on " + name + ".z_unique")
				q("begin")
				q(restore)
				verify(1, 2, 3)
				q("rollback")
				verify(2, 1, 2)
				q(restore)
				verify(1, 2, 3)
				q(restore)
				verify(1, 2, 3)
			})
		}
	})
}
