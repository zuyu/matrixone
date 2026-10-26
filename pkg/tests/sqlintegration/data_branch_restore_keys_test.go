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

func TestDataBranchRestoreBoundedDeleteKeys(t *testing.T) {
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
					{"empty", 0, false}, {"limit_rows", 128, false}, {"overflow_rows", 129, false},
					{"limit_cells", 102, true}, {"overflow_cells", 103, true},
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
				q("begin")
				q(restore)
				verify(true)
				q("rollback")
				verify(false)
				q(restore)
				verify(true)
				q(restore)
				verify(true)
			})
		}
	})
}
