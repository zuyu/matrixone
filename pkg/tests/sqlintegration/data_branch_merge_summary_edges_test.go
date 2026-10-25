// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
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

// Compare the fused counters against the separate logical DIFF, and compare
// resulting rows against plain MERGE on an independent equivalent fixture.
func TestDataBranchMergeSummaryMatchesDiffAndPlainMerge(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
		defer cancel()
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(1)
		execSQLRequire(t, ctx, db, "create database merge_summary_edges")
		defer cleanupSQLIntegration(t, cn, "drop database if exists merge_summary_edges")
		execSQLRequire(t, ctx, db, "use merge_summary_edges")

		combinations := []string{"create table %[2]s(id int primary key,v int)", "insert into %[2]s values "}
		var values []string
		for i := 0; i < 16; i++ {
			values = append(values, fmt.Sprintf("(%d,10)", i))
		}
		combinations[1] += strings.Join(values, ",")
		combinations = append(combinations, "data branch create table %[1]s from %[2]s")
		for i := 0; i < 16; i++ {
			for side, state := range []int{i / 4, i % 4} {
				table := "%[1]s"
				if side == 1 {
					table = "%[2]s"
				}
				switch state {
				case 1:
					combinations = append(combinations, fmt.Sprintf("update %s set v=20 where id=%d", table, i))
				case 2:
					combinations = append(combinations, fmt.Sprintf("update %s set v=NULL where id=%d", table, i))
				case 3:
					combinations = append(combinations, fmt.Sprintf("delete from %s where id=%d", table, i))
				}
			}
		}
		combinations = append(combinations,
			"insert into %[1]s values(100,1),(101,1),(102,NULL)",
			"insert into %[2]s values(100,2),(101,1),(103,NULL)")

		cases := []struct {
			name      string
			setup     []string
			conflicts uint64
		}{
			{"matrix", combinations, 7}, // six unequal update/delete pairs + insert/insert
			{"independent", []string{
				"create table %[2]s(id decimal(38,0) primary key,v varchar(20))",
				"insert into %[2]s values(123456789012345678901234567890,'old'),(2,'only-dst')",
				"create table %[1]s(id decimal(38,0) primary key,v varchar(20))",
				"insert into %[1]s values(123456789012345678901234567890,'new'),(3,'only-src')",
			}, 1},
			{"heap", []string{
				"create table %[2]s(v int)", "insert into %[2]s values(1),(1),(2),(3)",
				"data branch create table %[1]s from %[2]s",
				"update %[1]s set v=10 where v=1", "update %[2]s set v=20 where v=2",
				"delete from %[1]s where v=3", "insert into %[2]s values(4),(4)",
			}, 0},
			{"indexed_enum", []string{
				"create table %[2]s(id int primary key,tags enum('a','b','c','d','e'),v int,unique key uk_tags(tags))",
				"insert into %[2]s values(1,'a',10),(2,'b',20),(3,'c',30)",
				"data branch create table %[1]s from %[2]s",
				"update %[1]s set tags='d',v=11 where id=1",
				"update %[1]s set v=21 where id=2",
				"update %[2]s set tags='e',v=12 where id=1",
				"update %[2]s set v=21 where id=2",
			}, 1},
			{"schema", []string{
				"create table %[2]s(id int primary key,v int)", "insert into %[2]s values(1,10),(2,20)",
				"data branch create table %[1]s from %[2]s",
				"alter table %[1]s add column extra int",
				"update %[1]s set v=11,extra=1 where id=1", "update %[2]s set v=12 where id=1",
				"insert into %[1]s values(3,30,3)",
			}, 1},
		}
		for _, tc := range cases {
			for _, policy := range []string{"skip", "accept"} {
				t.Run(tc.name+"/"+policy, func(t *testing.T) {
					var plainRows []string
					for _, summary := range []bool{false, true} {
						prefix := fmt.Sprintf("%s_%s_%v", tc.name, policy, summary)
						src, dst := prefix+"_s", prefix+"_d"
						for _, q := range tc.setup {
							execSQLRequire(t, ctx, db, fmt.Sprintf(q, src, dst))
						}
						var expected [6]uint64
						rows, err := db.QueryContext(ctx, "data branch diff "+src+" against "+dst+" output summary")
						require.NoError(t, err)
						for rows.Next() {
							var metric string
							var a, b uint64
							require.NoError(t, rows.Scan(&metric, &a, &b))
							idx := map[string]int{"INSERTED": 0, "DELETED": 1, "UPDATED": 2}[strings.ToUpper(metric)]
							expected[idx], expected[idx+3] = a, b
						}
						require.NoError(t, rows.Err())
						require.NoError(t, rows.Close())
						q := "data branch merge " + src + " into " + dst + " when conflict " + policy
						if summary {
							var counts [7]uint64
							require.NoError(t, db.QueryRowContext(ctx, q+" output summary").Scan(
								&counts[0], &counts[1], &counts[2], &counts[3], &counts[4], &counts[5], &counts[6]))
							require.Equal(t, expected[:], counts[:6])
							require.Equal(t, tc.conflicts, counts[6])
						} else {
							execSQLRequire(t, ctx, db, q)
						}
						rows, err = db.QueryContext(ctx, "select * from "+dst+" order by 1")
						require.NoError(t, err)
						columns, err := rows.Columns()
						require.NoError(t, err)
						var actual []string
						for rows.Next() {
							cells := make([]sql.NullString, len(columns))
							ptrs := make([]any, len(columns))
							for i := range cells {
								ptrs[i] = &cells[i]
							}
							require.NoError(t, rows.Scan(ptrs...))
							actual = append(actual, fmt.Sprint(cells))
						}
						require.NoError(t, rows.Err())
						require.NoError(t, rows.Close())
						if summary {
							require.Equal(t, plainRows, actual)
						} else {
							plainRows = actual
						}
					}
				})
			}
		}
		for _, policy := range []string{"skip", "accept", "fail"} {
			t.Run("equal_only/"+policy, func(t *testing.T) {
				src, dst := "equal_"+policy+"_s", "equal_"+policy+"_d"
				for _, q := range []string{
					"create table " + dst + "(id int primary key,v int)", "insert into " + dst + " values(1,10),(2,20)",
					"data branch create table " + src + " from " + dst,
					"update " + src + " set v=11 where id=1", "update " + dst + " set v=11 where id=1",
					"delete from " + src + " where id=2", "delete from " + dst + " where id=2",
				} {
					execSQLRequire(t, ctx, db, q)
				}
				var counts [7]uint64
				require.NoError(t, db.QueryRowContext(ctx, "data branch merge "+src+" into "+dst+" when conflict "+policy+" output summary").Scan(
					&counts[0], &counts[1], &counts[2], &counts[3], &counts[4], &counts[5], &counts[6]))
				require.Equal(t, [7]uint64{}, counts)
				var n, v int
				require.NoError(t, db.QueryRowContext(ctx, "select count(*),sum(v) from "+dst).Scan(&n, &v))
				require.Equal(t, 1, n)
				require.Equal(t, 11, v)
			})
		}

		t.Run("fail_overlap_with_other_source_changes", func(t *testing.T) {
			for _, q := range []string{
				"create table fail_equal_dst(id int primary key,v int)", "insert into fail_equal_dst values(1,10)",
				"data branch create table fail_equal_src from fail_equal_dst",
				"update fail_equal_src set v=11", "update fail_equal_dst set v=11",
				"insert into fail_equal_src values(2,20)",
			} {
				execSQLRequire(t, ctx, db, q)
			}
			_, err := db.ExecContext(ctx, "data branch merge fail_equal_src into fail_equal_dst when conflict fail output summary")
			require.ErrorContains(t, err, "conflict:")
			var n, v int
			require.NoError(t, db.QueryRowContext(ctx, "select count(*),sum(v) from fail_equal_dst").Scan(&n, &v))
			require.Equal(t, 1, n)
			require.Equal(t, 11, v)
		})

		t.Run("snapshot_source", func(t *testing.T) {
			for _, q := range []string{
				"create table snap_dst(id int primary key,v int)", "insert into snap_dst values(1,1)",
				"data branch create table snap_src from snap_dst", "update snap_src set v=2",
				"create snapshot merge_summary_endpoint for database merge_summary_edges",
			} {
				execSQLRequire(t, ctx, db, q)
			}
			defer execSQLRequire(t, ctx, db, "drop snapshot merge_summary_endpoint")
			execSQLRequire(t, ctx, db, "update snap_src set v=3")
			var counts [7]uint64
			require.NoError(t, db.QueryRowContext(ctx, "data branch merge snap_src{snapshot='merge_summary_endpoint'} into snap_dst when conflict accept output summary").Scan(
				&counts[0], &counts[1], &counts[2], &counts[3], &counts[4], &counts[5], &counts[6]))
			require.Equal(t, [7]uint64{0, 0, 1, 0, 0, 0, 0}, counts)
			var v int
			require.NoError(t, db.QueryRowContext(ctx, "select v from snap_dst").Scan(&v))
			require.Equal(t, 2, v)
		})
		t.Run("explicit_transaction_rollback", func(t *testing.T) {
			for _, q := range []string{
				"create table txn_dst(id int primary key,v int)", "insert into txn_dst values(1,1)",
				"data branch create table txn_src from txn_dst", "update txn_src set v=2", "begin",
			} {
				execSQLRequire(t, ctx, db, q)
			}
			defer db.ExecContext(ctx, "rollback")
			var counts [7]uint64
			require.NoError(t, db.QueryRowContext(ctx, "data branch merge txn_src into txn_dst when conflict accept output summary").Scan(
				&counts[0], &counts[1], &counts[2], &counts[3], &counts[4], &counts[5], &counts[6]))
			require.Equal(t, [7]uint64{0, 0, 1, 0, 0, 0, 0}, counts)
			var v int
			require.NoError(t, db.QueryRowContext(ctx, "select v from txn_dst").Scan(&v))
			require.Equal(t, 2, v)
			execSQLRequire(t, ctx, db, "rollback")
			require.NoError(t, db.QueryRowContext(ctx, "select v from txn_dst").Scan(&v))
			require.Equal(t, 1, v)
		})
	})
}
