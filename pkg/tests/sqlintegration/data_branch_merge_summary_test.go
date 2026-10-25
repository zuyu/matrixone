// Copyright 2025 Matrix Origin
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
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestDataBranchMergeOutputSummary(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
		defer cancel()
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(1)
		execSQLRequire(t, ctx, db, "create database merge_summary")
		defer cleanupSQLIntegration(t, cn, "drop database if exists merge_summary")
		execSQLRequire(t, ctx, db, "use merge_summary")

		t.Run("composite_and_unchanged", func(t *testing.T) {
			for _, q := range []string{
				"create table composite_dst(a varchar(20),b varchar(20),v int,primary key(b,a))",
				"insert into composite_dst values('a,b','c',1),('a','b,c',1)",
				"data branch create table composite_src from composite_dst",
				"update composite_src set v=2 where a='a,b'",
				"update composite_dst set v=3 where a='a,b'",
			} {
				execSQLRequire(t, ctx, db, q)
			}
			var got [7]uint64
			scan := func() {
				err := db.QueryRowContext(ctx, "data branch merge composite_src into composite_dst when conflict accept output summary").Scan(
					&got[0], &got[1], &got[2], &got[3], &got[4], &got[5], &got[6])
				require.NoError(t, err)
			}
			scan()
			require.Equal(t, [7]uint64{0, 0, 1, 0, 0, 1, 1}, got)
			scan()
			require.Equal(t, [7]uint64{}, got)
		})
		t.Run("keyless", func(t *testing.T) {
			for _, q := range []string{
				"create table heap_dst(v int)",
				"insert into heap_dst values(1)",
				"data branch create table heap_src from heap_dst",
				"insert into heap_src values(2)",
			} {
				execSQLRequire(t, ctx, db, q)
			}
			var got [7]uint64
			err := db.QueryRowContext(ctx, "data branch merge heap_src into heap_dst when conflict skip output summary").Scan(
				&got[0], &got[1], &got[2], &got[3], &got[4], &got[5], &got[6])
			require.NoError(t, err)
			require.Equal(t, [7]uint64{1, 0, 0, 0, 0, 0, 0}, got)
			var n int
			require.NoError(t, db.QueryRowContext(ctx, "select count(*) from heap_dst").Scan(&n))
			require.Equal(t, 2, n)
		})

		for _, policy := range []string{"skip", "accept", "fail"} {
			t.Run(policy, func(t *testing.T) {
				dst, src := "d_"+policy, "s_"+policy
				for _, statement := range []string{
					"create table " + dst + "(id int primary key,v int)",
					"insert into " + dst + " values(1,10),(2,20),(3,30),(4,40),(5,50),(6,60)",
					"data branch create table " + src + " from " + dst,
					"update " + src + " set v=201 where id=1",
					"update " + src + " set v=202 where id=2",
					"delete from " + src + " where id in (3,4)",
					"update " + src + " set v=505 where id=5",
					"insert into " + src + " values(7,70),(9,901)",
					"update " + dst + " set v=101 where id=1",
					"delete from " + dst + " where id in (2,4)",
					"update " + dst + " set v=103 where id=3",
					"update " + dst + " set v=505 where id=5",
					"insert into " + dst + " values(8,80),(9,902)",
				} {
					execSQLRequire(t, ctx, db, statement)
				}

				queryRows := func() [][2]int {
					rows, err := db.QueryContext(ctx, "select id,v from "+dst+" order by id")
					require.NoError(t, err)
					defer rows.Close()
					var out [][2]int
					for rows.Next() {
						var row [2]int
						require.NoError(t, rows.Scan(&row[0], &row[1]))
						out = append(out, row)
					}
					require.NoError(t, rows.Err())
					return out
				}
				before := queryRows()
				q := "data branch merge " + src + " into " + dst + " when conflict " + policy + " output summary"
				var counts [7]uint64
				err = db.QueryRowContext(ctx, q).Scan(&counts[0], &counts[1], &counts[2],
					&counts[3], &counts[4], &counts[5], &counts[6])
				if policy == "fail" {
					require.Error(t, err)
					require.Equal(t, before, queryRows(), "failed MERGE must not publish partial writes")
					return
				}
				require.NoError(t, err)
				require.Equal(t, [7]uint64{2, 1, 2, 2, 1, 2, 4}, counts)
				if policy == "skip" {
					require.Equal(t, [][2]int{{1, 101}, {3, 103}, {5, 505}, {6, 60}, {7, 70}, {8, 80}, {9, 902}}, queryRows())
				} else {
					require.Equal(t, [][2]int{{1, 201}, {2, 202}, {5, 505}, {6, 60}, {7, 70}, {8, 80}, {9, 901}}, queryRows())
				}
			})
		}
	})
}
