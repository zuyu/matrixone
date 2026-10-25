// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func testMergeTiming(now *time.Time) *dataBranchMergeTiming {
	return &dataBranchMergeTiming{requestID: "test", started: *now,
		now: func() time.Time { return *now }, phases: make(map[string]time.Duration)}
}

func TestBranchMergeTimingExcludesSynchronousApply(t *testing.T) {
	for _, failure := range []string{"", "error", "panic"} {
		t.Run(failure, func(t *testing.T) {
			now := time.Now()
			timing := testMergeTiming(&now)
			run := func() error {
				return timing.produce(func(emit emitFunc) error {
					now = now.Add(2 * time.Millisecond)
					_, err := emit(batchWithKind{})
					return err
				}, func(batchWithKind) (bool, error) {
					err := timing.measure("apply", func() error {
						now = now.Add(20 * time.Millisecond)
						if failure == "panic" {
							panic("apply panic")
						}
						if failure == "error" {
							return errors.New("apply error")
						}
						return nil
					})
					return false, err
				})
			}
			if failure == "panic" {
				require.PanicsWithValue(t, "apply panic", func() { _ = run() })
			} else if failure == "error" {
				require.EqualError(t, run(), "apply error")
			} else {
				require.NoError(t, run())
			}
			require.Equal(t, 2*time.Millisecond, timing.phases["diff"])
			require.Equal(t, 20*time.Millisecond, timing.phases["apply"])
		})
	}
}

func TestBranchMergeTimingRollupCountsTablesOnce(t *testing.T) {
	now := time.Now()
	parent := testMergeTiming(&now)
	for i := 0; i < 2; i++ {
		child := parent.table(tree.TableName{}, tree.TableName{})
		child.phases["apply"] = 3 * time.Millisecond
		child.phases["diff"] = time.Millisecond
		phases := child.account(5 * time.Millisecond)
		require.Equal(t, float64(1), phases["other"])
	}
	parent.phases["txn_finish"] = 2 * time.Millisecond
	phases := parent.account(15 * time.Millisecond)
	require.Equal(t, 2, parent.tables)
	require.Equal(t, float64(6), phases["apply"])
	require.Equal(t, float64(2), phases["diff"])
	require.Equal(t, float64(5), phases["other"], "retain child residual plus request residual")
	var sum float64
	for _, ms := range phases {
		sum += ms
	}
	require.Equal(t, float64(15), sum)
}

func TestBranchMergeTimingFinishPreservesRollback(t *testing.T) {
	for _, mode := range []string{"owned", "explicit"} {
		for _, failure := range []string{"", "error", "panic", "commit"} {
			t.Run(mode+"/"+failure, func(t *testing.T) {
				now := time.Now()
				timing := testMergeTiming(&now)
				timing.transactionMode = mode
				called := false
				finish := func(err error) error {
					called = true
					now = now.Add(7 * time.Millisecond)
					if failure == "error" || failure == "panic" {
						require.Error(t, err)
					}
					if failure == "commit" {
						return errors.New("commit error")
					}
					return err
				}
				run := func() (err error) {
					defer timing.finish(&err)
					defer finishDataBranchComponent(context.Background(), timing.wrapFinish(finish), &err)
					if failure == "panic" {
						panic("merge panic")
					}
					if failure == "error" {
						return errors.New("merge error")
					}
					return nil
				}
				switch failure {
				case "panic":
					require.PanicsWithValue(t, "merge panic", func() { _ = run() })
				case "error":
					require.EqualError(t, run(), "merge error")
				case "commit":
					require.EqualError(t, run(), "commit error")
				default:
					require.NoError(t, run())
				}
				require.True(t, called)
				require.Equal(t, 7*time.Millisecond, timing.phases["txn_finish"])
				action := "commit"
				if failure == "error" || failure == "panic" {
					action = "rollback"
				}
				if mode == "explicit" {
					action = "statement"
				}
				require.Equal(t, action, timing.finishAction)
			})
		}
	}
}
