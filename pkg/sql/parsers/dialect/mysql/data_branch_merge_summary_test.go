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

package mysql

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestDataBranchMergeOutputSummary(t *testing.T) {
	for _, sql := range []string{
		"data branch merge s into d output summary",
		"data branch merge db.s{snapshot='v1'} into db.d when conflict skip output summary",
		"data branch merge s into d when conflict accept output summary",
		"data branch merge s into d when conflict fail output summary",
	} {
		stmt, err := ParseOne(context.Background(), sql, 1)
		require.NoError(t, err)
		merge := stmt.(*tree.DataBranchMerge)
		require.True(t, merge.OutputSummary)
		formatted := tree.String(merge, dialect.MYSQL)
		again, err := ParseOne(context.Background(), formatted, 1)
		require.NoError(t, err)
		require.True(t, again.(*tree.DataBranchMerge).OutputSummary)
		require.Equal(t, formatted, tree.String(again, dialect.MYSQL))
		stmt.Free()
		again.Free()
	}
	plain, err := ParseOne(context.Background(), "data branch merge s into d", 1)
	require.NoError(t, err)
	require.False(t, plain.(*tree.DataBranchMerge).OutputSummary)
	plain.Free()
	for _, sql := range []string{
		"data branch merge s into d output count",
		"data branch merge s into d output summary when conflict skip",
	} {
		_, err := ParseOne(context.Background(), sql, 1)
		require.Error(t, err)
	}
}
