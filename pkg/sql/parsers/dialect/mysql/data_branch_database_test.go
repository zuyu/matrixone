// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mysql

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestDataBranchDatabaseSummarySyntax(t *testing.T) {
	for _, query := range []string{
		"data branch reset database d from d{snapshot='old'} tables (a,b)",
		"data branch revert database d from s{snapshot='after'} against s{snapshot='before'} ignore tables (_bb_commits)",
		"data branch diff database a against b output summary",
		"data branch diff database `source db`{snapshot='old'} against d tables (`a b`, c) output summary",
		"data branch merge database s into d ignore tables (`_bb_commits`) when conflict skip output summary",
		"data branch merge database s into d tables (t) when conflict accept output summary",
	} {
		stmt, err := ParseOne(context.Background(), query, 1)
		require.NoError(t, err)
		require.IsType(t, &tree.DataBranchDatabase{}, stmt)
		formatted := tree.String(stmt, dialect.MYSQL)
		again, err := ParseOne(context.Background(), formatted, 1)
		require.NoError(t, err)
		require.Equal(t, formatted, tree.String(again, dialect.MYSQL))
		stmt.Free()
		again.Free()
	}
	for _, query := range []string{
		"data branch diff database a against b",
		"data branch merge database a into b output count",
		"data branch diff database a against b tables () output summary",
	} {
		_, err := ParseOne(context.Background(), query, 1)
		require.Error(t, err)
	}
}
