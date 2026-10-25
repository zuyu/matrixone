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
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestDataBranchInlineDeleteKeyTypes(t *testing.T) {
	for _, tc := range []struct {
		typ  types.T
		want bool
	}{
		{types.T_int8, true}, {types.T_int16, true}, {types.T_int32, true}, {types.T_int64, true},
		{types.T_uint8, true}, {types.T_uint16, true}, {types.T_uint32, true}, {types.T_uint64, true},
		{types.T_decimal64, false}, {types.T_decimal128, false}, {types.T_varchar, false},
		{types.T_float32, false}, {types.T_float64, false}, {types.T_uuid, false},
	} {
		t.Run(tc.typ.String(), func(t *testing.T) {
			ses := newValidateSession(t)
			tbl := newTestBranchTableStuff(gomock.NewController(t))
			tbl.def.colTypes[0] = tc.typ.ToType()
			// Keep a noninteger nonkey column: DELETE eligibility depends only on keys.
			tbl.def.baseColNames = append([]string(nil), tbl.def.colNames...)
			for _, mode := range []dataBranchApplyMode{dataBranchApplyModeOnlineMerge, dataBranchApplyModePortableSQL, dataBranchApplyModeOnlinePKOnly} {
				_, _, info := buildDataBranchApplyLayout(t.Context(), ses, tbl, mode)
				require.Equal(t, tc.want && mode == dataBranchApplyModeOnlineMerge, info.inlineDelete)
			}
			tbl.def.pkColIdxes = []int{0, 1}
			_, _, info := buildDataBranchApplyLayout(t.Context(), ses, tbl, dataBranchApplyModeOnlineMerge)
			require.False(t, info.inlineDelete, "a noninteger component keeps composite keys staged")
		})
	}
}
