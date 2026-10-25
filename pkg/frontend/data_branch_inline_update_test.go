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

func TestDataBranchInlineUpdateEligibility(t *testing.T) {
	for _, typ := range []types.T{types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_varchar, types.T_decimal128, types.T_float64, types.T_enum, types.T_timestamp} {
		t.Run(typ.String(), func(t *testing.T) {
			tbl := newTestBranchTableStuff(gomock.NewController(t))
			tbl.def.baseColNames = append([]string(nil), tbl.def.colNames...)
			tbl.def.colTypes[1] = typ.ToType()
			integer := typ >= types.T_int8 && typ <= types.T_uint64
			for _, mode := range []dataBranchApplyMode{dataBranchApplyModeOnlineMerge, dataBranchApplyModeOnlinePKOnly, dataBranchApplyModePortableSQL} {
				_, _, info := buildDataBranchApplyLayout(t.Context(), &Session{}, tbl, mode)
				require.NotNil(t, info)
				require.Equal(t, integer && mode == dataBranchApplyModeOnlineMerge, len(info.inlineUpdateCasts) > 0)
			}
		})
	}
	tbl := newFakePKBranchTableStuff(gomock.NewController(t))
	tbl.def.colTypes[1] = types.T_int64.ToType()
	_, _, info := buildDataBranchApplyLayout(t.Context(), &Session{}, tbl, dataBranchApplyModeOnlineMerge)
	require.Empty(t, info.inlineUpdateCasts, "keyless updates retain row identity handling")
}
