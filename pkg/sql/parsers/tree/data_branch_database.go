// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tree

// DataBranchDatabase applies a data operation to a set of tables. Endpoint
// ObjectName fields contain database names; SchemaName must be empty.
type DataBranchDatabase struct {
	statementImpl
	Operation            string
	Source, Destination  TableName
	Before               TableName
	Tables, IgnoreTables IdentifierList
	ConflictOpt          *ConflictOpt
}

func (s *DataBranchDatabase) StmtKind() StmtKind {
	if s.Operation == "reset" || s.Operation == "revert" {
		return frontendStatusTyp
	}
	return compositeResRowType
}
func (s *DataBranchDatabase) GetStatementType() string {
	return "data branch " + s.Operation + " database"
}
func (s *DataBranchDatabase) GetQueryType() string { return QueryTypeOth }
func (s *DataBranchDatabase) TypeName() string     { return s.GetStatementType() }
func (s *DataBranchDatabase) String() string       { return s.GetStatementType() }
func (s *DataBranchDatabase) Free()                {}
func (s *DataBranchDatabase) Format(ctx *FmtCtx) {
	formatDataBranchStatement(ctx, func() {
		ctx.WriteString("data branch " + s.Operation + " database ")
		if s.Operation == "reset" || s.Operation == "revert" {
			s.Destination.Format(ctx)
			ctx.WriteString(" from ")
			s.Source.Format(ctx)
			if s.Operation == "revert" {
				ctx.WriteString(" against ")
				s.Before.Format(ctx)
			}
		} else {
			s.Source.Format(ctx)
			if s.Operation == "diff" {
				ctx.WriteString(" against ")
			} else {
				ctx.WriteString(" into ")
			}
			s.Destination.Format(ctx)
		}
		if len(s.Tables) > 0 {
			ctx.WriteString(" tables (")
			s.Tables.Format(ctx)
			ctx.WriteByte(')')
		}
		if len(s.IgnoreTables) > 0 {
			ctx.WriteString(" ignore tables (")
			s.IgnoreTables.Format(ctx)
			ctx.WriteByte(')')
		}
		if s.ConflictOpt != nil {
			s.ConflictOpt.Format(ctx)
		}
		if s.Operation == "diff" || s.Operation == "merge" {
			ctx.WriteString(" output summary")
		}
	})
}
