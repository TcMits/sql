package sql

import (
	"fmt"
	"strings"
)

type Node interface {
	node() bool                          // return true if valid node
	subnodes(yield func(Node) bool) bool // yields all subnodes of the node

	fmt.Stringer
}

// statement nodes
func (s *AlterTableStatement) node() bool         { return s != nil }
func (s *AnalyzeStatement) node() bool            { return s != nil }
func (s *BeginStatement) node() bool              { return s != nil }
func (s *CommitStatement) node() bool             { return s != nil }
func (s *CreateIndexStatement) node() bool        { return s != nil }
func (s *CreateTableStatement) node() bool        { return s != nil }
func (s *CreateTriggerStatement) node() bool      { return s != nil }
func (s *CreateViewStatement) node() bool         { return s != nil }
func (s *CreateVirtualTableStatement) node() bool { return s != nil }
func (s *DeleteStatement) node() bool             { return s != nil }
func (s *DropIndexStatement) node() bool          { return s != nil }
func (s *DropTableStatement) node() bool          { return s != nil }
func (s *DropTriggerStatement) node() bool        { return s != nil }
func (s *DropViewStatement) node() bool           { return s != nil }
func (s *ExplainStatement) node() bool            { return s != nil }
func (s *InsertStatement) node() bool             { return s != nil }
func (s *PragmaStatement) node() bool             { return s != nil }
func (s *ReindexStatement) node() bool            { return s != nil }
func (s *ReleaseStatement) node() bool            { return s != nil }
func (s *RollbackStatement) node() bool           { return s != nil }
func (s *SavepointStatement) node() bool          { return s != nil }
func (s *SelectStatement) node() bool             { return s != nil }
func (s *UpdateStatement) node() bool             { return s != nil }
func (s *AttachStatement) node() bool             { return s != nil }
func (s *DetachStatement) node() bool             { return s != nil }
func (s *VacuumStatement) node() bool             { return s != nil }

// exprs
func (s *UnaryExpr) node() bool     { return s != nil }
func (s *BinaryExpr) node() bool    { return s != nil }
func (s *BindExpr) node() bool      { return s != nil }
func (s *BlobLit) node() bool       { return s != nil }
func (s *BoolLit) node() bool       { return s != nil }
func (s *Call) node() bool          { return s != nil }
func (s *CaseExpr) node() bool      { return s != nil }
func (s *CastExpr) node() bool      { return s != nil }
func (s *Exists) node() bool        { return s != nil }
func (s *Null) node() bool          { return s != nil }
func (s *ExprList) node() bool      { return s != nil }
func (s *Ident) node() bool         { return s != nil }
func (s *NullLit) node() bool       { return s != nil }
func (s *NumberLit) node() bool     { return s != nil }
func (s *QualifiedRef) node() bool  { return s != nil }
func (s *Raise) node() bool         { return s != nil }
func (s *StringLit) node() bool     { return s != nil }
func (s *TimestampLit) node() bool  { return s != nil }
func (s *InExpr) node() bool        { return s != nil }
func (s *ParenExpr) node() bool     { return s != nil }
func (s *JoinClause) node() bool    { return s != nil }
func (s *ParenSource) node() bool   { return s != nil }
func (s *QualifiedName) node() bool { return s != nil }

// constraints
func (s *OnConstraint) node() bool         { return s != nil }
func (s *UsingConstraint) node() bool      { return s != nil }
func (s *PrimaryKeyConstraint) node() bool { return s != nil }
func (s *NotNullConstraint) node() bool    { return s != nil }
func (s *UniqueConstraint) node() bool     { return s != nil }
func (s *CheckConstraint) node() bool      { return s != nil }
func (s *DefaultConstraint) node() bool    { return s != nil }
func (s *GeneratedConstraint) node() bool  { return s != nil }
func (s *CollateConstraint) node() bool    { return s != nil }
func (s *ForeignKeyConstraint) node() bool { return s != nil }

// inner clauses
func (s *ColumnDefinition) node() bool { return s != nil }
func (s *Type) node() bool             { return s != nil }
func (s *ConflictClause) node() bool   { return s != nil }
func (s *IndexedColumn) node() bool    { return s != nil }
func (s *ForeignKeyArg) node() bool    { return s != nil }
func (s *ModuleArgument) node() bool   { return s != nil }
func (s *CaseBlock) node() bool        { return s != nil }
func (s *WindowDefinition) node() bool { return s != nil }
func (s *WithClause) node() bool       { return s != nil }
func (s *UpsertClause) node() bool     { return s != nil }
func (s *ResultColumn) node() bool     { return s != nil }
func (s *Assignment) node() bool       { return s != nil }
func (s *OrderingTerm) node() bool     { return s != nil }
func (s *Window) node() bool           { return s != nil }
func (s *FunctionArg) node() bool      { return s != nil }
func (s *JoinOperator) node() bool     { return s != nil }
func (s *CTE) node() bool              { return s != nil }
func (s *FrameSpec) node() bool        { return s != nil }

type Statement interface {
	Node
	stmt()
}

func (*AlterTableStatement) stmt()         {}
func (*AnalyzeStatement) stmt()            {}
func (*BeginStatement) stmt()              {}
func (*CommitStatement) stmt()             {}
func (*CreateIndexStatement) stmt()        {}
func (*CreateTableStatement) stmt()        {}
func (*CreateTriggerStatement) stmt()      {}
func (*CreateViewStatement) stmt()         {}
func (*CreateVirtualTableStatement) stmt() {}
func (*DeleteStatement) stmt()             {}
func (*DropIndexStatement) stmt()          {}
func (*DropTableStatement) stmt()          {}
func (*DropTriggerStatement) stmt()        {}
func (*DropViewStatement) stmt()           {}
func (*ExplainStatement) stmt()            {}
func (*InsertStatement) stmt()             {}
func (*PragmaStatement) stmt()             {}
func (*ReindexStatement) stmt()            {}
func (*ReleaseStatement) stmt()            {}
func (*RollbackStatement) stmt()           {}
func (*SavepointStatement) stmt()          {}
func (*SelectStatement) stmt()             {}
func (*UpdateStatement) stmt()             {}
func (*AttachStatement) stmt()             {}
func (*DetachStatement) stmt()             {}
func (*VacuumStatement) stmt()             {}

type Expr interface {
	Node
	expr()
}

func (*UnaryExpr) expr()       {}
func (*BinaryExpr) expr()      {}
func (*BindExpr) expr()        {}
func (*BlobLit) expr()         {}
func (*BoolLit) expr()         {}
func (*Call) expr()            {}
func (*CaseExpr) expr()        {}
func (*CastExpr) expr()        {}
func (*Exists) expr()          {}
func (*Null) expr()            {}
func (*ExprList) expr()        {}
func (*Ident) expr()           {}
func (*NullLit) expr()         {}
func (*NumberLit) expr()       {}
func (*QualifiedRef) expr()    {}
func (*Raise) expr()           {}
func (*StringLit) expr()       {}
func (*TimestampLit) expr()    {}
func (*SelectStatement) expr() {}
func (*InExpr) expr()          {}
func (*ParenExpr) expr()       {}

// Source represents a table or subquery.
type Source interface {
	Node
	source()
}

func (*JoinClause) source()      {}
func (*ParenSource) source()     {}
func (*QualifiedName) source()   {}
func (*SelectStatement) source() {}

// JoinConstraint represents either an ON or USING join constraint.
type JoinConstraint interface {
	Node
	joinConstraint()
}

func (*OnConstraint) joinConstraint()    {}
func (*UsingConstraint) joinConstraint() {}

type Constraint interface {
	Node
	constraint()
}

func (*PrimaryKeyConstraint) constraint() {}
func (*NotNullConstraint) constraint()    {}
func (*UniqueConstraint) constraint()     {}
func (*CheckConstraint) constraint()      {}
func (*DefaultConstraint) constraint()    {}
func (*GeneratedConstraint) constraint()  {}
func (*CollateConstraint) constraint()    {}
func (*ForeignKeyConstraint) constraint() {}

type ExplainStatement struct {
	Explain   bool
	QueryPlan bool
	Stmt      Statement // target statement
}

func (s *ExplainStatement) subnodes(yield func(Node) bool) bool {
	return yieldNodes(yield, s.Stmt)
}

// String returns the string representation of the statement.
func (s *ExplainStatement) String() string {
	var buf strings.Builder
	buf.WriteString("EXPLAIN")
	if s.QueryPlan {
		buf.WriteString(" QUERY PLAN")
	}
	fmt.Fprintf(&buf, " %s", s.Stmt.String())
	return buf.String()
}

type BeginStatement struct {
	Deferred  bool
	Immediate bool
	Exclusive bool
}

func (s *BeginStatement) subnodes(yield func(Node) bool) bool {
	return true
}

// String returns the string representation of the statement.
func (s *BeginStatement) String() string {
	var buf strings.Builder
	buf.WriteString("BEGIN")
	if s.Deferred {
		buf.WriteString(" DEFERRED")
	} else if s.Immediate {
		buf.WriteString(" IMMEDIATE")
	} else if s.Exclusive {
		buf.WriteString(" EXCLUSIVE")
	}

	return buf.String()
}

type CommitStatement struct{}

func (s *CommitStatement) subnodes(yield func(Node) bool) bool {
	return true
}

// String returns the string representation of the statement.
func (s *CommitStatement) String() string {
	var buf strings.Builder
	buf.WriteString("COMMIT")
	return buf.String()
}

type RollbackStatement struct {
	SavepointName *Ident // name of savepoint
}

func (s *RollbackStatement) subnodes(yield func(Node) bool) bool {
	return yieldNodes(yield, s.SavepointName)
}

// String returns the string representation of the statement.
func (s *RollbackStatement) String() string {
	var buf strings.Builder
	buf.WriteString("ROLLBACK")

	if s.SavepointName != nil {
		buf.WriteString(" TO ")
		buf.WriteString(s.SavepointName.String())
	}
	return buf.String()
}

type SavepointStatement struct {
	Name *Ident // name of savepoint
}

func (s *SavepointStatement) subnodes(yield func(Node) bool) bool {
	return yieldNodes(yield, s.Name)
}

// String returns the string representation of the statement.
func (s *SavepointStatement) String() string {
	return fmt.Sprintf("SAVEPOINT %s", s.Name.String())
}

type ReleaseStatement struct {
	Name *Ident // name of savepoint
}

func (s *ReleaseStatement) subnodes(yield func(Node) bool) bool {
	return yieldNodes(yield, s.Name)
}

// String returns the string representation of the statement.
func (s *ReleaseStatement) String() string {
	var buf strings.Builder
	buf.WriteString("RELEASE ")
	buf.WriteString(s.Name.String())
	return buf.String()
}

type CreateTableStatement struct {
	Temp         bool
	IfNotExists  bool
	Name         *QualifiedName      // table name
	Columns      []*ColumnDefinition // column definitions
	Constraints  []Constraint        // table constraints
	WithoutRowID bool
	Strict       bool
	Select       *SelectStatement // select stmt to build from
}

func (s *CreateTableStatement) subnodes(yield func(Node) bool) bool {
	if !yieldNodes(yield, s.Name) {
		return false
	}

	for _, col := range s.Columns {
		if !yieldNodes(yield, col) {
			return false
		}
	}

	for _, col := range s.Constraints {
		if !yieldNodes(yield, col) {
			return false
		}
	}

	return yieldNodes(yield, s.Select)
}

// String returns the string representation of the statement.
func (s *CreateTableStatement) String() string {
	var buf strings.Builder
	buf.WriteString("CREATE ")

	if s.Temp {
		buf.WriteString("TEMP ")
	}

	buf.WriteString("TABLE ")

	if s.IfNotExists {
		buf.WriteString("IF NOT EXISTS ")
	}

	buf.WriteString(s.Name.String())

	if s.Select != nil {
		buf.WriteString(" AS ")
		buf.WriteString(s.Select.String())
	} else {
		buf.WriteString(" (")
		for i := range s.Columns {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(s.Columns[i].String())
		}
		for i := range s.Constraints {
			buf.WriteString(", ")
			buf.WriteString(s.Constraints[i].String())
		}
		buf.WriteString(")")
	}

	return buf.String()
}

type ColumnDefinition struct {
	Name        *Ident       // column name
	Type        *Type        // data type
	Constraints []Constraint // column constraints
}

func (c *ColumnDefinition) subnodes(yield func(Node) bool) bool {
	if !yieldNodes(yield, c.Name) {
		return false
	}

	if !yieldNodes(yield, c.Type) {
		return false
	}

	for _, constraint := range c.Constraints {
		if !yieldNodes(yield, constraint) {
			return false
		}
	}

	return true
}

// String returns the string representation of the statement.
func (c *ColumnDefinition) String() string {
	var buf strings.Builder
	buf.WriteString(c.Name.String())
	if c.Type != nil {
		buf.WriteString(" ")
		buf.WriteString(c.Type.String())
	}
	for i := range c.Constraints {
		buf.WriteString(" ")
		buf.WriteString(c.Constraints[i].String())
	}
	return buf.String()
}

type PrimaryKeyConstraint struct {
	Name          *Ident // constraint name (optional)
	Asc           bool
	Desc          bool
	Conflict      *ConflictClause // conflict clause (optional)
	Columns       []*Ident        // indexed columns (table only)
	Autoincrement bool
}

func (c *PrimaryKeyConstraint) subnodes(yield func(Node) bool) bool {
	if !yieldNodes(yield, c.Name) {
		return false
	}

	if !yieldNodes(yield, c.Conflict) {
		return false
	}

	for _, col := range c.Columns {
		if !yieldNodes(yield, col) {
			return false
		}
	}

	return true
}

// String returns the string representation of the constraint.
func (c *PrimaryKeyConstraint) String() string {
	var buf strings.Builder
	if c.Name != nil {
		buf.WriteString("CONSTRAINT ")
		buf.WriteString(c.Name.String())
		buf.WriteString(" ")
	}

	buf.WriteString("PRIMARY KEY")

	if c.Asc {
		buf.WriteString(" ASC")
	} else if c.Desc {
		buf.WriteString(" DESC")
	}

	if c.Conflict != nil {
		buf.WriteString(" ")
		buf.WriteString(c.Conflict.String())
	}

	if len(c.Columns) > 0 {
		buf.WriteString(" (")
		for i := range c.Columns {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(c.Columns[i].String())
		}
		buf.WriteString(")")
	}

	if c.Autoincrement {
		buf.WriteString(" AUTOINCREMENT")
	}
	return buf.String()
}

type NotNullConstraint struct {
	Name     *Ident          // constraint name (optional)
	Conflict *ConflictClause // conflict clause (optional)
}

func (c *NotNullConstraint) subnodes(yield func(Node) bool) bool {
	if !yieldNodes(yield, c.Name) {
		return false
	}

	if !yieldNodes(yield, c.Conflict) {
		return false
	}

	return true
}

// String returns the string representation of the constraint.
func (c *NotNullConstraint) String() string {
	var buf strings.Builder
	if c.Name != nil {
		buf.WriteString("CONSTRAINT ")
		buf.WriteString(c.Name.String())
		buf.WriteString(" ")
	}

	buf.WriteString("NOT NULL")

	if c.Conflict != nil {
		buf.WriteString(" ")
		buf.WriteString(c.Conflict.String())
	}

	return buf.String()
}

type UniqueConstraint struct {
	Name     *Ident           // constraint name (optional)
	Conflict *ConflictClause  // conflict clause (optional)
	Columns  []*IndexedColumn // indexed columns (table only)
}

func (c *UniqueConstraint) subnodes(yield func(Node) bool) bool {
	if !yieldNodes(yield, c.Name) {
		return false
	}

	if !yieldNodes(yield, c.Conflict) {
		return false
	}

	for _, col := range c.Columns {
		if !yieldNodes(yield, col) {
			return false
		}
	}

	return true
}

// String returns the string representation of the constraint.
func (c *UniqueConstraint) String() string {
	var buf strings.Builder
	if c.Name != nil {
		buf.WriteString("CONSTRAINT ")
		buf.WriteString(c.Name.String())
		buf.WriteString(" ")
	}

	buf.WriteString("UNIQUE")

	if c.Conflict != nil {
		buf.WriteString(" ")
		buf.WriteString(c.Conflict.String())
	}

	if len(c.Columns) > 0 {
		buf.WriteString(" (")
		for i := range c.Columns {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(c.Columns[i].String())
		}
		buf.WriteString(")")
	}

	return buf.String()
}

type CheckConstraint struct {
	Name *Ident // constraint name
	Expr Expr   // check expression
}

func (c *CheckConstraint) subnodes(yield func(Node) bool) bool {
	return yieldNodes(yield, c.Name, c.Expr)
}

// String returns the string representation of the constraint.
func (c *CheckConstraint) String() string {
	var buf strings.Builder
	if c.Name != nil {
		buf.WriteString("CONSTRAINT ")
		buf.WriteString(c.Name.String())
		buf.WriteString(" ")
	}

	buf.WriteString("CHECK (")
	buf.WriteString(c.Expr.String())
	buf.WriteString(")")
	return buf.String()
}

type DefaultConstraint struct {
	Name *Ident // constraint name
	Expr Expr   // default expression
}

func (c *DefaultConstraint) subnodes(yield func(Node) bool) bool {
	return yieldNodes(yield, c.Name, c.Expr)
}

// String returns the string representation of the constraint.
func (c *DefaultConstraint) String() string {
	var buf strings.Builder
	if c.Name != nil {
		buf.WriteString("CONSTRAINT ")
		buf.WriteString(c.Name.String())
		buf.WriteString(" ")
	}

	buf.WriteString("DEFAULT ")
	buf.WriteString(c.Expr.String())
	return buf.String()
}

type GeneratedConstraint struct {
	Name    *Ident // constraint name
	Expr    Expr   // default expression
	Stored  bool
	Virtual bool
}

func (c *GeneratedConstraint) subnodes(yield func(Node) bool) bool {
	return yieldNodes(yield, c.Name, c.Expr)
}

// String returns the string representation of the constraint.
func (c *GeneratedConstraint) String() string {
	var buf strings.Builder
	if c.Name != nil {
		buf.WriteString("CONSTRAINT ")
		buf.WriteString(c.Name.String())
		buf.WriteString(" ")
	}

	buf.WriteString("AS (")
	buf.WriteString(c.Expr.String())
	buf.WriteString(")")

	if c.Stored {
		buf.WriteString(" STORED")
	} else if c.Virtual {
		buf.WriteString(" VIRTUAL")
	}

	return buf.String()
}

type CollateConstraint struct {
	Name      *Ident // constraint name
	Collation *Ident // collation name
}

func (c *CollateConstraint) subnodes(yield func(Node) bool) bool {
	return yieldNodes(yield, c.Name, c.Collation)
}

// String returns the string representation of the constraint.
func (c *CollateConstraint) String() string {
	var buf strings.Builder
	if c.Name != nil {
		buf.WriteString("CONSTRAINT ")
		buf.WriteString(c.Name.String())
		buf.WriteString(" ")
	}

	buf.WriteString("COLLATE ")
	buf.WriteString(c.Collation.String())
	return buf.String()
}

type ForeignKeyConstraint struct {
	Name               *Ident           // constraint name
	Columns            []*Ident         // indexed columns (table only)
	ForeignTable       *Ident           // foreign table name
	ForeignColumns     []*Ident         // column list (optional)
	Args               []*ForeignKeyArg // arguments
	Deferrable         bool
	NotDeferrable      bool
	InitiallyDeferred  bool
	InitiallyImmediate bool
}

func (c *ForeignKeyConstraint) subnodes(yield func(Node) bool) bool {
	if !yieldNodes(yield, c.Name) {
		return false
	}

	for _, col := range c.Columns {
		if !yieldNodes(yield, col) {
			return false
		}
	}

	if !yieldNodes(yield, c.ForeignTable) {
		return false
	}

	for _, col := range c.ForeignColumns {
		if !yieldNodes(yield, col) {
			return false
		}
	}

	for _, arg := range c.Args {
		if !yieldNodes(yield, arg) {
			return false
		}
	}

	return true
}

// String returns the string representation of the constraint.
func (c *ForeignKeyConstraint) String() string {
	var buf strings.Builder
	if c.Name != nil {
		buf.WriteString("CONSTRAINT ")
		buf.WriteString(c.Name.String())
		buf.WriteString(" ")
	}

	if len(c.Columns) > 0 {
		buf.WriteString("FOREIGN KEY (")
		for i := range c.Columns {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(c.Columns[i].String())
		}
		buf.WriteString(") ")
	}

	buf.WriteString("REFERENCES ")
	buf.WriteString(c.ForeignTable.String())
	if len(c.ForeignColumns) > 0 {
		buf.WriteString(" (")
		for i := range c.ForeignColumns {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(c.ForeignColumns[i].String())
		}
		buf.WriteString(")")
	}

	for i := range c.Args {
		buf.WriteString(" ")
		buf.WriteString(c.Args[i].String())
	}

	if c.Deferrable {
		buf.WriteString(" DEFERRABLE")
	} else if c.NotDeferrable {
		buf.WriteString(" NOT DEFERRABLE")
	}

	if c.InitiallyDeferred {
		buf.WriteString(" INITIALLY DEFERRED")
	} else if c.InitiallyImmediate {
		buf.WriteString(" INITIALLY IMMEDIATE")
	}

	return buf.String()
}

type ForeignKeyArg struct {
	OnUpdate   bool
	OnDelete   bool
	SetNull    bool
	SetDefault bool
	Cascade    bool
	Restrict   bool
	NoAction   bool
}

func (a *ForeignKeyArg) subnodes(yield func(Node) bool) bool {
	return true
}

// String returns the string representation of the argument.
func (c *ForeignKeyArg) String() string {
	var buf strings.Builder
	buf.WriteString("ON")
	if c.OnUpdate {
		buf.WriteString(" UPDATE")
	} else {
		buf.WriteString(" DELETE")
	}

	if c.SetNull {
		buf.WriteString(" SET NULL")
	} else if c.SetDefault {
		buf.WriteString(" SET DEFAULT")
	} else if c.Cascade {
		buf.WriteString(" CASCADE")
	} else if c.Restrict {
		buf.WriteString(" RESTRICT")
	} else if c.NoAction {
		buf.WriteString(" NO ACTION")
	}
	return buf.String()
}

type CreateVirtualTableStatement struct {
	IfNotExists bool
	Name        *QualifiedName    // table name
	ModuleName  *Ident            // name of an object that implements the virtual table
	Arguments   []*ModuleArgument // module argument list (optional)
}

func (s *CreateVirtualTableStatement) subnodes(yield func(Node) bool) bool {
	if !yieldNodes(yield, s.Name, s.ModuleName) {
		return false
	}

	for _, arg := range s.Arguments {
		if !yieldNodes(yield, arg) {
			return false
		}
	}

	return true
}

// String returns the string representation of the statement.
func (s *CreateVirtualTableStatement) String() string {
	var buf strings.Builder
	buf.WriteString("CREATE VIRTUAL TABLE ")
	if s.IfNotExists {
		buf.WriteString("IF NOT EXISTS ")
	}

	buf.WriteString(s.Name.String())

	buf.WriteString(" USING ")
	buf.WriteString(s.ModuleName.String())
	buf.WriteString(" (")
	for i := range s.Arguments {
		if i != 0 {
			buf.WriteString(",")
		}
		buf.WriteString(s.Arguments[i].String())
	}
	buf.WriteString(")")
	return buf.String()
}

type ModuleArgument struct {
	Name    *Ident // argument name
	Literal Expr   // literal that is assigned to name (optional)
	Type    *Type  // type of Name, if Assign is set then Type cant be (optional)
}

func (a *ModuleArgument) subnodes(yield func(Node) bool) bool {
	if !yieldNodes(yield, a.Name, a.Literal) {
		return false
	}

	return yieldNodes(yield, a.Type)
}

func (a *ModuleArgument) String() string {
	var buf strings.Builder

	buf.WriteString(a.Name.String())
	if a.Literal != nil {
		buf.WriteString("=")
		buf.WriteString(a.Literal.String())
	} else if a.Type != nil {
		buf.WriteString(" ")
		buf.WriteString(a.Type.String())
	}

	return buf.String()
}

type AnalyzeStatement struct {
	Name *QualifiedName // table or index name (or schema.table, schema.index) (optional)
}

func (s *AnalyzeStatement) subnodes(yield func(Node) bool) bool {
	return yieldNodes(yield, s.Name)
}

// String returns the string representation of the statement.
func (s *AnalyzeStatement) String() string {
	if s.Name == nil {
		return "ANALYZE"
	}

	return fmt.Sprintf("ANALYZE %s", s.Name.String())
}

type ReindexStatement struct {
	Name *QualifiedName // collation, index or table name (or schema.table, schema.index)
}

func (s *ReindexStatement) subnodes(yield func(Node) bool) bool {
	return yieldNodes(yield, s.Name)
}

// String returns the string representation of the statement.
func (s *ReindexStatement) String() string {
	if s.Name == nil {
		return "REINDEX"
	}
	return fmt.Sprintf("REINDEX %s", s.Name.String())
}

type AlterTableStatement struct {
	Name          *QualifiedName    // table name
	NewName       *Ident            // new table name
	ColumnName    *Ident            // new column name
	NewColumnName *Ident            // new column name
	ColumnDef     *ColumnDefinition // new column definition
}

func (s *AlterTableStatement) subnodes(yield func(Node) bool) bool {
	if !yieldNodes(yield, s.Name, s.NewName, s.ColumnName, s.NewColumnName) {
		return false
	}

	return yieldNodes(yield, s.ColumnDef)
}

// String returns the string representation of the statement.
func (s *AlterTableStatement) String() string {
	var buf strings.Builder
	buf.WriteString("ALTER TABLE ")

	buf.WriteString(s.Name.String())

	if s.NewName != nil {
		buf.WriteString(" RENAME TO ")
		buf.WriteString(s.NewName.String())
	} else if s.ColumnName != nil {
		buf.WriteString(" RENAME COLUMN ")
		buf.WriteString(s.ColumnName.String())
		buf.WriteString(" TO ")
		buf.WriteString(s.NewColumnName.String())
	} else if s.ColumnDef != nil {
		buf.WriteString(" ADD COLUMN ")
		buf.WriteString(s.ColumnDef.String())
	}

	return buf.String()
}

type Ident struct {
	Quoted bool   // true if double quoted
	Name   string // identifier name
}

func (i *Ident) subnodes(yield func(Node) bool) bool {
	return true
}

// String returns the string representation of the expression.
func (i *Ident) String() string {
	return `"` + strings.Replace(i.Name, `"`, `""`, -1) + `"`
}

type Type struct {
	Name      *Ident     // type name
	Precision *NumberLit // precision (optional)
	Scale     *NumberLit // scale (optional)
}

func (t *Type) subnodes(yield func(Node) bool) bool {
	return yieldNodes(yield, t.Name, t.Precision, t.Scale)
}

// String returns the string representation of the type.
func (t *Type) String() string {
	if t.Precision != nil && t.Scale != nil {
		return fmt.Sprintf("%s(%s,%s)", t.Name.Name, t.Precision.String(), t.Scale.String())
	} else if t.Precision != nil {
		return fmt.Sprintf("%s(%s)", t.Name.Name, t.Precision.String())
	}
	return t.Name.Name
}

type StringLit struct {
	Value string // literal value (without quotes)
}

func (lit *StringLit) subnodes(yield func(Node) bool) bool {
	return true
}

// String returns the string representation of the expression.
func (lit *StringLit) String() string {
	return `'` + strings.Replace(lit.Value, `'`, `''`, -1) + `'`
}

type TimestampLit struct {
	Value string // literal value
}

func (lit *TimestampLit) subnodes(yield func(Node) bool) bool {
	return true
}

// String returns the string representation of the expression.
func (lit *TimestampLit) String() string {
	return lit.Value
}

type BlobLit struct {
	Value string // literal value
}

func (lit *BlobLit) subnodes(yield func(Node) bool) bool {
	return true
}

// String returns the string representation of the expression.
func (lit *BlobLit) String() string {
	return `x'` + lit.Value + `'`
}

type NumberLit struct {
	Value string // literal value
}

func (lit *NumberLit) subnodes(yield func(Node) bool) bool {
	return true
}

// String returns the string representation of the expression.
func (lit *NumberLit) String() string {
	return lit.Value
}

type NullLit struct{}

func (lit *NullLit) subnodes(yield func(Node) bool) bool {
	return true
}

// String returns the string representation of the expression.
func (lit *NullLit) String() string {
	return "NULL"
}

type BoolLit struct {
	Value bool // literal value
}

func (lit *BoolLit) subnodes(yield func(Node) bool) bool {
	return true
}

// String returns the string representation of the expression.
func (lit *BoolLit) String() string {
	if lit.Value {
		return "TRUE"
	}
	return "FALSE"
}

type BindExpr struct {
	Name string // binding name
}

func (expr *BindExpr) subnodes(yield func(Node) bool) bool {
	return true
}

// String returns the string representation of the expression.
func (expr *BindExpr) String() string {
	// TODO(BBJ): Support all bind characters.
	return expr.Name
}

type UnaryExpr struct {
	Op OpType // PLUS / MINUS / NOT / BITNOT
	X  Expr   // target expression
}

func (expr *UnaryExpr) subnodes(yield func(Node) bool) bool {
	return yieldNodes(yield, expr.X)
}

// String returns the string representation of the expression.
func (expr *UnaryExpr) String() string {
	switch expr.Op {
	case OP_PLUS:
		return "+" + expr.X.String()
	case OP_MINUS:
		return "-" + expr.X.String()
	case OP_NOT:
		return "NOT " + expr.X.String()
	case OP_BITNOT:
		return "~" + expr.X.String()
	default:
		panic("invalid op")
	}
}

type BinaryExpr struct {
	X  Expr   // lhs
	Op OpType // operator
	Y  Expr   // rhs
}

func (expr *BinaryExpr) subnodes(yield func(Node) bool) bool {
	return yieldNodes(yield, expr.X, expr.Y)
}

// String returns the string representation of the expression.
func (expr *BinaryExpr) String() string {
	switch expr.Op {
	case OP_PLUS:
		return expr.X.String() + " + " + expr.Y.String()
	case OP_MINUS:
		return expr.X.String() + " - " + expr.Y.String()
	case OP_MULTIPLY:
		return expr.X.String() + " * " + expr.Y.String()
	case OP_DIVIDE:
		return expr.X.String() + " / " + expr.Y.String()
	case OP_MODULO:
		return expr.X.String() + " % " + expr.Y.String()
	case OP_CONCAT:
		return expr.X.String() + " || " + expr.Y.String()
	case OP_BETWEEN:
		return expr.X.String() + " BETWEEN " + expr.Y.String()
	case OP_NOT_BETWEEN:
		return expr.X.String() + " NOT BETWEEN " + expr.Y.String()
	case OP_LSHIFT:
		return expr.X.String() + " << " + expr.Y.String()
	case OP_RSHIFT:
		return expr.X.String() + " >> " + expr.Y.String()
	case OP_BITAND:
		return expr.X.String() + " & " + expr.Y.String()
	case OP_BITOR:
		return expr.X.String() + " | " + expr.Y.String()
	case OP_LT:
		return expr.X.String() + " < " + expr.Y.String()
	case OP_LE:
		return expr.X.String() + " <= " + expr.Y.String()
	case OP_GT:
		return expr.X.String() + " > " + expr.Y.String()
	case OP_GE:
		return expr.X.String() + " >= " + expr.Y.String()
	case OP_EQ:
		return expr.X.String() + " = " + expr.Y.String()
	case OP_NE:
		return expr.X.String() + " != " + expr.Y.String()
	case OP_JSON_EXTRACT_JSON:
		return expr.X.String() + " -> " + expr.Y.String()
	case OP_JSON_EXTRACT_SQL:
		return expr.X.String() + " ->> " + expr.Y.String()
	case OP_IS:
		return expr.X.String() + " IS " + expr.Y.String()
	case OP_IS_NOT:
		return expr.X.String() + " IS NOT " + expr.Y.String()
	case OP_LIKE:
		return expr.X.String() + " LIKE " + expr.Y.String()
	case OP_NOT_LIKE:
		return expr.X.String() + " NOT LIKE " + expr.Y.String()
	case OP_GLOB:
		return expr.X.String() + " GLOB " + expr.Y.String()
	case OP_NOT_GLOB:
		return expr.X.String() + " NOT GLOB " + expr.Y.String()
	case OP_MATCH:
		return expr.X.String() + " MATCH " + expr.Y.String()
	case OP_NOT_MATCH:
		return expr.X.String() + " NOT MATCH " + expr.Y.String()
	case OP_REGEXP:
		return expr.X.String() + " REGEXP " + expr.Y.String()
	case OP_NOT_REGEXP:
		return expr.X.String() + " NOT REGEXP " + expr.Y.String()
	case OP_AND:
		return expr.X.String() + " AND " + expr.Y.String()
	case OP_OR:
		return expr.X.String() + " OR " + expr.Y.String()
	case OP_IS_DISTINCT_FROM:
		return expr.X.String() + " IS DISTINCT FROM " + expr.Y.String()
	case OP_IS_NOT_DISTINCT_FROM:
		return expr.X.String() + " IS NOT DISTINCT FROM " + expr.Y.String()
	case OP_ESCAPE:
		return expr.X.String() + " ESCAPE " + expr.Y.String()
	case OP_COLLATE:
		return expr.X.String() + " COLLATE " + expr.Y.String()
	default:
		panic("invalid op")
	}
}

type CastExpr struct {
	X    Expr  // target expression
	Type *Type // cast type
}

func (expr *CastExpr) subnodes(yield func(Node) bool) bool {
	if !yieldNodes(yield, expr.X) {
		return false
	}

	return yieldNodes(yield, expr.Type)
}

// String returns the string representation of the expression.
func (expr *CastExpr) String() string {
	return fmt.Sprintf("CAST(%s AS %s)", expr.X.String(), expr.Type.String())
}

type CaseExpr struct {
	Operand  Expr         // optional condition after the CASE keyword
	Blocks   []*CaseBlock // list of WHEN/THEN pairs
	ElseExpr Expr         // expression used by default case
}

func (expr *CaseExpr) subnodes(yield func(Node) bool) bool {
	if !yieldNodes(yield, expr.Operand) {
		return false
	}
	for _, blk := range expr.Blocks {
		if !yieldNodes(yield, blk) {
			return false
		}
	}

	return yieldNodes(yield, expr.ElseExpr)
}

// String returns the string representation of the expression.
func (expr *CaseExpr) String() string {
	var buf strings.Builder
	buf.WriteString("CASE")
	if expr.Operand != nil {
		buf.WriteString(" ")
		buf.WriteString(expr.Operand.String())
	}
	for _, blk := range expr.Blocks {
		buf.WriteString(" ")
		buf.WriteString(blk.String())
	}
	if expr.ElseExpr != nil {
		buf.WriteString(" ELSE ")
		buf.WriteString(expr.ElseExpr.String())
	}
	buf.WriteString(" END")
	return buf.String()
}

type CaseBlock struct {
	Condition Expr // block condition
	Body      Expr // result expression
}

func (b *CaseBlock) subnodes(yield func(Node) bool) bool {
	return yieldNodes(yield, b.Condition, b.Body)
}

// String returns the string representation of the block.
func (b *CaseBlock) String() string {
	return fmt.Sprintf("WHEN %s THEN %s", b.Condition.String(), b.Body.String())
}

type Raise struct {
	Ignore   bool
	Rollback bool
	Abort    bool
	Fail     bool
	Error    *StringLit // error message
}

func (r *Raise) subnodes(yield func(Node) bool) bool {
	return yieldNodes(yield, r.Error)
}

// String returns the string representation of the raise function.
func (r *Raise) String() string {
	var buf strings.Builder
	buf.WriteString("RAISE(")
	if r.Rollback {
		fmt.Fprintf(&buf, "ROLLBACK, %s", r.Error.String())
	} else if r.Abort {
		fmt.Fprintf(&buf, "ABORT, %s", r.Error.String())
	} else if r.Fail {
		fmt.Fprintf(&buf, "FAIL, %s", r.Error.String())
	} else {
		buf.WriteString("IGNORE")
	}
	buf.WriteString(")")
	return buf.String()
}

type Exists struct {
	Not    bool
	Select *SelectStatement // select statement
}

func (expr *Exists) subnodes(yield func(Node) bool) bool {
	return yieldNodes(yield, expr.Select)
}

// String returns the string representation of the expression.
func (expr *Exists) String() string {
	if expr.Not {
		return fmt.Sprintf("NOT EXISTS (%s)", expr.Select.String())
	}
	return fmt.Sprintf("EXISTS (%s)", expr.Select.String())
}

type Null struct {
	X  Expr   // expression being checked for null
	Op OpType // NOTNULl / ISNULL
}

func (expr *Null) subnodes(yield func(Node) bool) bool {
	return yieldNodes(yield, expr.X)
}

// String returns the string representation of the expression.
func (expr *Null) String() string {
	var buf strings.Builder

	buf.WriteString(expr.X.String())
	switch expr.Op {
	case OP_ISNULL:
		buf.WriteString(" IS NULL")
	case OP_NOTNULL:
		buf.WriteString(" NOT NULL")
	default:
		panic("invalid op")
	}

	return buf.String()
}

type ExprList struct {
	Exprs []Expr // list of expressions
}

func (l *ExprList) subnodes(yield func(Node) bool) bool {
	for _, expr := range l.Exprs {
		if !yieldNodes(yield, expr) {
			return false
		}
	}
	return true
}

// String returns the string representation of the expression.
func (l *ExprList) String() string {
	var buf strings.Builder
	buf.WriteString("(")
	for i, expr := range l.Exprs {
		if i != 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(expr.String())
	}
	buf.WriteString(")")
	return buf.String()
}

type QualifiedRef struct {
	Table  *QualifiedName // table name
	Star   bool
	Column *Ident // column name (optional, if star)
}

func (r *QualifiedRef) subnodes(yield func(Node) bool) bool {
	return yieldNodes(yield, r.Table, r.Column)
}

// String returns the string representation of the expression.
func (r *QualifiedRef) String() string {
	if r.Star {
		return fmt.Sprintf("%s.*", r.Table.String())
	}
	return fmt.Sprintf("%s.%s", r.Table.String(), r.Column.String())
}

type Call struct {
	Name       *QualifiedName    // function name
	Filter     Expr              // filter clause (optional)
	OverName   *Ident            // over name (optional)
	OverWindow *WindowDefinition // over window (optional)
}

func (c *Call) subnodes(yield func(Node) bool) bool {
	if !yieldNodes(yield, c.Name, c.Filter, c.OverName) {
		return false
	}

	return yieldNodes(yield, c.OverWindow)
}

// String returns the string representation of the expression.
func (c *Call) String() string {
	var buf strings.Builder
	buf.WriteString(c.Name.String())

	if c.Filter != nil {
		buf.WriteString(" FILTER (WHERE ")
		buf.WriteString(c.Filter.String())
		buf.WriteString(")")
	}

	if c.OverName != nil {
		buf.WriteString(" OVER ")
		buf.WriteString(c.OverName.String())
	} else if c.OverWindow != nil {
		buf.WriteString(" OVER ")
		buf.WriteString(c.OverWindow.String())
	}

	return buf.String()
}

type OrderingTerm struct {
	X Expr // ordering expression

	Asc        bool
	Desc       bool
	NullsFirst bool
	NullsLast  bool
}

func (t *OrderingTerm) subnodes(yield func(Node) bool) bool {
	return yieldNodes(yield, t.X)
}

// String returns the string representation of the term.
func (t *OrderingTerm) String() string {
	var buf strings.Builder
	buf.WriteString(t.X.String())

	if t.Asc {
		buf.WriteString(" ASC")
	} else if t.Desc {
		buf.WriteString(" DESC")
	}

	if t.NullsFirst {
		buf.WriteString(" NULLS FIRST")
	} else if t.NullsLast {
		buf.WriteString(" NULLS LAST")
	}

	return buf.String()
}

type FrameSpec struct {
	Range   bool
	Rows    bool
	Groups  bool
	Between bool // (optional)

	X           Expr // lhs expression (optional)
	CurrentRowX bool
	FollowingX  bool
	PrecedingX  bool
	UnboundedX  bool

	Y           Expr // rhs expression
	CurrentRowY bool
	FollowingY  bool
	PrecedingY  bool
	UnboundedY  bool

	ExcludeNoOthers   bool
	ExcludeCurrentRow bool
	ExcludeGroup      bool
	ExcludeTies       bool
}

func (s *FrameSpec) subnodes(yield func(Node) bool) bool {
	return yieldNodes(yield, s.X, s.Y)
}

// String returns the string representation of the frame spec.
func (s *FrameSpec) String() string {
	var buf strings.Builder
	if s.Range {
		buf.WriteString("RANGE")
	} else if s.Rows {
		buf.WriteString("ROWS")
	} else if s.Groups {
		buf.WriteString("GROUPS")
	}

	if s.Between {
		buf.WriteString(" BETWEEN")
		if s.UnboundedX && s.PrecedingX {
			buf.WriteString(" UNBOUNDED PRECEDING")
		} else if s.X != nil && s.PrecedingX {
			fmt.Fprintf(&buf, " %s PRECEDING", s.X.String())
		} else if s.CurrentRowX {
			buf.WriteString(" CURRENT ROW")
		} else if s.X != nil && s.FollowingX {
			fmt.Fprintf(&buf, " %s FOLLOWING", s.X.String())
		}

		buf.WriteString(" AND")

		if s.Y != nil && s.PrecedingY {
			fmt.Fprintf(&buf, " %s PRECEDING", s.Y.String())
		} else if s.CurrentRowY {
			buf.WriteString(" CURRENT ROW")
		} else if s.Y != nil && s.FollowingY {
			fmt.Fprintf(&buf, " %s FOLLOWING", s.Y.String())
		} else if s.UnboundedY && s.FollowingY {
			buf.WriteString(" UNBOUNDED FOLLOWING")
		}
	} else {
		if s.UnboundedX && s.PrecedingX {
			buf.WriteString(" UNBOUNDED PRECEDING")
		} else if s.X != nil && s.PrecedingX {
			fmt.Fprintf(&buf, " %s PRECEDING", s.X.String())
		} else if s.CurrentRowX {
			buf.WriteString(" CURRENT ROW")
		}
	}

	if s.ExcludeNoOthers {
		buf.WriteString(" EXCLUDE NO OTHERS")
	} else if s.ExcludeCurrentRow {
		buf.WriteString(" EXCLUDE CURRENT ROW")
	} else if s.ExcludeGroup {
		buf.WriteString(" EXCLUDE GROUP")
	} else if s.ExcludeTies {
		buf.WriteString(" EXCLUDE TIES")
	}

	return buf.String()
}

type DropTableStatement struct {
	IfExists bool
	Name     *QualifiedName // table name
}

func (s *DropTableStatement) subnodes(yield func(Node) bool) bool {
	return yieldNodes(yield, s.Name)
}

// String returns the string representation of the statement.
func (s *DropTableStatement) String() string {
	var buf strings.Builder
	buf.WriteString("DROP TABLE ")
	if s.IfExists {
		buf.WriteString("IF EXISTS ")
	}

	buf.WriteString(s.Name.String())
	return buf.String()
}

type CreateViewStatement struct {
	Temp        bool
	IfNotExists bool
	Name        *QualifiedName   // view name
	Columns     []*Ident         // column list
	Select      *SelectStatement // source statement
}

func (s *CreateViewStatement) subnodes(yield func(Node) bool) bool {
	if !yieldNodes(yield, s.Name) {
		return false
	}

	for _, col := range s.Columns {
		if !yieldNodes(yield, col) {
			return false
		}
	}

	return yieldNodes(yield, s.Select)
}

// String returns the string representation of the statement.
func (s *CreateViewStatement) String() string {
	var buf strings.Builder
	buf.WriteString("CREATE ")

	if s.Temp {
		buf.WriteString("TEMP ")
	}

	buf.WriteString("VIEW ")

	if s.IfNotExists {
		buf.WriteString("IF NOT EXISTS ")
	}

	buf.WriteString(s.Name.String())

	if len(s.Columns) > 0 {
		buf.WriteString(" (")
		for i, col := range s.Columns {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(col.String())
		}
		buf.WriteString(")")
	}

	fmt.Fprintf(&buf, " AS %s", s.Select.String())

	return buf.String()
}

type DropViewStatement struct {
	IfExists bool
	Name     *QualifiedName // view name
}

func (s *DropViewStatement) subnodes(yield func(Node) bool) bool {
	return yieldNodes(yield, s.Name)
}

// String returns the string representation of the statement.
func (s *DropViewStatement) String() string {
	var buf strings.Builder
	buf.WriteString("DROP VIEW ")
	if s.IfExists {
		buf.WriteString("IF EXISTS ")
	}

	buf.WriteString(s.Name.String())
	return buf.String()
}

type CreateIndexStatement struct {
	Unique      bool
	IfNotExists bool
	Name        *QualifiedName   // index name
	Table       *Ident           // table name
	Columns     []*IndexedColumn // column list
	WhereExpr   Expr             // conditional expression
}

func (s *CreateIndexStatement) subnodes(yield func(Node) bool) bool {
	if !yieldNodes(yield, s.Name, s.Table) {
		return false
	}

	for _, col := range s.Columns {
		if !yieldNodes(yield, col) {
			return false
		}
	}

	return yieldNodes(yield, s.WhereExpr)
}

// String returns the string representation of the statement.
func (s *CreateIndexStatement) String() string {
	var buf strings.Builder
	buf.WriteString("CREATE")
	if s.Unique {
		buf.WriteString(" UNIQUE")
	}
	buf.WriteString(" INDEX")
	if s.IfNotExists {
		buf.WriteString(" IF NOT EXISTS")
	}

	fmt.Fprintf(&buf, " %s ON %s ", s.Name.String(), s.Table.String())

	buf.WriteString("(")
	for i, col := range s.Columns {
		if i != 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(col.String())
	}
	buf.WriteString(")")

	if s.WhereExpr != nil {
		fmt.Fprintf(&buf, " WHERE %s", s.WhereExpr.String())
	}

	return buf.String()
}

type DropIndexStatement struct {
	IfExists bool
	Name     *QualifiedName // index name
}

func (s *DropIndexStatement) subnodes(yield func(Node) bool) bool {
	return yieldNodes(yield, s.Name)
}

// String returns the string representation of the statement.
func (s *DropIndexStatement) String() string {
	var buf strings.Builder
	buf.WriteString("DROP INDEX ")
	if s.IfExists {
		buf.WriteString("IF EXISTS ")
	}

	buf.WriteString(s.Name.String())
	return buf.String()
}

type CreateTriggerStatement struct {
	Temp        bool
	IfNotExists bool
	Name        *QualifiedName // trigger name

	Before    bool
	After     bool
	InsteadOf bool

	Delete          bool
	Insert          bool
	Update          bool
	UpdateOfColumns []*Ident
	Table           *Ident // table name

	ForEachRow bool
	WhenExpr   Expr

	Body []Statement // trigger body
}

func (s *CreateTriggerStatement) subnodes(yield func(Node) bool) bool {
	if !yieldNodes(yield, s.Name) {
		return false
	}

	for _, col := range s.UpdateOfColumns {
		if !yieldNodes(yield, col) {
			return false
		}
	}

	if !yieldNodes(yield, s.Table, s.WhenExpr) {
		return false
	}

	for _, stmt := range s.Body {
		if !yieldNodes(yield, stmt) {
			return false
		}
	}

	return true
}

// String returns the string representation of the statement.
func (s *CreateTriggerStatement) String() string {
	var buf strings.Builder
	buf.WriteString("CREATE ")

	if s.Temp {
		buf.WriteString("TEMP ")
	}

	buf.WriteString("TRIGGER ")

	if s.IfNotExists {
		buf.WriteString("IF NOT EXISTS ")
	}

	buf.WriteString(s.Name.String())

	if s.Before {
		buf.WriteString(" BEFORE")
	} else if s.After {
		buf.WriteString(" AFTER")
	} else if s.InsteadOf {
		buf.WriteString(" INSTEAD OF")
	}

	if s.Delete {
		buf.WriteString(" DELETE")
	} else if s.Insert {
		buf.WriteString(" INSERT")
	} else if s.Update {
		buf.WriteString(" UPDATE")
		if len(s.UpdateOfColumns) > 0 {
			buf.WriteString(" OF ")
			for i, col := range s.UpdateOfColumns {
				if i != 0 {
					buf.WriteString(", ")
				}
				buf.WriteString(col.String())
			}
		}
	}

	fmt.Fprintf(&buf, " ON %s", s.Table.String())

	if s.ForEachRow {
		buf.WriteString(" FOR EACH ROW")
	}

	if s.WhenExpr != nil {
		fmt.Fprintf(&buf, " WHEN %s", s.WhenExpr.String())
	}

	buf.WriteString(" BEGIN")
	for i := range s.Body {
		fmt.Fprintf(&buf, " %s;", s.Body[i].String())
	}
	buf.WriteString(" END")

	return buf.String()
}

type DropTriggerStatement struct {
	IfExists bool
	Name     *QualifiedName // trigger name
}

func (s *DropTriggerStatement) subnodes(yield func(Node) bool) bool {
	return yieldNodes(yield, s.Name)
}

// String returns the string representation of the statement.
func (s *DropTriggerStatement) String() string {
	var buf strings.Builder
	buf.WriteString("DROP TRIGGER ")
	if s.IfExists {
		buf.WriteString("IF EXISTS ")
	}

	buf.WriteString(s.Name.String())
	return buf.String()
}

type InsertStatement struct {
	WithClause *WithClause // clause containing CTEs

	Replace          bool
	InsertOrReplace  bool
	InsertOrRollback bool
	InsertOrAbort    bool
	InsertOrFail     bool
	InsertOrIgnore   bool
	Table            *QualifiedName   // table name (or schema.table)
	Columns          []*Ident         // optional column list
	ValueLists       []*ExprList      // lists of lists of values
	Select           *SelectStatement // SELECT statement
	DefaultValues    bool
	UpsertClause     *UpsertClause   // optional upsert clause
	ReturningColumns []*ResultColumn // list of result columns
}

func (s *InsertStatement) subnodes(yield func(Node) bool) bool {
	if !yieldNodes(yield, s.WithClause) {
		return false
	}

	if !yieldNodes(yield, s.Table) {
		return false
	}

	for _, col := range s.Columns {
		if !yieldNodes(yield, col) {
			return false
		}
	}

	for _, valueList := range s.ValueLists {
		if !yieldNodes(yield, valueList) {
			return false
		}
	}

	if !yieldNodes(yield, s.Select) {
		return false
	}

	if !yieldNodes(yield, s.UpsertClause) {
		return false
	}

	for _, col := range s.ReturningColumns {
		if !yieldNodes(yield, col) {
			return false
		}
	}

	return true
}

// String returns the string representation of the statement.
func (s *InsertStatement) String() string {
	var buf strings.Builder
	if s.WithClause != nil {
		buf.WriteString(s.WithClause.String())
		buf.WriteString(" ")
	}

	if s.Replace {
		buf.WriteString("REPLACE")
	} else {
		buf.WriteString("INSERT")
		if s.InsertOrReplace {
			buf.WriteString(" OR REPLACE")
		} else if s.InsertOrRollback {
			buf.WriteString(" OR ROLLBACK")
		} else if s.InsertOrAbort {
			buf.WriteString(" OR ABORT")
		} else if s.InsertOrFail {
			buf.WriteString(" OR FAIL")
		} else if s.InsertOrIgnore {
			buf.WriteString(" OR IGNORE")
		}
	}

	buf.WriteString(" INTO ")

	buf.WriteString(s.Table.String())

	if len(s.Columns) != 0 {
		buf.WriteString(" (")
		for i, col := range s.Columns {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(col.String())
		}
		buf.WriteString(")")
	}

	if s.DefaultValues {
		buf.WriteString(" DEFAULT VALUES")
	} else if s.Select != nil {
		fmt.Fprintf(&buf, " %s", s.Select.String())
	} else {
		buf.WriteString(" VALUES")
		for i := range s.ValueLists {
			if i != 0 {
				buf.WriteString(",")
			}
			buf.WriteString(" (")
			for j, expr := range s.ValueLists[i].Exprs {
				if j != 0 {
					buf.WriteString(", ")
				}
				buf.WriteString(expr.String())
			}
			buf.WriteString(")")
		}
	}

	if s.UpsertClause != nil {
		fmt.Fprintf(&buf, " %s", s.UpsertClause.String())
	}

	if len(s.ReturningColumns) > 0 {
		buf.WriteString(" RETURNING ")
		for i, col := range s.ReturningColumns {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(col.String())
		}
	}

	return buf.String()
}

type UpsertClause struct {
	Columns         []*IndexedColumn // optional indexed column list
	WhereExpr       Expr             // optional conditional expression
	DoNothing       bool
	DoUpdateSet     bool
	Assignments     []*Assignment // list of column assignments
	UpdateWhereExpr Expr          // optional conditional expression for DO UPDATE SET
}

func (c *UpsertClause) subnodes(yield func(Node) bool) bool {
	for _, col := range c.Columns {
		if !yieldNodes(yield, col) {
			return false
		}
	}

	if !yieldNodes(yield, c.WhereExpr) {
		return false
	}

	for _, assignment := range c.Assignments {
		if !yieldNodes(yield, assignment) {
			return false
		}
	}

	return yieldNodes(yield, c.UpdateWhereExpr)
}

// String returns the string representation of the clause.
func (c *UpsertClause) String() string {
	var buf strings.Builder
	buf.WriteString("ON CONFLICT")

	if len(c.Columns) != 0 {
		buf.WriteString(" (")
		for i, col := range c.Columns {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(col.String())
		}
		buf.WriteString(")")

		if c.WhereExpr != nil {
			fmt.Fprintf(&buf, " WHERE %s", c.WhereExpr.String())
		}
	}

	buf.WriteString(" DO")
	if c.DoNothing {
		buf.WriteString(" NOTHING")
	} else {
		buf.WriteString(" UPDATE SET ")
		for i := range c.Assignments {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(c.Assignments[i].String())
		}

		if c.UpdateWhereExpr != nil {
			fmt.Fprintf(&buf, " WHERE %s", c.UpdateWhereExpr.String())
		}
	}

	return buf.String()
}

type UpdateStatement struct {
	WithClause       *WithClause // clause containing CTEs
	UpdateOrReplace  bool
	UpdateOrRollback bool
	UpdateOrAbort    bool
	UpdateOrFail     bool
	UpdateOrIgnore   bool
	Table            *QualifiedName  // table name
	Assignments      []*Assignment   // list of column assignments
	WhereExpr        Expr            // conditional expression
	ReturningColumns []*ResultColumn // list of result columns
	OrderingTerms    []*OrderingTerm // terms of ORDER BY clause
	LimitExpr        Expr            // limit expression
	OffsetExpr       Expr            // offset expression
}

func (s *UpdateStatement) subnodes(yield func(Node) bool) bool {
	if !yieldNodes(yield, s.WithClause) {
		return false
	}

	if !yieldNodes(yield, s.Table) {
		return false
	}

	for _, assignment := range s.Assignments {
		if !yieldNodes(yield, assignment) {
			return false
		}
	}
	if !yieldNodes(yield, s.WhereExpr) {
		return false
	}

	for _, col := range s.ReturningColumns {
		if !yieldNodes(yield, col) {
			return false
		}
	}

	for _, term := range s.OrderingTerms {
		if !yieldNodes(yield, term) {
			return false
		}
	}
	return yieldNodes(yield, s.LimitExpr, s.OffsetExpr)
}

// String returns the string representation of the clause.
func (s *UpdateStatement) String() string {
	var buf strings.Builder
	if s.WithClause != nil {
		buf.WriteString(s.WithClause.String())
		buf.WriteString(" ")
	}

	buf.WriteString("UPDATE")
	if s.UpdateOrRollback {
		buf.WriteString(" OR ROLLBACK")
	} else if s.UpdateOrAbort {
		buf.WriteString(" OR ABORT")
	} else if s.UpdateOrReplace {
		buf.WriteString(" OR REPLACE")
	} else if s.UpdateOrFail {
		buf.WriteString(" OR FAIL")
	} else if s.UpdateOrIgnore {
		buf.WriteString(" OR IGNORE")
	}

	fmt.Fprintf(&buf, " %s ", s.Table.String())

	buf.WriteString("SET ")
	for i := range s.Assignments {
		if i != 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(s.Assignments[i].String())
	}

	if s.WhereExpr != nil {
		fmt.Fprintf(&buf, " WHERE %s", s.WhereExpr.String())
	}

	if len(s.ReturningColumns) > 0 {
		buf.WriteString(" RETURNING ")
		for i, col := range s.ReturningColumns {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(col.String())
		}
	}

	// Write ORDER BY.
	if len(s.OrderingTerms) != 0 {
		buf.WriteString(" ORDER BY ")
		for i, term := range s.OrderingTerms {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(term.String())
		}
	}

	// Write LIMIT/OFFSET.
	if s.LimitExpr != nil {
		fmt.Fprintf(&buf, " LIMIT %s", s.LimitExpr.String())
		if s.OffsetExpr != nil {
			fmt.Fprintf(&buf, " OFFSET %s", s.OffsetExpr.String())
		}
	}

	return buf.String()
}

type DeleteStatement struct {
	WithClause       *WithClause     // clause containing CTEs
	Table            *QualifiedName  // table name
	WhereExpr        Expr            // conditional expression
	ReturningColumns []*ResultColumn // list of result columns
	OrderingTerms    []*OrderingTerm // terms of ORDER BY clause
	LimitExpr        Expr            // limit expression
	OffsetExpr       Expr            // offset expression
}

func (s *DeleteStatement) subnodes(yield func(Node) bool) bool {
	if !yieldNodes(yield, s.WithClause) {
		return false
	}
	if !yieldNodes(yield, s.Table) {
		return false
	}
	if !yieldNodes(yield, s.WhereExpr) {
		return false
	}
	for _, col := range s.ReturningColumns {
		if !yieldNodes(yield, col) {
			return false
		}
	}
	for _, term := range s.OrderingTerms {
		if !yieldNodes(yield, term) {
			return false
		}
	}
	return yieldNodes(yield, s.LimitExpr, s.OffsetExpr)
}

// String returns the string representation of the clause.
func (s *DeleteStatement) String() string {
	var buf strings.Builder
	if s.WithClause != nil {
		buf.WriteString(s.WithClause.String())
		buf.WriteString(" ")
	}

	fmt.Fprintf(&buf, "DELETE FROM %s", s.Table.String())
	if s.WhereExpr != nil {
		fmt.Fprintf(&buf, " WHERE %s", s.WhereExpr.String())
	}

	if len(s.ReturningColumns) > 0 {
		buf.WriteString(" RETURNING ")
		for i, col := range s.ReturningColumns {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(col.String())
		}
	}

	// Write ORDER BY.
	if len(s.OrderingTerms) != 0 {
		buf.WriteString(" ORDER BY ")
		for i, term := range s.OrderingTerms {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(term.String())
		}
	}

	// Write LIMIT/OFFSET.
	if s.LimitExpr != nil {
		fmt.Fprintf(&buf, " LIMIT %s", s.LimitExpr.String())
		if s.OffsetExpr != nil {
			fmt.Fprintf(&buf, " OFFSET %s", s.OffsetExpr.String())
		}
	}

	return buf.String()
}

// Assignment is used within the UPDATE statement & upsert clause.
// It is similiar to an expression except that it must be an equality.
type Assignment struct {
	Columns []*Ident // column list
	Expr    Expr     // assigned expression
}

func (a *Assignment) subnodes(yield func(Node) bool) bool {
	for _, col := range a.Columns {
		if !yieldNodes(yield, col) {
			return false
		}
	}
	return yieldNodes(yield, a.Expr)
}

// String returns the string representation of the clause.
func (a *Assignment) String() string {
	var buf strings.Builder
	if len(a.Columns) == 1 {
		buf.WriteString(a.Columns[0].String())
	} else if len(a.Columns) > 1 {
		buf.WriteString("(")
		for i, col := range a.Columns {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(col.String())
		}
		buf.WriteString(")")
	}

	fmt.Fprintf(&buf, " = %s", a.Expr.String())
	return buf.String()
}

type IndexedColumn struct {
	X    Expr // column expression
	Asc  bool
	Desc bool
}

func (c *IndexedColumn) subnodes(yield func(Node) bool) bool {
	return yieldNodes(yield, c.X)
}

// String returns the string representation of the column.
func (c *IndexedColumn) String() string {
	var buf strings.Builder
	buf.WriteString(c.X.String())

	if c.Asc {
		buf.WriteString(" ASC")
	} else if c.Desc {
		buf.WriteString(" DESC")
	}

	return buf.String()
}

type SelectStatement struct {
	WithClause    *WithClause // clause containing CTEs
	ValueLists    []*ExprList // lists of lists of values
	Distinct      bool
	All           bool
	Columns       []*ResultColumn // list of result columns in the SELECT clause
	Source        Source          // chain of tables & subqueries in FROM clause
	WhereExpr     Expr            // condition for WHERE clause
	GroupByExprs  []Expr          // group by expression list
	HavingExpr    Expr            // HAVING expression
	Windows       []*Window       // window list
	UnionAll      bool
	Intersect     bool
	Except        bool
	Compound      *SelectStatement // compounded SELECT statement
	OrderingTerms []*OrderingTerm  // terms of ORDER BY clause
	LimitExpr     Expr             // limit expression
	OffsetExpr    Expr             // offset expression
}

func (s *SelectStatement) subnodes(yield func(Node) bool) bool {
	if !yieldNodes(yield, s.WithClause) {
		return false
	}

	for _, valueList := range s.ValueLists {
		if !yieldNodes(yield, valueList) {
			return false
		}
	}

	for _, col := range s.Columns {
		if !yieldNodes(yield, col) {
			return false
		}
	}

	if !yieldNodes(yield, s.Source, s.WhereExpr) {
		return false
	}

	for _, expr := range s.GroupByExprs {
		if !yieldNodes(yield, expr) {
			return false
		}
	}

	if !yieldNodes(yield, s.HavingExpr) {
		return false
	}

	for _, win := range s.Windows {
		if !yieldNodes(yield, win) {
			return false
		}
	}

	if !yieldNodes(yield, s.Compound) {
		return false
	}

	for _, term := range s.OrderingTerms {
		if !yieldNodes(yield, term) {
			return false
		}
	}

	return yieldNodes(yield, s.LimitExpr, s.OffsetExpr)
}

// String returns the string representation of the statement.
func (s *SelectStatement) String() string {
	var buf strings.Builder
	if s.WithClause != nil {
		buf.WriteString(s.WithClause.String())
		buf.WriteString(" ")
	}

	if len(s.ValueLists) > 0 {
		buf.WriteString("VALUES ")
		for i, exprs := range s.ValueLists {
			if i != 0 {
				buf.WriteString(", ")
			}

			buf.WriteString("(")
			for j, expr := range exprs.Exprs {
				if j != 0 {
					buf.WriteString(", ")
				}
				buf.WriteString(expr.String())
			}
			buf.WriteString(")")
		}
	} else {
		buf.WriteString("SELECT ")
		if s.Distinct {
			buf.WriteString("DISTINCT ")
		} else if s.All {
			buf.WriteString("ALL ")
		}

		for i, col := range s.Columns {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(col.String())
		}

		if s.Source != nil {
			fmt.Fprintf(&buf, " FROM %s", s.Source.String())
		}

		if s.WhereExpr != nil {
			fmt.Fprintf(&buf, " WHERE %s", s.WhereExpr.String())
		}

		if len(s.GroupByExprs) != 0 {
			buf.WriteString(" GROUP BY ")
			for i, expr := range s.GroupByExprs {
				if i != 0 {
					buf.WriteString(", ")
				}
				buf.WriteString(expr.String())
			}

			if s.HavingExpr != nil {
				fmt.Fprintf(&buf, " HAVING %s", s.HavingExpr.String())
			}
		}

		if len(s.Windows) != 0 {
			buf.WriteString(" WINDOW ")
			for i, window := range s.Windows {
				if i != 0 {
					buf.WriteString(", ")
				}
				buf.WriteString(window.String())
			}
		}
	}

	// Write compound operator.
	if s.Compound != nil {
		switch {
		case s.UnionAll:
			buf.WriteString(" UNION ALL")
		case s.Intersect:
			buf.WriteString(" INTERSECT")
		case s.Except:
			buf.WriteString(" EXCEPT")
		default:
			buf.WriteString(" UNION")
		}

		fmt.Fprintf(&buf, " %s", s.Compound.String())
	}

	// Write ORDER BY.
	if len(s.OrderingTerms) != 0 {
		buf.WriteString(" ORDER BY ")
		for i, term := range s.OrderingTerms {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(term.String())
		}
	}

	// Write LIMIT/OFFSET.
	if s.LimitExpr != nil {
		fmt.Fprintf(&buf, " LIMIT %s", s.LimitExpr.String())
		if s.OffsetExpr != nil {
			fmt.Fprintf(&buf, " OFFSET %s", s.OffsetExpr.String())
		}
	}

	return buf.String()
}

type ResultColumn struct {
	Star  bool
	Expr  Expr   // column expression (may be "tbl.*")
	Alias *Ident // alias name
}

func (c *ResultColumn) subnodes(yield func(Node) bool) bool {
	return yieldNodes(yield, c.Expr, c.Alias)
}

// String returns the string representation of the column.
func (c *ResultColumn) String() string {
	if c.Star {
		return "*"
	} else if c.Alias != nil {
		return fmt.Sprintf("%s AS %s", c.Expr.String(), c.Alias.String())
	}
	return c.Expr.String()
}

type QualifiedName struct {
	Schema           *Ident         // schema name (optional)
	Name             *Ident         // name
	FunctionCall     bool           // true if this is a function call
	FunctionStar     bool           // true if this is a star for function call
	FunctionDistinct bool           // true if this is a distinct for function call
	FunctionArgs     []*FunctionArg // function arguments (optional)
	Alias            *Ident         // optional table alias (optional)
	NotIndexed       bool
	Index            *Ident // name of index (optional)
}

func (s *QualifiedName) subnodes(yield func(Node) bool) bool {
	if !yieldNodes(yield, s.Schema, s.Name) {
		return false
	}

	for _, arg := range s.FunctionArgs {
		if !yieldNodes(yield, arg) {
			return false
		}
	}

	return yieldNodes(yield, s.Alias, s.Index)
}

// String returns the string representation of the table name.
func (n *QualifiedName) String() string {
	var buf strings.Builder
	if n.Schema != nil {
		buf.WriteString(n.Schema.String())
		buf.WriteString(".")
	}

	buf.WriteString(n.Name.String())

	if n.FunctionCall {
		buf.WriteString("(")

		if n.FunctionStar {
			buf.WriteString("*")
			assert(len(n.FunctionArgs) == 0)
			assert(!n.FunctionDistinct)
		}

		if n.FunctionDistinct {
			buf.WriteString("DISTINCT ")
		}

		for i, arg := range n.FunctionArgs {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(arg.String())
		}

		buf.WriteString(")")
	}

	if n.Alias != nil {
		fmt.Fprintf(&buf, " AS %s", n.Alias.String())
	}

	if n.Index != nil {
		fmt.Fprintf(&buf, " INDEXED BY %s", n.Index.String())
	} else if n.NotIndexed {
		buf.WriteString(" NOT INDEXED")
	}
	return buf.String()
}

type ParenSource struct {
	X     Source // nested source
	Alias *Ident // optional table alias (select source only)
}

func (s *ParenSource) subnodes(yield func(Node) bool) bool {
	return yieldNodes(yield, s.X, s.Alias)
}

// String returns the string representation of the source.
func (s *ParenSource) String() string {
	if s.Alias != nil {
		return fmt.Sprintf("(%s) AS %s", s.X.String(), s.Alias.String())
	}
	return fmt.Sprintf("(%s)", s.X.String())
}

type JoinClause struct {
	X          Source         // lhs source
	Operator   *JoinOperator  // join operator
	Y          Source         // rhs source
	Constraint JoinConstraint // join constraint
}

func (c *JoinClause) subnodes(yield func(Node) bool) bool {
	if !yieldNodes(yield, c.X) {
		return false
	}

	if !yieldNodes(yield, c.Operator) {
		return false
	}

	return yieldNodes(yield, c.Y, c.Constraint)
}

// String returns the string representation of the clause.
func (c *JoinClause) String() string {
	var buf strings.Builder

	// Print the left side
	buf.WriteString(c.X.String())

	// Print the operator
	buf.WriteString(c.Operator.String())

	// Handle the right side
	if y, ok := c.Y.(*JoinClause); ok {
		// Special case: right side is a JoinClause

		// Check if the X of the nested JoinClause is also a JoinClause
		if yx, ok := y.X.(*JoinClause); ok {
			// Handle the double-nested case

			// Print the first table of the inner JoinClause
			buf.WriteString(yx.X.String())

			// Add the constraint for the first join
			if c.Constraint != nil {
				fmt.Fprintf(&buf, " %s", c.Constraint.String())
			}

			// Print the operator of the inner JoinClause
			buf.WriteString(yx.Operator.String())

			// Print the second table of the inner JoinClause
			buf.WriteString(yx.Y.String())

			// Add the constraint for the inner JoinClause
			if yx.Constraint != nil {
				fmt.Fprintf(&buf, " %s", yx.Constraint.String())
			}

			// Print the operator of the outer JoinClause
			buf.WriteString(y.Operator.String())

			// Print the right side of the outer JoinClause
			buf.WriteString(y.Y.String())

			// Add the constraint for the outer JoinClause
			if y.Constraint != nil {
				fmt.Fprintf(&buf, " %s", y.Constraint.String())
			}
		} else {
			// Handle the singly-nested case

			// Print the left side of the nested JoinClause
			buf.WriteString(y.X.String())

			// Add the constraint for the first join
			if c.Constraint != nil {
				fmt.Fprintf(&buf, " %s", c.Constraint.String())
			}

			// Print the operator of the nested JoinClause
			buf.WriteString(y.Operator.String())

			// Print the right side of the nested JoinClause
			buf.WriteString(y.Y.String())

			// Add the constraint for the nested JoinClause
			if y.Constraint != nil {
				fmt.Fprintf(&buf, " %s", y.Constraint.String())
			}
		}
	} else {
		// Normal case: right side is not a JoinClause
		buf.WriteString(c.Y.String())

		// Add the constraint
		if c.Constraint != nil {
			fmt.Fprintf(&buf, " %s", c.Constraint.String())
		}
	}

	return buf.String()
}

type JoinOperator struct {
	Natural bool
	Left    bool
	Right   bool
	Full    bool
	Outer   bool
	Inner   bool
	Cross   bool
}

func (op *JoinOperator) subnodes(yield func(Node) bool) bool {
	return true
}

// String returns the string representation of the operator.
func (op *JoinOperator) String() string {
	if !op.Natural && !op.Left && !op.Right && !op.Full && !op.Outer && !op.Inner && !op.Cross {
		return ", "
	}

	var buf strings.Builder
	if op.Natural {
		buf.WriteString(" NATURAL")
	}

	if op.Left {
		buf.WriteString(" LEFT")
		if op.Outer {
			buf.WriteString(" OUTER")
		}
	} else if op.Right {
		buf.WriteString(" RIGHT")
		if op.Outer {
			buf.WriteString(" OUTER")
		}
	} else if op.Full {
		buf.WriteString(" FULL")
		if op.Outer {
			buf.WriteString(" OUTER")
		}
	} else if op.Inner {
		buf.WriteString(" INNER")
	} else if op.Cross {
		buf.WriteString(" CROSS")
	}
	buf.WriteString(" JOIN ")

	return buf.String()
}

type OnConstraint struct {
	X Expr // constraint expression
}

func (c *OnConstraint) subnodes(yield func(Node) bool) bool {
	return yieldNodes(yield, c.X)
}

// String returns the string representation of the constraint.
func (c *OnConstraint) String() string {
	return "ON " + c.X.String()
}

type UsingConstraint struct {
	Columns []*Ident // column list
}

func (c *UsingConstraint) subnodes(yield func(Node) bool) bool {
	for _, col := range c.Columns {
		if !yieldNodes(yield, col) {
			return false
		}
	}
	return true
}

// String returns the string representation of the constraint.
func (c *UsingConstraint) String() string {
	var buf strings.Builder
	buf.WriteString("USING (")
	for i, col := range c.Columns {
		if i != 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(col.String())
	}
	buf.WriteString(")")
	return buf.String()
}

type WithClause struct {
	Recursive bool
	CTEs      []*CTE // common table expressions
}

func (c *WithClause) subnodes(yield func(Node) bool) bool {
	for _, cte := range c.CTEs {
		if !yieldNodes(yield, cte) {
			return false
		}
	}
	return true
}

// String returns the string representation of the clause.
func (c *WithClause) String() string {
	var buf strings.Builder
	buf.WriteString("WITH ")
	if c.Recursive {
		buf.WriteString("RECURSIVE ")
	}

	for i, cte := range c.CTEs {
		if i != 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(cte.String())
	}

	return buf.String()
}

// CTE represents an AST node for a common table expression.
type CTE struct {
	TableName *Ident           // table name
	Columns   []*Ident         // optional column list
	Select    *SelectStatement // select statement
}

func (cte *CTE) subnodes(yield func(Node) bool) bool {
	if !yieldNodes(yield, cte.TableName) {
		return false
	}

	for _, col := range cte.Columns {
		if !yieldNodes(yield, col) {
			return false
		}
	}

	return yieldNodes(yield, cte.Select)
}

// String returns the string representation of the CTE.
func (cte *CTE) String() string {
	var buf strings.Builder
	fmt.Fprintf(&buf, "%s", cte.TableName.String())

	if len(cte.Columns) != 0 {
		buf.WriteString(" (")
		for i, col := range cte.Columns {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(col.String())
		}
		buf.WriteString(")")
	}

	fmt.Fprintf(&buf, " AS (%s)", cte.Select.String())

	return buf.String()
}

type Window struct {
	Name       *Ident            // name of window
	Definition *WindowDefinition // window definition
}

func (w *Window) subnodes(yield func(Node) bool) bool {
	if !yieldNodes(yield, w.Name) {
		return false
	}

	return yieldNodes(yield, w.Definition)
}

// String returns the string representation of the window.
func (w *Window) String() string {
	return fmt.Sprintf("%s AS %s", w.Name.String(), w.Definition.String())
}

type WindowDefinition struct {
	Base          *Ident          // base window name (optional)
	Partitions    []Expr          // partition expressions
	OrderingTerms []*OrderingTerm // ordering terms
	Frame         *FrameSpec      // frame
}

func (d *WindowDefinition) subnodes(yield func(Node) bool) bool {
	if !yieldNodes(yield, d.Base) {
		return false
	}

	for _, p := range d.Partitions {
		if !yieldNodes(yield, p) {
			return false
		}
	}

	for _, term := range d.OrderingTerms {
		if !yieldNodes(yield, term) {
			return false
		}
	}

	return yieldNodes(yield, d.Frame)
}

// String returns the string representation of the window definition.
func (d *WindowDefinition) String() string {
	var buf strings.Builder
	buf.WriteString("(")
	if d.Base != nil {
		buf.WriteString(d.Base.String())
	}

	if len(d.Partitions) != 0 {
		if buf.Len() > 1 {
			buf.WriteString(" ")
		}
		buf.WriteString("PARTITION BY ")

		for i, p := range d.Partitions {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(p.String())
		}
	}

	if len(d.OrderingTerms) != 0 {
		if buf.Len() > 1 {
			buf.WriteString(" ")
		}
		buf.WriteString("ORDER BY ")

		for i, term := range d.OrderingTerms {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(term.String())
		}
	}

	if d.Frame != nil {
		if buf.Len() > 1 {
			buf.WriteString(" ")
		}
		buf.WriteString(d.Frame.String())
	}

	buf.WriteString(")")

	return buf.String()
}

type PragmaStatement struct {
	Schema *Ident // name of schema (optional)
	Expr   Expr   // can be Ident, Call or BinaryExpr
}

func (s *PragmaStatement) subnodes(yield func(Node) bool) bool {
	return yieldNodes(yield, s.Schema, s.Expr)
}

// String returns the string representation of the pragma statement.
func (s *PragmaStatement) String() string {
	var buf strings.Builder

	buf.WriteString("PRAGMA ")
	if s.Schema != nil {
		buf.WriteString(s.Schema.String())
		buf.WriteString(".")
	}
	buf.WriteString(s.Expr.String())

	return buf.String()
}

type AttachStatement struct {
	Expr   *Ident // database expression (can be a string literal or identifier)
	Schema *Ident // optional schema name
}

func (s *AttachStatement) subnodes(yield func(Node) bool) bool {
	return yieldNodes(yield, s.Expr, s.Schema)
}

func (s *AttachStatement) String() string {
	var buf strings.Builder
	buf.WriteString("ATTACH ")

	buf.WriteString(s.Expr.String())
	if s.Schema != nil {
		buf.WriteString(" AS ")
		buf.WriteString(s.Schema.String())
	}

	return buf.String()
}

type DetachStatement struct {
	Schema *Ident // schema name to detach
}

func (s *DetachStatement) subnodes(yield func(Node) bool) bool {
	return yieldNodes(yield, s.Schema)
}

func (s *DetachStatement) String() string {
	var buf strings.Builder
	buf.WriteString("DETACH ")
	if s.Schema != nil {
		buf.WriteString(s.Schema.String())
	}
	return buf.String()
}

type VacuumStatement struct {
	Schema *Ident // schema name (optional)
	Expr   *Ident // optional expression (can be a string literal or identifier)
}

func (s *VacuumStatement) subnodes(yield func(Node) bool) bool {
	return yieldNodes(yield, s.Schema, s.Expr)
}

func (s *VacuumStatement) String() string {
	var buf strings.Builder
	buf.WriteString("VACUUM")

	if s.Schema != nil {
		buf.WriteString(" ")
		buf.WriteString(s.Schema.String())
	}

	if s.Expr != nil {
		buf.WriteString(" INTO ")
		buf.WriteString(s.Expr.String())
	}

	return buf.String()
}

type ConflictClause struct {
	Rollback bool
	Abort    bool
	Fail     bool
	Ignore   bool
	Replace  bool
}

func (c *ConflictClause) subnodes(yield func(Node) bool) bool {
	return true
}

func (c *ConflictClause) String() string {
	var buf strings.Builder
	buf.WriteString("ON CONFLICT")
	if c.Rollback {
		buf.WriteString(" ROLLBACK")
	} else if c.Abort {
		buf.WriteString(" ABORT")
	} else if c.Fail {
		buf.WriteString(" FAIL")
	} else if c.Ignore {
		buf.WriteString(" IGNORE")
	} else if c.Replace {
		buf.WriteString(" REPLACE")
	} else {
		panic("ConflictClause must have one of ROLLBACK, ABORT, FAIL, IGNORE or REPLACE set")
	}
	return buf.String()
}

type FunctionArg struct {
	Expr          Expr            // expression for the argument
	OrderingTerms []*OrderingTerm // ordering terms (optional)
}

func (a *FunctionArg) subnodes(yield func(Node) bool) bool {
	if !yieldNodes(yield, a.Expr) {
		return false
	}

	for _, term := range a.OrderingTerms {
		if !yieldNodes(yield, term) {
			return false
		}
	}

	return true
}

func (a *FunctionArg) String() string {
	var buf strings.Builder

	buf.WriteString(a.Expr.String())

	if len(a.OrderingTerms) > 0 {
		buf.WriteString(" ORDER BY ")

		for i, term := range a.OrderingTerms {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(term.String())
		}
	}

	return buf.String()
}

type InExpr struct {
	X               Expr             // left-hand side expression
	Op              OpType           // operator type (IN, NOT IN)
	Select          *SelectStatement // optional SELECT statement (if IN is a subquery)
	Values          *ExprList        // list of expressions (if IN list)
	TableOrFunction *QualifiedName   // table or function reference (if IN table/function)
}

func (e *InExpr) subnodes(yield func(Node) bool) bool {
	return yieldNodes(yield, e.X, e.Select, e.Values, e.TableOrFunction)
}

func (e *InExpr) String() string {
	var buf strings.Builder
	buf.WriteString(e.X.String())
	switch e.Op {
	case OP_IN:
		buf.WriteString(" IN ")
	case OP_NOT_IN:
		buf.WriteString(" NOT IN ")
	default:
		panic("invalid operator for InExpr")
	}

	if e.TableOrFunction != nil {
		buf.WriteString(" ")
		buf.WriteString(e.TableOrFunction.String())
	} else if e.Select != nil {
		buf.WriteString(e.Select.String())
	} else if e.Values != nil {
		buf.WriteString(e.Values.String())
	} else {
		panic("InExpr must have either Select, Values or TableOrFunction set")
	}

	return buf.String()
}

type ParenExpr struct {
	Expr Expr
}

func (e *ParenExpr) subnodes(yield func(Node) bool) bool {
	return yieldNodes(yield, e.Expr)
}

func (e *ParenExpr) String() string {
	return fmt.Sprintf("(%s)", e.Expr.String())
}
