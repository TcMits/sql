package sql

import (
	"io"
)

// Parser represents a SQL parser.
type Parser struct {
	s Scanner

	pos  Pos    // current position
	tok  Token  // current token
	lit  string // current literal value
	full bool   // buffer full
}

// ParseStmtString parses s into a single statement.
func ParseStmtString(s string) (Statement, error) {
	p := Parser{s: NewScanner(s)}
	return p.ParseStatement()
}

// ParseMultiStmtString parses s into multiple statements, yielding each
func ParseMultiStmtString(s string, yield func(Statement) error) error {
	p := Parser{s: NewScanner(s)}
	return p.ParseMultiStatements(s, yield)
}

// ParseExprString parses s into an expression. Returns nil if s is blank.
func ParseExprString(s string) (Expr, error) {
	if s == "" {
		return nil, nil
	}
	p := Parser{s: NewScanner(s)}
	return p.ParseExpr()
}

func (p *Parser) ParseMultiStatements(s string, yield func(Statement) error) error {
	for p.peek() != EOF {
		stmt, err := p.ParseStatement()
		if err != nil {
			return err
		}
		if err := yield(stmt); err != nil {
			return err
		}
	}
	return nil
}

func (p *Parser) ParseStatement() (stmt Statement, err error) {
	switch tok := p.peek(); tok {
	case EOF:
		return nil, io.EOF
	case EXPLAIN:
		if stmt, err = p.parseExplainStatement(); err != nil {
			return stmt, err
		}
	default:
		if stmt, err = p.parseNonExplainStatement(); err != nil {
			return stmt, err
		}
	}

	// Read trailing semicolon or end of file.
	if tok := p.peek(); tok != EOF && tok != SEMI {
		return stmt, p.errorExpected(p.pos, p.tok, "semicolon or EOF")
	}
	p.scan()

	return stmt, nil
}

// parseExplain parses EXPLAIN [QUERY PLAN] STMT.
func (p *Parser) parseExplainStatement() (_ *ExplainStatement, err error) {
	// Parse initial "EXPLAIN" token.
	var stmt ExplainStatement
	stmt.Explain = p.scanExpectedTok(EXPLAIN)

	// Parse optional "QUERY PLAN" tokens.
	if p.peek() == QUERY {
		p.scan()

		if p.peek() != PLAN {
			return &stmt, p.errorExpected(p.pos, p.tok, "PLAN")
		}
		stmt.QueryPlan = p.scanExpectedTok(PLAN)
	}

	// Parse statement to be explained.
	if stmt.Stmt, err = p.parseNonExplainStatement(); err != nil {
		return &stmt, err
	}
	return &stmt, nil
}

// parseStmt parses all statement types.
func (p *Parser) parseNonExplainStatement() (Statement, error) {
	switch p.peek() {
	case PRAGMA:
		return p.parsePragmaStatement()
	case ANALYZE:
		return p.parseAnalyzeStatement()
	case REINDEX:
		return p.parseReindexStatement()
	case ALTER:
		return p.parseAlterTableStatement()
	case BEGIN:
		return p.parseBeginStatement()
	case COMMIT, END:
		return p.parseCommitStatement()
	case ROLLBACK:
		return p.parseRollbackStatement()
	case SAVEPOINT:
		return p.parseSavepointStatement()
	case RELEASE:
		return p.parseReleaseStatement()
	case CREATE:
		return p.parseCreateStatement()
	case DROP:
		return p.parseDropStatement()
	case SELECT, VALUES:
		return p.parseSelectStatement(false, nil)
	case INSERT, REPLACE:
		return p.parseInsertStatement(false, nil)
	case UPDATE:
		return p.parseUpdateStatement(false, nil)
	case DELETE:
		return p.parseDeleteStatement(false, nil)
	case WITH:
		return p.parseWithStatement()
	case ATTACH:
		return p.parseAttachStatement()
	case DETACH:
		return p.parseDetachStatement()
	case VACUUM:
		return p.parseVacuumStatement()
	default:
		return nil, p.errorExpected(p.pos, p.tok, "statement")
	}
}

// parseWithStatement is called only from parseNonExplainStatement as we don't
// know what kind of statement we'll have after the CTEs (e.g. SELECT, INSERT, etc).
func (p *Parser) parseWithStatement() (Statement, error) {
	withClause, err := p.parseWithClause()
	if err != nil {
		return nil, err
	}

	switch p.peek() {
	case SELECT, VALUES:
		return p.parseSelectStatement(false, withClause)
	case INSERT, REPLACE:
		return p.parseInsertStatement(false, withClause)
	case UPDATE:
		return p.parseUpdateStatement(false, withClause)
	case DELETE:
		return p.parseDeleteStatement(false, withClause)
	default:
		return nil, p.errorExpected(p.pos, p.tok, "SELECT, VALUES, INSERT, REPLACE, UPDATE, or DELETE")
	}
}

func (p *Parser) parseBeginStatement() (*BeginStatement, error) {
	assert(p.peek() == BEGIN)

	var stmt BeginStatement
	p.scan()

	// Parse transaction type.
	switch p.peek() {
	case DEFERRED:
		stmt.Deferred = p.scanExpectedTok(DEFERRED)
	case IMMEDIATE:
		stmt.Immediate = p.scanExpectedTok(IMMEDIATE)
	case EXCLUSIVE:
		stmt.Exclusive = p.scanExpectedTok(EXCLUSIVE)
	}

	// Parse optional TRANSCTION keyword.
	if p.peek() == TRANSACTION {
		p.scan()
	}
	return &stmt, nil
}

func (p *Parser) parseCommitStatement() (*CommitStatement, error) {
	assert(p.peek() == COMMIT || p.peek() == END)

	var stmt CommitStatement
	p.scan()

	if p.peek() == TRANSACTION {
		p.scan()
	}
	return &stmt, nil
}

func (p *Parser) parseRollbackStatement() (_ *RollbackStatement, err error) {
	assert(p.peek() == ROLLBACK)

	var stmt RollbackStatement
	p.scan()

	// Parse optional "TRANSACTION".
	if p.peek() == TRANSACTION {
		p.scan()
	}

	// Parse optional "TO SAVEPOINT savepoint-name"
	if p.peek() == TO {
		p.scan()
		if p.peek() == SAVEPOINT {
			p.scan()
		}

		if stmt.SavepointName, err = p.parseIdent("savepoint name"); err != nil {
			return &stmt, err
		}
	}
	return &stmt, nil
}

func (p *Parser) parseSavepointStatement() (_ *SavepointStatement, err error) {
	assert(p.peek() == SAVEPOINT)

	var stmt SavepointStatement
	p.scan()
	if stmt.Name, err = p.parseIdent("savepoint name"); err != nil {
		return &stmt, err
	}
	return &stmt, nil
}

func (p *Parser) parseReleaseStatement() (_ *ReleaseStatement, err error) {
	assert(p.peek() == RELEASE)

	var stmt ReleaseStatement
	p.scan()

	if p.peek() == SAVEPOINT {
		p.scan()
	}

	if stmt.Name, err = p.parseIdent("savepoint name"); err != nil {
		return &stmt, err
	}
	return &stmt, nil
}

func (p *Parser) parseCreateStatement() (Statement, error) {
	assert(p.peek() == CREATE)
	pos, tok, _ := p.scan()

	switch p.peek() {
	case TABLE:
		return p.parseCreateTableStatement(false)
	case VIRTUAL:
		return p.parseCreateVirtualTableStatement()
	case VIEW:
		return p.parseCreateViewStatement(false)
	case INDEX, UNIQUE:
		return p.parseCreateIndexStatement()
	case TRIGGER:
		return p.parseCreateTriggerStatement(false)
	case TEMP, TEMPORARY:
		pos, tok, _ := p.scan()

		switch p.peek() {
		case TABLE:
			return p.parseCreateTableStatement(true)
		case VIEW:
			return p.parseCreateViewStatement(true)
		case TRIGGER:
			return p.parseCreateTriggerStatement(true)
		default:
			return nil, p.errorExpected(pos, tok, "TABLE, VIEW, or TRIGGER")
		}
	default:
		return nil, p.errorExpected(pos, tok, "TABLE, VIEW, INDEX, TRIGGER")
	}
}

func (p *Parser) parseDropStatement() (Statement, error) {
	assert(p.peek() == DROP)
	pos, tok, _ := p.scan()

	switch p.peek() {
	case TABLE:
		return p.parseDropTableStatement()
	case VIEW:
		return p.parseDropViewStatement()
	case INDEX:
		return p.parseDropIndexStatement()
	case TRIGGER:
		return p.parseDropTriggerStatement()
	default:
		return nil, p.errorExpected(pos, tok, "TABLE, VIEW, INDEX, or TRIGGER")
	}
}

func (p *Parser) parseCreateTableStatement(temp bool) (_ *CreateTableStatement, err error) {
	assert(p.peek() == TABLE)

	var stmt CreateTableStatement
	stmt.Temp = temp
	p.scan()

	// Parse optional "IF NOT EXISTS".
	if p.peek() == IF {
		p.scan()

		pos, tok, _ := p.scan()
		if tok != NOT {
			return &stmt, p.errorExpected(pos, tok, "NOT")
		}

		pos, tok, _ = p.scan()
		if tok != EXISTS {
			return &stmt, p.errorExpected(pos, tok, "EXISTS")
		}
		stmt.IfNotExists = true
	}

	// Parse the first identifier (either schema or table name)
	stmt.Name, err = p.parseQualifiedName(true, false, false, false, false)
	if err != nil {
		return &stmt, err
	}

	// Parse either a column/constraint list or build table from "AS <select>".
	switch p.peek() {
	case LP:
		p.scan()

		if stmt.Columns, err = p.parseColumnDefinitions(); err != nil {
			return &stmt, err
		} else if stmt.Constraints, err = p.parseTableConstraints(); err != nil {
			return &stmt, err
		}

		if p.peek() != RP {
			return &stmt, p.errorExpected(p.pos, p.tok, "right paren")
		}
		p.scan()

		if c := p.peek(); c == WITHOUT || c == STRICT {
			for {
				switch p.peek() {
				case STRICT:
					stmt.Strict = p.scanExpectedTok(STRICT)
				case WITHOUT:
					p.scan()
					if p.peek() != ROWID {
						return &stmt, p.errorExpected(p.pos, p.tok, "ROWID")
					}
					stmt.WithoutRowID = p.scanExpectedTok(ROWID)

				default:
					return &stmt, p.errorExpected(p.pos, p.tok, "STRICT or WITHOUT ROWID")
				}

				if p.peek() != COMMA {
					break
				}
				p.scan()
			}
		}

		return &stmt, nil
	case AS:
		p.scan()
		if stmt.Select, err = p.parseSelectStatement(false, nil); err != nil {
			return &stmt, err
		}
		return &stmt, nil
	default:
		return &stmt, p.errorExpected(p.pos, p.tok, "AS or left paren")
	}
}

func (p *Parser) parseColumnDefinitions() (_ []*ColumnDefinition, err error) {
	var columns []*ColumnDefinition
	for {
		if tok := p.peek(); isIdentToken(tok) {
			col, err := p.parseColumnDefinition()
			columns = append(columns, col)
			if err != nil {
				return columns, err
			}
			if p.peek() == COMMA {
				p.scan()
			}
		} else if tok == RP || isConstraintStartToken(tok, true) {
			return columns, nil
		} else {
			return columns, p.errorExpected(p.pos, p.tok, "column name, CONSTRAINT, or right paren")
		}
	}
}

func (p *Parser) parseColumnDefinition() (_ *ColumnDefinition, err error) {
	var col ColumnDefinition
	if col.Name, err = p.parseIdent("column name"); err != nil {
		return &col, err
	}

	if tok := p.peek(); tok == IDENT || tok == NULL {
		if col.Type, err = p.parseType(); err != nil {
			return &col, err
		}
	}

	if col.Constraints, err = p.parseColumnConstraints(); err != nil {
		return &col, err
	}
	return &col, nil
}

func (p *Parser) parseTableConstraints() (_ []Constraint, err error) {
	if !isConstraintStartToken(p.peek(), true) {
		return nil, nil
	}

	var a []Constraint
	for {
		cons, err := p.parseConstraint(true)
		if cons != nil {
			a = append(a, cons)
		}
		if err != nil {
			return a, err
		}

		// Scan delimiting comma.
		if p.peek() != COMMA {
			return a, nil
		}
		p.scan()
	}
}

func (p *Parser) parseColumnConstraints() (_ []Constraint, err error) {
	var a []Constraint
	for isConstraintStartToken(p.peek(), false) {
		cons, err := p.parseConstraint(false)
		if cons != nil {
			a = append(a, cons)
		}
		if err != nil {
			return a, err
		}
	}
	return a, nil
}

func (p *Parser) parseConstraint(isTable bool) (_ Constraint, err error) {
	assert(isConstraintStartToken(p.peek(), isTable))

	var name *Ident

	// Parse constraint name, if specified.
	if p.peek() == CONSTRAINT {
		p.scan()

		if name, err = p.parseIdent("constraint name"); err != nil {
			return nil, err
		}
	}

	// Table constraints only use a subset of column constraints.
	if isTable {
		switch p.peek() {
		case PRIMARY:
			return p.parsePrimaryKeyConstraint(name, isTable)
		case UNIQUE:
			return p.parseUniqueConstraint(name, isTable)
		case CHECK:
			return p.parseCheckConstraint(name)
		default:
			assert(p.peek() == FOREIGN)
			return p.parseForeignKeyConstraint(name, isTable)
		}
	}

	// Parse column constraints.
	switch p.peek() {
	case PRIMARY:
		return p.parsePrimaryKeyConstraint(name, isTable)
	case NOT:
		return p.parseNotNullConstraint(name)
	case UNIQUE:
		return p.parseUniqueConstraint(name, isTable)
	case CHECK:
		return p.parseCheckConstraint(name)
	case DEFAULT:
		return p.parseDefaultConstraint(name)
	case GENERATED, AS:
		return p.parseGeneratedConstraint(name)
	case COLLATE:
		return p.parseCollateConstraint(name)
	default:
		assert(p.peek() == REFERENCES)
		return p.parseForeignKeyConstraint(name, isTable)
	}
}

func (p *Parser) parsePrimaryKeyConstraint(name *Ident, isTable bool) (_ *PrimaryKeyConstraint, err error) {
	assert(p.peek() == PRIMARY)

	var cons PrimaryKeyConstraint
	cons.Name = name
	p.scan()

	if p.peek() != KEY {
		return &cons, p.errorExpected(p.pos, p.tok, "KEY")
	}
	p.scan()

	switch p.peek() {
	case ASC:
		cons.Asc = p.scanExpectedTok(ASC)
	case DESC:
		cons.Desc = p.scanExpectedTok(DESC)
	}

	// Table constraints specify columns; column constraints specify sort direction.
	if isTable {
		if p.peek() != LP {
			return &cons, p.errorExpected(p.pos, p.tok, "left paren")
		}
		p.scan()

		for {
			col, err := p.parseIdent("column name")
			if err != nil {
				return &cons, err
			}
			cons.Columns = append(cons.Columns, col)

			if p.peek() == RP {
				break
			} else if p.peek() != COMMA {
				return &cons, p.errorExpected(p.pos, p.tok, "comma or right paren")
			}
			p.scan()
		}
		p.scan()

	}

	if p.peek() == ON {
		cons.Conflict, err = p.parseConflictClause()
		if err != nil {
			return &cons, err
		}
	}

	if !isTable {
		if p.peek() == AUTOINCREMENT {
			cons.Autoincrement = p.scanExpectedTok(AUTOINCREMENT)
		}
	}
	return &cons, nil
}

func (p *Parser) parseNotNullConstraint(name *Ident) (_ *NotNullConstraint, err error) {
	assert(p.peek() == NOT)

	var cons NotNullConstraint
	cons.Name = name
	p.scan()

	if p.peek() != NULL {
		return &cons, p.errorExpected(p.pos, p.tok, "NULL")
	}
	p.scan()

	if p.peek() == ON {
		cons.Conflict, err = p.parseConflictClause()
		if err != nil {
			return &cons, err
		}
	}

	return &cons, nil
}

func (p *Parser) parseUniqueConstraint(name *Ident, isTable bool) (_ *UniqueConstraint, err error) {
	assert(p.peek() == UNIQUE)

	var cons UniqueConstraint
	cons.Name = name
	p.scan()

	if isTable {
		if p.peek() != LP {
			return &cons, p.errorExpected(p.pos, p.tok, "left paren")
		}
		p.scan()

		for {
			col, err := p.parseIndexedColumn()
			if err != nil {
				return &cons, err
			}
			cons.Columns = append(cons.Columns, col)

			if p.peek() == RP {
				break
			} else if p.peek() != COMMA {
				return &cons, p.errorExpected(p.pos, p.tok, "comma or right paren")
			}
			p.scan()
		}
		p.scan()
	}

	if p.peek() == ON {
		cons.Conflict, err = p.parseConflictClause()
		if err != nil {
			return &cons, err
		}
	}

	return &cons, nil
}

func (p *Parser) parseCheckConstraint(name *Ident) (_ *CheckConstraint, err error) {
	assert(p.peek() == CHECK)

	var cons CheckConstraint
	cons.Name = name
	p.scan()

	if p.peek() != LP {
		return &cons, p.errorExpected(p.pos, p.tok, "left paren")
	}
	p.scan()

	if cons.Expr, err = p.ParseExpr(); err != nil {
		return &cons, err
	}

	if p.peek() != RP {
		return &cons, p.errorExpected(p.pos, p.tok, "right paren")
	}
	p.scan()

	return &cons, nil
}

func (p *Parser) parseDefaultConstraint(name *Ident) (_ *DefaultConstraint, err error) {
	assert(p.peek() == DEFAULT)

	var cons DefaultConstraint
	cons.Name = name
	p.scan()

	// This parses a double-quoted identifier as a string value even though
	// SQLite docs say that it shouldn't if DQS is disabled. For that reason,
	// we are including it only on the DEFAULT value parsing.
	//
	// See: https://github.com/rqlite/sql/issues/18
	if p.peek() == QIDENT {
		_, _, lit := p.scan()
		cons.Expr = &StringLit{Value: lit}
	} else if isLiteralToken(p.peek()) {
		cons.Expr = p.mustParseLiteral()
	} else if p.peek() == PLUS || p.peek() == MINUS {
		if cons.Expr, err = p.parseSignedNumber("signed number"); err != nil {
			return &cons, err
		}
	} else {
		if p.peek() != LP {
			return &cons, p.errorExpected(p.pos, p.tok, "literal value or left paren")
		}
		p.scan()

		if cons.Expr, err = p.ParseExpr(); err != nil {
			return &cons, err
		}

		if p.peek() != RP {
			return &cons, p.errorExpected(p.pos, p.tok, "right paren")
		}
		p.scan()
	}
	return &cons, nil
}

func (p *Parser) parseGeneratedConstraint(name *Ident) (_ *GeneratedConstraint, err error) {
	assert(p.peek() == GENERATED || p.peek() == AS)

	var cons GeneratedConstraint
	cons.Name = name

	if p.peek() == GENERATED {
		p.scan()

		if p.peek() != ALWAYS {
			return &cons, p.errorExpected(p.pos, p.tok, "ALWAYS")
		}
		p.scan()
	}

	if p.peek() != AS {
		return &cons, p.errorExpected(p.pos, p.tok, "AS")
	}
	p.scan()

	if p.peek() != LP {
		return &cons, p.errorExpected(p.pos, p.tok, "left paren")
	}
	p.scan()

	if cons.Expr, err = p.ParseExpr(); err != nil {
		return &cons, err
	}

	if p.peek() != RP {
		return &cons, p.errorExpected(p.pos, p.tok, "right paren")
	}
	p.scan()

	switch p.peek() {
	case STORED:
		cons.Stored = p.scanExpectedTok(STORED)
	case VIRTUAL:
		cons.Virtual = p.scanExpectedTok(VIRTUAL)
	}

	return &cons, nil
}

func (p *Parser) parseCollateConstraint(name *Ident) (_ *CollateConstraint, err error) {
	assert(p.peek() == COLLATE)

	var cons CollateConstraint
	cons.Name = name

	if p.peek() != COLLATE {
		return &cons, p.errorExpected(p.pos, p.tok, "COLLATE")
	}
	p.scan()

	collation, err := p.parseIdent("collation name")
	if err != nil {
		return &cons, err
	}
	cons.Collation = collation

	return &cons, nil
}

func (p *Parser) parseForeignKeyConstraint(name *Ident, isTable bool) (_ *ForeignKeyConstraint, err error) {
	var cons ForeignKeyConstraint
	cons.Name = name

	// Table constraints start with "FOREIGN KEY (col1, col2, etc)".
	if isTable {
		assert(p.peek() == FOREIGN)
		p.scan()

		if p.peek() != KEY {
			return &cons, p.errorExpected(p.pos, p.tok, "KEY")
		}
		p.scan()

		if p.peek() != LP {
			return &cons, p.errorExpected(p.pos, p.tok, "left paren")
		}
		p.scan()

		for {
			col, err := p.parseIdent("column name")
			if err != nil {
				return &cons, err
			}
			cons.Columns = append(cons.Columns, col)

			if p.peek() == RP {
				break
			} else if p.peek() != COMMA {
				return &cons, p.errorExpected(p.pos, p.tok, "comma or right paren")
			}
			p.scan()
		}
		p.scan()
	}

	if p.peek() != REFERENCES {
		return &cons, p.errorExpected(p.pos, p.tok, "REFERENCES")
	}
	p.scan()

	if cons.ForeignTable, err = p.parseIdent("foreign table name"); err != nil {
		return &cons, err
	}

	// Parse column list.
	if p.peek() == LP {
		p.scan()

		for {
			col, err := p.parseIdent("foreign column name")
			if err != nil {
				return &cons, err
			}
			cons.ForeignColumns = append(cons.ForeignColumns, col)

			if p.peek() == RP {
				break
			} else if p.peek() != COMMA {
				return &cons, p.errorExpected(p.pos, p.tok, "comma or right paren")
			}
			p.scan()
		}

		p.scan()
	}

	// Parse foreign key args.
	for p.peek() == ON {
		var arg ForeignKeyArg
		p.scan()

		// Parse foreign key type.
		if p.peek() == UPDATE {
			arg.OnUpdate = p.scanExpectedTok(UPDATE)
		} else if p.peek() == DELETE {
			arg.OnDelete = p.scanExpectedTok(DELETE)
		} else {
			return &cons, p.errorExpected(p.pos, p.tok, "UPDATE or DELETE")
		}

		// Parse foreign key action.
		if p.peek() == SET {
			p.scan()
			if p.peek() == NULL {
				arg.SetNull = p.scanExpectedTok(NULL)
			} else if p.peek() == DEFAULT {
				arg.SetDefault = p.scanExpectedTok(DEFAULT)
			} else {
				return &cons, p.errorExpected(p.pos, p.tok, "NULL or DEFAULT")
			}
		} else if p.peek() == CASCADE {
			arg.Cascade = p.scanExpectedTok(CASCADE)
		} else if p.peek() == RESTRICT {
			arg.Restrict = p.scanExpectedTok(RESTRICT)
		} else if p.peek() == NO {
			p.scan()
			if p.peek() == ACTION {
				arg.NoAction = p.scanExpectedTok(ACTION)
			} else {
				return &cons, p.errorExpected(p.pos, p.tok, "ACTION")
			}
		} else {
			return &cons, p.errorExpected(p.pos, p.tok, "SET NULL, SET DEFAULT, CASCADE, RESTRICT, or NO ACTION")
		}

		cons.Args = append(cons.Args, &arg)
	}

	// Parse deferrable subclause.
	if p.peek() == NOT || p.peek() == DEFERRABLE {
		if p.peek() == NOT {
			p.scan()
			if p.peek() != DEFERRABLE {
				return &cons, p.errorExpected(p.pos, p.tok, "DEFERRABLE")
			}
			cons.NotDeferrable = p.scanExpectedTok(DEFERRABLE)
		} else {
			cons.Deferrable = p.scanExpectedTok(DEFERRABLE)
		}

		if p.peek() == INITIALLY {
			p.scan()
			if p.peek() == DEFERRED {
				cons.InitiallyDeferred = p.scanExpectedTok(DEFERRED)
			} else if p.peek() == IMMEDIATE {
				cons.InitiallyImmediate = p.scanExpectedTok(IMMEDIATE)
			}
		}
	}

	return &cons, nil
}

func (p *Parser) parseCreateVirtualTableStatement() (_ *CreateVirtualTableStatement, err error) {
	assert(p.peek() == VIRTUAL)

	var stmt CreateVirtualTableStatement
	p.scan()
	p.scan()

	// Parse optional "IF NOT EXISTS".
	if p.peek() == IF {
		p.scan()

		pos, tok, _ := p.scan()
		if tok != NOT {
			return &stmt, p.errorExpected(pos, tok, "NOT")
		}

		pos, tok, _ = p.scan()
		if tok != EXISTS {
			return &stmt, p.errorExpected(pos, tok, "EXISTS")
		}
		stmt.IfNotExists = true
	}

	stmt.Name, err = p.parseQualifiedName(true, false, false, false, false)
	if err != nil {
		return &stmt, err
	}

	pos, tok, _ := p.scan()
	if tok != USING {
		return &stmt, p.errorExpected(pos, tok, "USING")
	}

	if stmt.ModuleName, err = p.parseIdent("module name"); err != nil {
		return &stmt, err
	}
	// Module arguments can be optional
	if p.peek() != LP {
		return &stmt, nil
	}

	if stmt.Arguments, err = p.parseModuleArguments(); err != nil {
		return &stmt, err
	}

	return &stmt, nil
}

func (p *Parser) parseModuleArguments() (_ []*ModuleArgument, err error) {
	assert(p.peek() == LP)

	var args []*ModuleArgument
	p.scan()

	for {
		arg, err := p.parseModuleArgument()
		if err != nil {
			return args, err
		}
		args = append(args, arg)

		if c := p.peek(); c == RP {
			break
		} else if c == COMMA {
			p.scan()
		} else {
			return args, p.errorExpected(p.pos, p.tok, "comma or right paren")
		}
	}

	p.scan()

	return args, nil
}

func (p *Parser) parseModuleArgument() (_ *ModuleArgument, err error) {
	var arg ModuleArgument

	if tok := p.peek(); isIdentToken(tok) {
		if arg.Name, err = p.parseIdent("module argument name"); err != nil {
			return &arg, err
		}
	} else if isLiteralToken(tok) { // arg name allow literals
		_, _, lit := p.scan()
		arg.Name = &Ident{Name: lit}
	} else if keywordOrIdent(p.lit) != IDENT { // arg name allow keywords
		_, _, lit := p.scan()
		arg.Name = &Ident{Name: lit}
	} else {
		return &arg, p.errorExpected(p.pos, p.tok, "module argument name")
	}

	if p.peek() == EQ {
		// Parse literal
		p.scan()
		if arg.Literal, err = p.parseOperand(); err != nil {
			return &arg, err
		}
	} else if isTypeName(p.lit) {
		if arg.Type, err = p.parseType(); err != nil {
			return &arg, err
		}
	}

	return &arg, nil
}

func (p *Parser) parseDropTableStatement() (_ *DropTableStatement, err error) {
	assert(p.peek() == TABLE)

	var stmt DropTableStatement
	p.scan()

	// Parse optional "IF EXISTS".
	if p.peek() == IF {
		p.scan()
		if p.peek() != EXISTS {
			return &stmt, p.errorExpected(p.pos, p.tok, "EXISTS")
		}
		stmt.IfExists = p.scanExpectedTok(EXISTS)
	}

	stmt.Name, err = p.parseQualifiedName(true, false, false, false, false)
	if err != nil {
		return &stmt, err
	}

	return &stmt, nil
}

func (p *Parser) parseCreateViewStatement(temp bool) (_ *CreateViewStatement, err error) {
	assert(p.peek() == VIEW)

	var stmt CreateViewStatement
	stmt.Temp = temp
	p.scan()

	// Parse optional "IF NOT EXISTS".
	if p.peek() == IF {
		p.scan()

		if p.peek() != NOT {
			return &stmt, p.errorExpected(p.pos, p.tok, "NOT")
		}
		p.scan()

		if p.peek() != EXISTS {
			return &stmt, p.errorExpected(p.pos, p.tok, "EXISTS")
		}
		stmt.IfNotExists = p.scanExpectedTok(EXISTS)
	}

	stmt.Name, err = p.parseQualifiedName(true, false, false, false, false)
	if err != nil {
		return &stmt, err
	}

	// Parse optional column list.
	if p.peek() == LP {
		p.scan()
		for {
			col, err := p.parseIdent("column name")
			if err != nil {
				return &stmt, err
			}
			stmt.Columns = append(stmt.Columns, col)

			if p.peek() == RP {
				break
			} else if p.peek() != COMMA {
				return &stmt, p.errorExpected(p.pos, p.tok, "comma or right paren")
			}
			p.scan()
		}
		p.scan()
	}

	// Parse "AS select-stmt"
	if p.peek() != AS {
		return &stmt, p.errorExpected(p.pos, p.tok, "AS")
	}
	p.scan()
	if stmt.Select, err = p.parseSelectStatement(false, nil); err != nil {
		return &stmt, err
	}
	return &stmt, nil
}

func (p *Parser) parseDropViewStatement() (_ *DropViewStatement, err error) {
	assert(p.peek() == VIEW)

	var stmt DropViewStatement
	p.scan()

	// Parse optional "IF EXISTS".
	if p.peek() == IF {
		p.scan()
		if p.peek() != EXISTS {
			return &stmt, p.errorExpected(p.pos, p.tok, "EXISTS")
		}
		stmt.IfExists = p.scanExpectedTok(EXISTS)
	}

	stmt.Name, err = p.parseQualifiedName(true, false, false, false, false)
	if err != nil {
		return &stmt, err
	}

	return &stmt, nil
}

func (p *Parser) parseCreateIndexStatement() (_ *CreateIndexStatement, err error) {
	assert(p.peek() == INDEX || p.peek() == UNIQUE)

	var stmt CreateIndexStatement
	if p.peek() == UNIQUE {
		stmt.Unique = p.scanExpectedTok(UNIQUE)
	}

	if p.peek() != INDEX {
		return &stmt, p.errorExpected(p.pos, p.tok, "INDEX")
	}
	p.scan()

	// Parse optional "IF NOT EXISTS".
	if p.peek() == IF {
		p.scan()

		if p.peek() != NOT {
			return &stmt, p.errorExpected(p.pos, p.tok, "NOT")
		}
		p.scan()

		if p.peek() != EXISTS {
			return &stmt, p.errorExpected(p.pos, p.tok, "EXISTS")
		}
		stmt.IfNotExists = p.scanExpectedTok(EXISTS)
	}

	stmt.Name, err = p.parseQualifiedName(true, false, false, false, false)
	if err != nil {
		return &stmt, err
	}

	if p.peek() != ON {
		return &stmt, p.errorExpected(p.pos, p.tok, "ON")
	}
	p.scan()

	if stmt.Table, err = p.parseIdent("table name"); err != nil {
		return &stmt, err
	}

	// Parse optional column list.
	if p.peek() != LP {
		return &stmt, p.errorExpected(p.pos, p.tok, "left paren")
	}
	p.scan()

	for {
		col, err := p.parseIndexedColumn()
		if err != nil {
			return &stmt, err
		}
		stmt.Columns = append(stmt.Columns, col)

		if p.peek() == RP {
			break
		} else if p.peek() != COMMA {
			return &stmt, p.errorExpected(p.pos, p.tok, "comma or right paren")
		}
		p.scan()
	}

	p.scan()

	// Parse optional "WHERE expr"
	if p.peek() == WHERE {
		p.scan()
		if stmt.WhereExpr, err = p.ParseExpr(); err != nil {
			return &stmt, err
		}
	}
	return &stmt, nil
}

func (p *Parser) parseDropIndexStatement() (_ *DropIndexStatement, err error) {
	assert(p.peek() == INDEX)

	var stmt DropIndexStatement
	p.scan()

	// Parse optional "IF EXISTS".
	if p.peek() == IF {
		p.scan()
		if p.peek() != EXISTS {
			return &stmt, p.errorExpected(p.pos, p.tok, "EXISTS")
		}
		stmt.IfExists = p.scanExpectedTok(EXISTS)
	}

	stmt.Name, err = p.parseQualifiedName(true, false, false, false, false)
	if err != nil {
		return &stmt, err
	}

	return &stmt, nil
}

func (p *Parser) parseCreateTriggerStatement(temp bool) (_ *CreateTriggerStatement, err error) {
	assert(p.peek() == TRIGGER)

	var stmt CreateTriggerStatement
	stmt.Temp = temp
	p.scan()

	// Parse optional "IF NOT EXISTS".
	if p.peek() == IF {
		p.scan()

		if p.peek() != NOT {
			return &stmt, p.errorExpected(p.pos, p.tok, "NOT")
		}
		p.scan()

		if p.peek() != EXISTS {
			return &stmt, p.errorExpected(p.pos, p.tok, "EXISTS")
		}
		stmt.IfNotExists = p.scanExpectedTok(EXISTS)
	}

	stmt.Name, err = p.parseQualifiedName(true, false, false, false, false)
	if err != nil {
		return &stmt, err
	}

	// Parse BEFORE, AFTER, or INSTEAD OF
	switch p.peek() {
	case BEFORE:
		stmt.Before = p.scanExpectedTok(BEFORE)
	case AFTER:
		stmt.After = p.scanExpectedTok(AFTER)
	case INSTEAD:
		p.scan()
		if p.peek() != OF {
			return &stmt, p.errorExpected(p.pos, p.tok, "OF")
		}
		stmt.InsteadOf = p.scanExpectedTok(OF)
	}

	// Parse DELETE, INSERT, UPDATE, or UPDATE OF [columns]
	switch p.peek() {
	case DELETE:
		stmt.Delete = p.scanExpectedTok(DELETE)
	case INSERT:
		stmt.Insert = p.scanExpectedTok(INSERT)
	case UPDATE:
		stmt.Update = p.scanExpectedTok(UPDATE)
		if p.peek() == OF {
			p.scan()
			for {
				col, err := p.parseIdent("column name")
				if err != nil {
					return &stmt, err
				}
				stmt.UpdateOfColumns = append(stmt.UpdateOfColumns, col)

				if p.peek() != COMMA {
					break
				}
				p.scan()
			}
		}
	default:
		return &stmt, p.errorExpected(p.pos, p.tok, "DELETE, INSERT, or UPDATE")
	}

	// Parse "ON table-name".
	if p.peek() != ON {
		return &stmt, p.errorExpected(p.pos, p.tok, "ON")
	}
	p.scan()
	if stmt.Table, err = p.parseIdent("table name"); err != nil {
		return &stmt, err
	}

	// Parse optional "FOR EACH ROW".
	if p.peek() == FOR {
		p.scan()
		if p.peek() != EACH {
			return &stmt, p.errorExpected(p.pos, p.tok, "EACH")
		}
		p.scan()
		if p.peek() != ROW {
			return &stmt, p.errorExpected(p.pos, p.tok, "ROW")
		}
		stmt.ForEachRow = p.scanExpectedTok(ROW)
	}

	// Parse optional "WHEN expr".
	if p.peek() == WHEN {
		p.scan()
		if stmt.WhenExpr, err = p.ParseExpr(); err != nil {
			return &stmt, err
		}
	}

	// Parse trigger body.
	if p.peek() != BEGIN {
		return &stmt, p.errorExpected(p.pos, p.tok, "BEGIN")
	}
	p.scan()

	for {
		s, err := p.parseTriggerBodyStatement()
		if err != nil {
			return &stmt, err
		}
		stmt.Body = append(stmt.Body, s)

		if p.peek() == END {
			break
		}
	}
	p.scan()

	return &stmt, nil
}

func (p *Parser) parseTriggerBodyStatement() (stmt Statement, err error) {
	switch p.peek() {
	case SELECT, VALUES:
		stmt, err = p.parseSelectStatement(false, nil)
	case INSERT, REPLACE:
		stmt, err = p.parseInsertStatement(true, nil)
	case UPDATE:
		stmt, err = p.parseUpdateStatement(true, nil)
	case DELETE:
		stmt, err = p.parseDeleteStatement(true, nil)
	case WITH:
		stmt, err = p.parseWithStatement()
	default:
		return nil, p.errorExpected(p.pos, p.tok, "statement")
	}
	if err != nil {
		return stmt, err
	}

	// Ensure trailing semicolon exists.
	if p.peek() != SEMI {
		return stmt, p.errorExpected(p.pos, p.tok, "semicolon")
	}
	p.scan()

	return stmt, nil
}

func (p *Parser) parseDropTriggerStatement() (_ *DropTriggerStatement, err error) {
	assert(p.peek() == TRIGGER)

	var stmt DropTriggerStatement
	p.scan()

	// Parse optional "IF EXISTS".
	if p.peek() == IF {
		p.scan()
		if p.peek() != EXISTS {
			return &stmt, p.errorExpected(p.pos, p.tok, "EXISTS")
		}
		stmt.IfExists = p.scanExpectedTok(EXISTS)
	}

	stmt.Name, err = p.parseQualifiedName(true, false, false, false, false)
	if err != nil {
		return &stmt, err
	}

	return &stmt, nil
}

func (p *Parser) parseIdent(desc string) (*Ident, error) {
	pos, tok, lit := p.scan()
	switch tok {
	case IDENT, QIDENT:
		return &Ident{Name: lit, Quoted: tok == QIDENT}, nil
	case NULL:
		return &Ident{Name: lit}, nil
	case STRING:
		return &Ident{Name: lit, Quoted: true}, nil
	default:
		if isBareToken(tok) {
			return &Ident{Name: lit}, nil
		}
		return nil, p.errorExpected(pos, tok, desc)
	}
}

func (p *Parser) parseType() (_ *Type, err error) {
	var typ Type
	for {
		tok := p.peek()
		if tok != IDENT && tok != NULL {
			break
		}
		typeName, err := p.parseIdent("type name")
		if err != nil {
			return &typ, err
		}
		if typ.Name == nil {
			typ.Name = typeName
		} else {
			typ.Name.Name += " " + typeName.Name
		}
	}

	if typ.Name == nil {
		return &typ, p.errorExpected(p.pos, p.tok, "type name")
	}

	// Optionally parse precision & scale.
	if p.peek() == LP {
		p.scan()
		if typ.Precision, err = p.parseSignedNumber("precision"); err != nil {
			return &typ, err
		}

		if p.peek() == COMMA {
			p.scan()
			if typ.Scale, err = p.parseSignedNumber("scale"); err != nil {
				return &typ, err
			}
		}

		if p.peek() != RP {
			return nil, p.errorExpected(p.pos, p.tok, "right paren")
		}
		p.scan()
	}

	return &typ, nil
}

func (p *Parser) parseInsertStatement(inTrigger bool, withClause *WithClause) (_ *InsertStatement, err error) {
	assert(p.peek() == INSERT || p.peek() == REPLACE)

	var stmt InsertStatement
	stmt.WithClause = withClause

	if p.peek() == INSERT {
		p.scan()

		if p.peek() == OR {
			p.scan()

			switch p.peek() {
			case ROLLBACK:
				stmt.InsertOrRollback = p.scanExpectedTok(ROLLBACK)
			case REPLACE:
				stmt.InsertOrReplace = p.scanExpectedTok(REPLACE)
			case ABORT:
				stmt.InsertOrAbort = p.scanExpectedTok(ABORT)
			case FAIL:
				stmt.InsertOrFail = p.scanExpectedTok(FAIL)
			case IGNORE:
				stmt.InsertOrIgnore = p.scanExpectedTok(IGNORE)
			default:
				return &stmt, p.errorExpected(p.pos, p.tok, "ROLLBACK, REPLACE, ABORT, FAIL, or IGNORE")
			}
		}
	} else {
		stmt.Replace = p.scanExpectedTok(REPLACE)
	}

	if p.peek() != INTO {
		return &stmt, p.errorExpected(p.pos, p.tok, "INTO")
	}
	p.scan()

	stmt.Table, err = p.parseQualifiedName(!inTrigger, !inTrigger, false, false, false)
	if err != nil {
		return &stmt, err
	}

	// Parse optional column list.
	if p.peek() == LP {
		p.scan()
		for {
			col, err := p.parseIdent("column name")
			if err != nil {
				return &stmt, err
			}
			stmt.Columns = append(stmt.Columns, col)

			if p.peek() == RP {
				break
			} else if p.peek() != COMMA {
				return &stmt, p.errorExpected(p.pos, p.tok, "comma or right paren")
			}
			p.scan()
		}
		p.scan()
	}

	switch p.peek() {
	case VALUES:
		p.scan()
		for {
			var list ExprList
			if p.peek() != LP {
				return &stmt, p.errorExpected(p.pos, p.tok, "left paren")
			}
			p.scan()

			for {
				expr, err := p.ParseExpr()
				if err != nil {
					return &stmt, err
				}
				list.Exprs = append(list.Exprs, expr)

				if p.peek() == RP {
					break
				} else if p.peek() != COMMA {
					return &stmt, p.errorExpected(p.pos, p.tok, "comma or right paren")
				}
				p.scan()
			}
			p.scan()
			stmt.ValueLists = append(stmt.ValueLists, &list)

			if p.peek() != COMMA {
				break
			}
			p.scan()
		}
	case SELECT:
		if stmt.Select, err = p.parseSelectStatement(false, nil); err != nil {
			return &stmt, err
		}
	case DEFAULT:
		if inTrigger {
			return &stmt, p.errorExpected(p.pos, p.tok, "non-DEFAULT VALUES")
		}

		p.scan()
		if p.peek() != VALUES {
			return &stmt, p.errorExpected(p.pos, p.tok, "VALUES")
		}
		stmt.DefaultValues = p.scanExpectedTok(VALUES)
	default:
		return &stmt, p.errorExpected(p.pos, p.tok, "VALUES, SELECT, or DEFAULT VALUES")
	}

	// Parse optional upsert clause.
	if p.peek() == ON {
		if stmt.UpsertClause, err = p.parseUpsertClause(); err != nil {
			return &stmt, err
		}
	}

	// Parse optional RETURNING clause.
	if p.peek() == RETURNING {
		if stmt.ReturningColumns, err = p.parseReturningClause(); err != nil {
			return &stmt, err
		}
	}

	return &stmt, nil
}

func (p *Parser) parseUpsertClause() (_ *UpsertClause, err error) {
	assert(p.peek() == ON)

	var clause UpsertClause

	// Parse "ON CONFLICT"
	p.scan()
	if p.peek() != CONFLICT {
		return &clause, p.errorExpected(p.pos, p.tok, "CONFLICT")
	}
	p.scan()

	// Parse optional indexed column list & WHERE conditional.
	if p.peek() == LP {
		p.scan()
		for {
			col, err := p.parseIndexedColumn()
			if err != nil {
				return &clause, err
			}
			clause.Columns = append(clause.Columns, col)

			if p.peek() == RP {
				break
			} else if p.peek() != COMMA {
				return &clause, p.errorExpected(p.pos, p.tok, "comma or right paren")
			}
			p.scan()
		}
		p.scan()

		if p.peek() == WHERE {
			p.scan()
			if clause.WhereExpr, err = p.ParseExpr(); err != nil {
				return &clause, err
			}
		}
	}

	// Parse "DO NOTHING" or "DO UPDATE SET".
	if p.peek() != DO {
		return &clause, p.errorExpected(p.pos, p.tok, "DO")
	}
	p.scan()

	// If next token is NOTHING, then read it and exit immediately.
	if p.peek() == NOTHING {
		clause.DoNothing = p.scanExpectedTok(NOTHING)
		return &clause, nil
	} else if p.peek() != UPDATE {
		return &clause, p.errorExpected(p.pos, p.tok, "NOTHING or UPDATE SET")
	}

	// Otherwise parse "UPDATE SET"
	p.scan()
	if p.peek() != SET {
		return &clause, p.errorExpected(p.pos, p.tok, "SET")
	}
	clause.DoUpdateSet = p.scanExpectedTok(SET)

	// Parse list of assignments.
	for {
		assignment, err := p.parseAssignment()
		if err != nil {
			return &clause, err
		}
		clause.Assignments = append(clause.Assignments, assignment)

		if p.peek() != COMMA {
			break
		}
		p.scan()
	}

	// Parse WHERE after DO UPDATE SET.
	if p.peek() == WHERE {
		p.scan()
		if clause.UpdateWhereExpr, err = p.ParseExpr(); err != nil {
			return &clause, err
		}
	}

	return &clause, nil
}

func (p *Parser) parseReturningClause() (_ []*ResultColumn, err error) {
	assert(p.peek() == RETURNING)

	var clause []*ResultColumn

	p.scan()
	// Parse result columns.
	for {
		col, err := p.parseResultColumn()
		if err != nil {
			return clause, err
		}
		clause = append(clause, col)

		if p.peek() != COMMA {
			break
		}
		p.scan()
	}

	if len(clause) == 0 {
		return clause, p.errorExpected(p.pos, p.tok, "result column")
	}

	return clause, nil
}

func (p *Parser) parseIndexedColumn() (_ *IndexedColumn, err error) {
	var col IndexedColumn
	if col.X, err = p.ParseExpr(); err != nil { // make sure not to parse COLLATE as an operator
		return &col, err
	}

	switch p.peek() {
	case ASC:
		col.Asc = p.scanExpectedTok(ASC)
	case DESC:
		col.Desc = p.scanExpectedTok(DESC)
	}

	return &col, nil
}

func (p *Parser) parseUpdateStatement(inTrigger bool, withClause *WithClause) (_ *UpdateStatement, err error) {
	assert(p.peek() == UPDATE)

	var stmt UpdateStatement
	stmt.WithClause = withClause

	p.scan()
	if p.peek() == OR {
		p.scan()

		switch p.peek() {
		case ROLLBACK:
			stmt.UpdateOrRollback = p.scanExpectedTok(ROLLBACK)
		case REPLACE:
			stmt.UpdateOrReplace = p.scanExpectedTok(REPLACE)
		case ABORT:
			stmt.UpdateOrAbort = p.scanExpectedTok(ABORT)
		case FAIL:
			stmt.UpdateOrFail = p.scanExpectedTok(FAIL)
		case IGNORE:
			stmt.UpdateOrIgnore = p.scanExpectedTok(IGNORE)
		default:
			return &stmt, p.errorExpected(p.pos, p.tok, "ROLLBACK, REPLACE, ABORT, FAIL, or IGNORE")
		}
	}

	if stmt.Table, err = p.parseQualifiedName(!inTrigger, !inTrigger, !inTrigger, false, false); err != nil {
		return &stmt, err
	}

	// Parse SET + list of assignments.
	if p.peek() != SET {
		return &stmt, p.errorExpected(p.pos, p.tok, "SET")
	}
	p.scan()

	for {
		assignment, err := p.parseAssignment()
		if err != nil {
			return &stmt, err
		}
		stmt.Assignments = append(stmt.Assignments, assignment)

		if p.peek() != COMMA {
			break
		}
		p.scan()
	}

	// Parse WHERE clause.
	if p.peek() == WHERE {
		p.scan()
		if stmt.WhereExpr, err = p.ParseExpr(); err != nil {
			return &stmt, err
		}
	}

	// Parse optional RETURNING clause.
	if p.peek() == RETURNING {
		if stmt.ReturningColumns, err = p.parseReturningClause(); err != nil {
			return &stmt, err
		}
	}

	// Parse ORDER BY clause. This differs from the SELECT parsing in that
	// if an ORDER BY is specified then the LIMIT is required.
	if p.peek() == ORDER || p.peek() == LIMIT {
		if inTrigger {
			return &stmt, p.errorExpected(p.pos, p.tok, "ORDER BY or LIMIT in a UPDATE statement")
		}

		if p.peek() == ORDER {
			p.scan()
			if p.peek() != BY {
				return &stmt, p.errorExpected(p.pos, p.tok, "BY")
			}
			p.scan()

			for {
				term, err := p.parseOrderingTerm()
				if err != nil {
					return &stmt, err
				}
				stmt.OrderingTerms = append(stmt.OrderingTerms, term)

				if p.peek() != COMMA {
					break
				}
				p.scan()
			}
		}

		// Parse LIMIT/OFFSET clause.
		if p.peek() != LIMIT {
			return &stmt, p.errorExpected(p.pos, p.tok, "LIMIT")
		}
		p.scan()
		if stmt.LimitExpr, err = p.ParseExpr(); err != nil {
			return &stmt, err
		}

		if tok := p.peek(); tok == OFFSET || tok == COMMA {
			if tok == OFFSET {
				p.scan()
			} else {
				p.scan()
			}
			if stmt.OffsetExpr, err = p.ParseExpr(); err != nil {
				return &stmt, err
			}
		}
	}

	return &stmt, nil
}

func (p *Parser) parseDeleteStatement(inTrigger bool, withClause *WithClause) (_ *DeleteStatement, err error) {
	assert(p.peek() == DELETE)

	var stmt DeleteStatement
	stmt.WithClause = withClause

	// Parse "DELETE FROM tbl"
	p.scan()
	if p.peek() != FROM {
		return &stmt, p.errorExpected(p.pos, p.tok, "FROM")
	}

	p.scan()
	if stmt.Table, err = p.parseQualifiedName(!inTrigger, !inTrigger, !inTrigger, false, false); err != nil {
		return &stmt, err
	}

	// Parse WHERE clause.
	if p.peek() == WHERE {
		p.scan()
		if stmt.WhereExpr, err = p.ParseExpr(); err != nil {
			return &stmt, err
		}
	}

	// Parse optional RETURNING clause.
	if p.peek() == RETURNING {
		if stmt.ReturningColumns, err = p.parseReturningClause(); err != nil {
			return &stmt, err
		}
	}

	// Parse ORDER BY clause. This differs from the SELECT parsing in that
	// if an ORDER BY is specified then the LIMIT is required.
	if p.peek() == ORDER || p.peek() == LIMIT {
		if inTrigger {
			return &stmt, p.errorExpected(p.pos, p.tok, "ORDER BY or LIMIT in a DELETE statement")
		}

		if p.peek() == ORDER {
			p.scan()
			if p.peek() != BY {
				return &stmt, p.errorExpected(p.pos, p.tok, "BY")
			}
			p.scan()

			for {
				term, err := p.parseOrderingTerm()
				if err != nil {
					return &stmt, err
				}
				stmt.OrderingTerms = append(stmt.OrderingTerms, term)

				if p.peek() != COMMA {
					break
				}
				p.scan()
			}
		}

		// Parse LIMIT/OFFSET clause.
		if p.peek() != LIMIT {
			return &stmt, p.errorExpected(p.pos, p.tok, "LIMIT")
		}
		p.scan()
		if stmt.LimitExpr, err = p.ParseExpr(); err != nil {
			return &stmt, err
		}

		if tok := p.peek(); tok == OFFSET || tok == COMMA {
			if tok == OFFSET {
				p.scan()
			} else {
				p.scan()
			}
			if stmt.OffsetExpr, err = p.ParseExpr(); err != nil {
				return &stmt, err
			}
		}
	}

	return &stmt, nil
}

func (p *Parser) parseAssignment() (_ *Assignment, err error) {
	var assignment Assignment

	// Parse either a single column (IDENT) or a column list (LP IDENT COMMA IDENT RP)
	if isIdentToken(p.peek()) {
		col, _ := p.parseIdent("column name")
		assignment.Columns = []*Ident{col}
	} else if p.peek() == LP {
		p.scan()
		for {
			col, err := p.parseIdent("column name")
			if err != nil {
				return &assignment, err
			}
			assignment.Columns = append(assignment.Columns, col)

			if p.peek() == RP {
				break
			} else if p.peek() != COMMA {
				return &assignment, p.errorExpected(p.pos, p.tok, "comma or right paren")
			}
			p.scan()
		}
		p.scan()
	} else {
		return &assignment, p.errorExpected(p.pos, p.tok, "column name or column list")
	}

	if p.peek() != EQ {
		return &assignment, p.errorExpected(p.pos, p.tok, "=")
	}
	p.scan()

	if assignment.Expr, err = p.ParseExpr(); err != nil {
		return &assignment, err
	}

	return &assignment, nil
}

// parseSelectStatement parses a SELECT statement.
// If compounded is true, WITH, ORDER BY, & LIMIT/OFFSET are skipped.
func (p *Parser) parseSelectStatement(compounded bool, withClause *WithClause) (_ *SelectStatement, err error) {
	var stmt SelectStatement
	stmt.WithClause = withClause

	// Parse optional "WITH [RECURSIVE} cte, cte..."
	// This is only called here if this method is called directly. Generic
	// statement parsing will parse the WITH clause and pass it in instead.
	if !compounded && stmt.WithClause == nil && p.peek() == WITH {
		if stmt.WithClause, err = p.parseWithClause(); err != nil {
			return &stmt, err
		}
	}

	switch p.peek() {
	case VALUES:
		p.scan()

		for {
			var list ExprList
			if p.peek() != LP {
				return &stmt, p.errorExpected(p.pos, p.tok, "left paren")
			}
			p.scan()

			for {
				expr, err := p.ParseExpr()
				if err != nil {
					return &stmt, err
				}
				list.Exprs = append(list.Exprs, expr)

				if p.peek() == RP {
					break
				} else if p.peek() != COMMA {
					return &stmt, p.errorExpected(p.pos, p.tok, "comma or right paren")
				}
				p.scan()
			}
			p.scan()
			stmt.ValueLists = append(stmt.ValueLists, &list)

			if p.peek() != COMMA {
				break
			}
			p.scan()

		}

	case SELECT:
		p.scan()

		// Parse optional "DISTINCT" or "ALL".
		if tok := p.peek(); tok == DISTINCT {
			stmt.Distinct = p.scanExpectedTok(DISTINCT)
		} else if tok == ALL {
			stmt.All = p.scanExpectedTok(ALL)
		}

		// Parse result columns.
		for {
			col, err := p.parseResultColumn()
			if err != nil {
				return &stmt, err
			}
			stmt.Columns = append(stmt.Columns, col)

			if p.peek() != COMMA {
				break
			}
			p.scan()
		}

		// Parse FROM clause.
		if p.peek() == FROM {
			p.scan()
			if stmt.Source, err = p.parseSource(); err != nil {
				return &stmt, err
			}
		}

		// Parse WHERE clause.
		if p.peek() == WHERE {
			p.scan()
			if stmt.WhereExpr, err = p.ParseExpr(); err != nil {
				return &stmt, err
			}
		}

		// Parse GROUP BY/HAVING clause.
		if p.peek() == GROUP {
			p.scan()
			if p.peek() != BY {
				return &stmt, p.errorExpected(p.pos, p.tok, "BY")
			}
			p.scan()

			for {
				expr, err := p.ParseExpr()
				if err != nil {
					return &stmt, err
				}
				stmt.GroupByExprs = append(stmt.GroupByExprs, expr)

				if p.peek() != COMMA {
					break
				}
				p.scan()
			}

			// Parse optional HAVING clause.
			if p.peek() == HAVING {
				p.scan()
				if stmt.HavingExpr, err = p.ParseExpr(); err != nil {
					return &stmt, err
				}
			}
		}

		// Parse WINDOW clause.
		if p.peek() == WINDOW {
			p.scan()

			for {
				var window Window
				if window.Name, err = p.parseIdent("window name"); err != nil {
					return &stmt, err
				}

				if p.peek() != AS {
					return &stmt, p.errorExpected(p.pos, p.tok, "AS")
				}
				p.scan()

				if window.Definition, err = p.parseWindowDefinition(); err != nil {
					return &stmt, err
				}

				stmt.Windows = append(stmt.Windows, &window)

				if p.peek() != COMMA {
					break
				}
				p.scan()
			}
		}
	default:
		return &stmt, p.errorExpected(p.pos, p.tok, "SELECT or VALUES")
	}

	// Optionally compound additional SELECT/VALUES.
	switch tok := p.peek(); tok {
	case UNION, INTERSECT, EXCEPT:
		if tok == UNION {
			p.scan()
			if p.peek() == ALL {
				stmt.UnionAll = p.scanExpectedTok(ALL)
			}
		} else if tok == INTERSECT {
			stmt.Intersect = p.scanExpectedTok(INTERSECT)
		} else {
			stmt.Except = p.scanExpectedTok(EXCEPT)
		}

		if stmt.Compound, err = p.parseSelectStatement(true, nil); err != nil {
			return &stmt, err
		}
	}

	// Parse ORDER BY clause.
	if !compounded && p.peek() == ORDER {
		p.scan()
		if p.peek() != BY {
			return &stmt, p.errorExpected(p.pos, p.tok, "BY")
		}
		p.scan()

		for {
			term, err := p.parseOrderingTerm()
			if err != nil {
				return &stmt, err
			}
			stmt.OrderingTerms = append(stmt.OrderingTerms, term)

			if p.peek() != COMMA {
				break
			}
			p.scan()
		}
	}

	// Parse LIMIT/OFFSET clause.
	// The offset is optional. Can be specified with COMMA or OFFSET.
	// e.g. "LIMIT 1 OFFSET 2" or "LIMIT 1, 2"
	if !compounded && p.peek() == LIMIT {
		p.scan()
		if stmt.LimitExpr, err = p.ParseExpr(); err != nil {
			return &stmt, err
		}

		if tok := p.peek(); tok == OFFSET || tok == COMMA {
			if tok == OFFSET {
				p.scan()
			} else {
				p.scan()
			}
			if stmt.OffsetExpr, err = p.ParseExpr(); err != nil {
				return &stmt, err
			}
		}
	}

	return &stmt, nil
}

func (p *Parser) parseResultColumn() (_ *ResultColumn, err error) {
	var col ResultColumn

	// An initial "*" returns all columns.
	if p.peek() == STAR {
		col.Star = p.scanExpectedTok(STAR)
		return &col, nil
	}

	// Next can be either "EXPR [[AS] column-alias]" or "IDENT DOT STAR".
	// We need read the next element as an expression and then determine what next.
	if col.Expr, err = p.ParseExpr(); err != nil {
		return &col, err
	}

	// If we have a qualified ref w/ a star, don't allow an alias.
	if ref, ok := col.Expr.(*QualifiedRef); ok && ref.Star {
		return &col, nil
	}

	// If "AS" is next, the alias must follow.
	// Otherwise it can optionally be an IDENT alias.
	if p.peek() == AS {
		p.scan()
		if !isIdentToken(p.peek()) {
			return &col, p.errorExpected(p.pos, p.tok, "column alias")
		}
		col.Alias, _ = p.parseIdent("column alias")
	} else if isIdentToken(p.peek()) {
		col.Alias, _ = p.parseIdent("column alias")
	}

	return &col, nil
}

func (p *Parser) parseSource() (source Source, err error) {
	source, err = p.parseUnarySource()
	if err != nil {
		return source, err
	}

	for {
		// Exit immediately if not part of a join operator.
		switch p.peek() {
		case COMMA, NATURAL, LEFT, INNER, CROSS, JOIN:
		default:
			return source, nil
		}

		// Parse join operator.
		operator, err := p.parseJoinOperator()
		if err != nil {
			return source, err
		}
		y, err := p.parseUnarySource()
		if err != nil {
			return source, err
		}
		constraint, err := p.parseJoinConstraint()
		if err != nil {
			return source, err
		}

		// Rewrite last source to nest next join on right side.
		if lhs, ok := source.(*JoinClause); ok {
			source = &JoinClause{
				X:        lhs.X,
				Operator: lhs.Operator,
				Y: &JoinClause{
					X:          lhs.Y,
					Operator:   operator,
					Y:          y,
					Constraint: constraint,
				},
				Constraint: lhs.Constraint,
			}
		} else {
			source = &JoinClause{X: source, Operator: operator, Y: y, Constraint: constraint}
		}
	}
}

// parseUnarySource parses a qualified table name, table function name, or subquery but not a JOIN.
func (p *Parser) parseUnarySource() (source Source, err error) {
	switch p.peek() {
	case LP:
		return p.parseParenSource()
	case VALUES:
		return p.parseSelectStatement(false, nil)
	default:
		return p.parseQualifiedName(true, true, true, true, true)
	}
}

func (p *Parser) parseJoinOperator() (*JoinOperator, error) {
	var op JoinOperator

	// Handle single comma join.
	if p.peek() == COMMA {
		p.scan()
		return &op, nil
	}

	if p.peek() == NATURAL {
		op.Natural = p.scanExpectedTok(NATURAL)
	}

	switch p.peek() {
	case LEFT:
		op.Left = p.scanExpectedTok(LEFT)
		if p.peek() == OUTER {
			op.Outer = p.scanExpectedTok(OUTER)
		}
	case RIGHT:
		op.Left = p.scanExpectedTok(RIGHT)
		if p.peek() == OUTER {
			op.Outer = p.scanExpectedTok(OUTER)
		}
	case FULL:
		op.Left = p.scanExpectedTok(FULL)
		if p.peek() == OUTER {
			op.Outer = p.scanExpectedTok(OUTER)
		}
	case INNER:
		op.Inner = p.scanExpectedTok(INNER)
	case CROSS:
		op.Cross = p.scanExpectedTok(CROSS)
	}

	// Parse final JOIN.
	if p.peek() != JOIN {
		return &op, p.errorExpected(p.pos, p.tok, "JOIN")
	}
	p.scan()

	return &op, nil
}

func (p *Parser) parseJoinConstraint() (JoinConstraint, error) {
	switch p.peek() {
	case ON:
		return p.parseOnConstraint()
	case USING:
		return p.parseUsingConstraint()
	default:
		return nil, nil
	}
}

func (p *Parser) parseOnConstraint() (_ *OnConstraint, err error) {
	assert(p.peek() == ON)

	var con OnConstraint
	p.scan()
	if con.X, err = p.ParseExpr(); err != nil {
		return &con, err
	}
	return &con, nil
}

func (p *Parser) parseUsingConstraint() (*UsingConstraint, error) {
	assert(p.peek() == USING)

	var con UsingConstraint
	p.scan()

	if p.peek() != LP {
		return &con, p.errorExpected(p.pos, p.tok, "left paren")
	}
	p.scan()

	for {
		col, err := p.parseIdent("column name")
		if err != nil {
			return &con, err
		}
		con.Columns = append(con.Columns, col)

		if p.peek() == RP {
			break
		} else if p.peek() != COMMA {
			return &con, p.errorExpected(p.pos, p.tok, "comma or right paren")
		}
		p.scan()
	}
	p.scan()

	return &con, nil
}

func (p *Parser) parseParenSource() (_ *ParenSource, err error) {
	assert(p.peek() == LP)

	var source ParenSource
	p.scan()

	if p.peek() == SELECT {
		if source.X, err = p.parseSelectStatement(false, nil); err != nil {
			return &source, err
		}
	} else {
		if source.X, err = p.parseSource(); err != nil {
			return &source, err
		}
	}

	if p.peek() != RP {
		return nil, p.errorExpected(p.pos, p.tok, "right paren")
	}
	p.scan()

	if p.peek() == AS || isIdentToken(p.peek()) {
		if p.peek() == AS {
			p.scan()
		}
		if source.Alias, err = p.parseIdent("table alias"); err != nil {
			return &source, err
		}
	}

	return &source, nil
}

func (p *Parser) parseQualifiedName(schemaOK, aliasOK, indexedOK, functionOK, withoutKeywordAs bool) (_ *QualifiedName, err error) {
	ident, err := p.parseIdent("qualified name")
	if err != nil {
		return nil, err
	}

	return p.parseQualifiedNameFromIdent(ident, schemaOK, aliasOK, indexedOK, functionOK, withoutKeywordAs)
}

func (p *Parser) parseQualifiedNameFromIdent(ident *Ident, schemaOK, aliasOK, indexedOK, functionOK, withoutKeywordAs bool) (_ *QualifiedName, err error) {
	var tbl QualifiedName

	if tok := p.peek(); tok == DOT && schemaOK {
		tbl.Schema = ident
		p.scan()

		if tbl.Name, err = p.parseIdent("qualified name"); err != nil {
			return &tbl, err
		}
	} else {
		tbl.Name = ident
	}

	if p.peek() == LP && functionOK {
		tbl.FunctionCall = p.scanExpectedTok(LP)
		switch p.peek() {
		case STAR:
			tbl.FunctionStar = p.scanExpectedTok(STAR)
		case DISTINCT:
			tbl.FunctionDistinct = p.scanExpectedTok(DISTINCT)
			fallthrough
		default:
			for p.peek() != RP {
				expr, err := p.parseFunctionArg()
				if err != nil {
					return &tbl, err
				}
				tbl.FunctionArgs = append(tbl.FunctionArgs, expr)

				if p.peek() == RP {
					break
				} else if p.peek() != COMMA {
					return &tbl, p.errorExpected(p.pos, p.tok, "comma or right paren")
				}
				p.scan()
			}
		}

		if p.peek() != RP {
			return &tbl, p.errorExpected(p.pos, p.tok, "right paren")
		}
		p.scan()
	}

	// Parse optional table alias ("AS alias" or just "alias").
	if tok := p.peek(); tok == AS && aliasOK {
		p.scan()
		if tbl.Alias, err = p.parseIdent("alias name"); err != nil {
			return &tbl, err
		}
	} else if isIdentToken(tok) && !isBareToken(tok) && aliasOK && withoutKeywordAs {
		if tbl.Alias, err = p.parseIdent("alias name"); err != nil {
			return &tbl, err
		}
	}

	// Parse optional "INDEXED BY index-name" or "NOT INDEXED".
	switch p.peek() {
	case INDEXED:
		if !indexedOK {
			return &tbl, nil
		}

		p.scan()
		if p.peek() != BY {
			return &tbl, p.errorExpected(p.pos, p.tok, "BY")
		}
		p.scan()

		if tbl.Index, err = p.parseIdent("index name"); err != nil {
			return &tbl, err
		}
	case NOT:
		if !indexedOK {
			return &tbl, nil
		}

		p.scan()
		if p.peek() != INDEXED {
			return &tbl, p.errorExpected(p.pos, p.tok, "INDEXED")
		}
		tbl.NotIndexed = p.scanExpectedTok(INDEXED)
	}

	return &tbl, nil
}

func (p *Parser) parseWithClause() (*WithClause, error) {
	assert(p.peek() == WITH)

	var clause WithClause
	p.scan()
	if p.peek() == RECURSIVE {
		clause.Recursive = p.scanExpectedTok(RECURSIVE)
	}

	// Parse comma-delimited list of common table expressions (CTE).
	for {
		cte, err := p.parseCTE()
		if err != nil {
			return &clause, err
		}
		clause.CTEs = append(clause.CTEs, cte)

		if p.peek() != COMMA {
			break
		}
		p.scan()
	}
	return &clause, nil
}

func (p *Parser) parseCTE() (_ *CTE, err error) {
	var cte CTE
	if cte.TableName, err = p.parseIdent("table name"); err != nil {
		return &cte, err
	}

	// Parse optional column list.
	if p.peek() == LP {
		p.scan()

		for {
			column, err := p.parseIdent("column name")
			if err != nil {
				return &cte, err
			}
			cte.Columns = append(cte.Columns, column)

			if p.peek() == RP {
				break
			} else if p.peek() != COMMA {
				return nil, p.errorExpected(p.pos, p.tok, "comma or right paren")
			}
			p.scan()
		}
		p.scan()
	}

	if p.peek() != AS {
		return nil, p.errorExpected(p.pos, p.tok, "AS")
	}
	p.scan()

	// Parse select statement.
	if p.peek() != LP {
		return nil, p.errorExpected(p.pos, p.tok, "left paren")
	}
	p.scan()

	if cte.Select, err = p.parseSelectStatement(false, nil); err != nil {
		return &cte, err
	}

	if p.peek() != RP {
		return nil, p.errorExpected(p.pos, p.tok, "right paren")
	}
	p.scan()

	return &cte, nil
}

func (p *Parser) mustParseLiteral() Expr {
	assert(isLiteralToken(p.tok))
	_, tok, lit := p.scan()
	switch tok {
	case STRING:
		return &StringLit{Value: lit}
	case CURRENT_TIME, CURRENT_DATE, CURRENT_TIMESTAMP:
		return &TimestampLit{Value: lit}
	case BLOB:
		return &BlobLit{Value: lit}
	case FLOAT, INTEGER:
		return &NumberLit{Value: lit}
	case TRUE, FALSE:
		return &BoolLit{Value: tok == TRUE}
	default:
		assert(tok == NULL)
		return &NullLit{}
	}
}

func (p *Parser) ParseExpr() (expr Expr, err error) {
	return p.parseBinaryExpr(1)
}

func (p *Parser) parseOperand() (expr Expr, err error) {
	_, tok, lit := p.scan()
	switch {
	case tok == CAST:
		p.unscan()
		return p.parseCastExpr()
	case tok == CASE:
		p.unscan()
		return p.parseCaseExpr()
	case tok == RAISE:
		p.unscan()
		return p.parseRaise()
	case tok == NOT:
		if p.peek() == EXISTS {
			return p.parseExists(true)
		}

		expr, err = p.parseOperand()
		if err != nil {
			return nil, err
		}

		return &UnaryExpr{Op: OP_NOT, X: expr}, nil
	case tok == EXISTS:
		p.unscan()
		return p.parseExists(false)
	case tok == SELECT || tok == WITH:
		p.unscan()
		sel, err := p.parseSelectStatement(false, nil)
		return sel, err
	case tok == STRING:
		if p.peek() != DOT && p.peek() != LP {
			return &StringLit{Value: lit}, nil
		}

		fallthrough
	case isExprIdentToken(tok):
		ident := &Ident{Name: lit, Quoted: tok == QIDENT || tok == STRING}
		switch p.peek() {
		case DOT:
			qr, err := p.parseQualifiedRef(ident)
			if err != nil {
				return nil, err
			}

			return qr, nil
		case LP:
			return p.parseCall(ident)
		}

		return ident, nil
	case tok == BLOB:
		return &BlobLit{Value: lit}, nil
	case tok == FLOAT, tok == INTEGER:
		return &NumberLit{Value: lit}, nil
	case tok == NULL:
		return &NullLit{}, nil
	case tok == TRUE, tok == FALSE:
		return &BoolLit{Value: tok == TRUE}, nil
	case tok == BIND:
		return &BindExpr{Name: lit}, nil
	case tok == PLUS, tok == MINUS, tok == BITNOT:
		expr, err = p.parseOperand()
		if err != nil {
			return nil, err
		}

		switch tok {
		case PLUS:
			return &UnaryExpr{Op: OP_MINUS, X: expr}, nil
		case MINUS:
			return &UnaryExpr{Op: OP_MINUS, X: expr}, nil
		case BITNOT:
			return &UnaryExpr{Op: OP_BITNOT, X: expr}, nil
		}

		panic("unreachable")
	case tok == LP:
		p.unscan()
		return p.parseParenExpr()
	default:
		return nil, p.errorExpected(p.pos, p.tok, "expression")
	}
}

func (p *Parser) parseBinaryExpr(prec1 int) (expr Expr, err error) {
	x, err := p.parseOperand()
	if err != nil {
		return nil, err
	}

	for {
		if precedenceByStartBinaryOp(p.peek()) < prec1 {
			return x, nil
		}

		_, op, err := p.scanBinaryOp()
		if err != nil {
			return nil, err
		}

		switch op {
		case OP_NOTNULL, OP_ISNULL:
			x = &Null{X: x, Op: op}
		case OP_IN, OP_NOT_IN:
			var y InExpr
			y.X = x
			y.Op = op

			switch p.peek() {
			case LP:
				p.scan()

				switch p.peek() {
				case SELECT, WITH:
					y.Select, err = p.parseSelectStatement(false, nil)
					if err != nil {
						return x, err
					}
				default:
					y.Values = &ExprList{}
					for p.peek() != RP {
						x, err := p.ParseExpr()
						if err != nil {
							return x, err
						}
						y.Values.Exprs = append(y.Values.Exprs, x)

						if p.peek() == RP {
							break
						} else if p.peek() != COMMA {
							return x, p.errorExpected(p.pos, p.tok, "comma or right paren")
						}
						p.scan()
					}
				}

				if p.peek() != RP {
					return x, p.errorExpected(p.pos, p.tok, "right paren")
				}
				p.scan()
			default:
				y.TableOrFunction, err = p.parseQualifiedName(true, false, false, true, false)
				if err != nil {
					return x, err
				}
			}

			x = &y
		case OP_BETWEEN, OP_NOT_BETWEEN:
			lhs, err := p.parseBinaryExpr(op.Precedence() + 1)
			if err != nil {
				return x, err
			}

			if p.peek() != AND {
				return x, p.errorExpected(p.pos, p.tok, "AND")
			}
			p.scan()

			rhs, err := p.parseBinaryExpr(op.Precedence() + 1)
			if err != nil {
				return x, err
			}

			x = &BinaryExpr{
				X:  x,
				Op: op,
				Y:  &BinaryExpr{X: lhs, Op: OP_AND, Y: rhs},
			}
		case OP_LIKE, OP_NOT_LIKE:
			y, err := p.parseBinaryExpr(OP_ESCAPE.Precedence() + 1) // make sure we not consume the ESCAPE token
			if err != nil {
				return nil, err
			}

			if p.peek() == ESCAPE {
				p.scan()

				if c := p.peek(); c != STRING && c != QIDENT {
					return x, p.errorExpected(p.pos, p.tok, "string or quoted identifier")
				}

				escapeExpr, err := p.parseOperand()
				if err != nil {
					return x, err
				}

				x = &BinaryExpr{
					X:  &BinaryExpr{X: x, Op: op, Y: y},
					Op: OP_ESCAPE,
					Y:  escapeExpr,
				}
			} else {
				x = &BinaryExpr{X: x, Op: op, Y: y}
			}
		case OP_ESCAPE:
			return x, p.errorExpected(p.pos, p.tok, "op ESCAPE can not be used without LIKE")
		case OP_COLLATE:
			if !isIdentToken(p.peek()) {
				return x, p.errorExpected(p.pos, p.tok, "collation name")
			}

			fallthrough
		default:
			y, err := p.parseBinaryExpr(op.Precedence() + 1)
			if err != nil {
				return nil, err
			}
			x = &BinaryExpr{X: x, Op: op, Y: y}
		}
	}
}

func (p *Parser) parseQualifiedRef(table *Ident) (_ *QualifiedRef, err error) {
	assert(p.peek() == DOT)

	var expr QualifiedRef
	expr.Table = &QualifiedName{Name: table}
	p.scan()

	if p.peek() == STAR {
		expr.Star = p.scanExpectedTok(STAR)
	} else if isIdentToken(p.peek()) {
		if expr.Column, err = p.parseIdent("column name"); err != nil {
			return &expr, err
		}
	} else {
		return &expr, p.errorExpected(p.pos, p.tok, "column name")
	}

	if p.peek() == DOT {
		if expr.Star {
			return &expr, p.errorExpected(p.pos, p.tok, "qualified ref with star can not have another dot")
		}

		p.scan()
		expr.Table.Schema = expr.Table.Name
		expr.Table.Name = expr.Column
		if expr.Column, err = p.parseIdent("column name"); err != nil {
			return &expr, err
		}
	}

	return &expr, nil
}

func (p *Parser) parseCall(name *Ident) (_ *Call, err error) {
	assert(p.peek() == LP)

	var expr Call
	expr.Name, err = p.parseQualifiedNameFromIdent(name, false, false, false, true, false)
	if err != nil {
		return &expr, err
	}

	if !expr.Name.FunctionCall {
		return &expr, p.errorExpected(p.pos, p.tok, "function call")
	}

	// Parse optional filter clause.
	if p.peek() == FILTER {
		p.scan()

		if p.peek() != LP {
			return &expr, p.errorExpected(p.pos, p.tok, "left paren")
		}
		p.scan()

		if p.peek() != WHERE {
			return &expr, p.errorExpected(p.pos, p.tok, "WHERE")
		}
		p.scan()

		if expr.Filter, err = p.ParseExpr(); err != nil {
			return &expr, err
		}

		if p.peek() != RP {
			return &expr, p.errorExpected(p.pos, p.tok, "right paren")
		}
		p.scan()
	}

	// Parse optional over clause.
	if p.peek() == OVER {
		p.scan()

		// If specifying a window name, read it and exit.
		if isIdentToken(p.peek()) {
			if expr.OverName, err = p.parseIdent("window name"); err != nil {
				return &expr, err
			}
		} else if expr.OverWindow, err = p.parseWindowDefinition(); err != nil {
			return &expr, err
		}
	}

	return &expr, nil
}

func (p *Parser) parseWindowDefinition() (_ *WindowDefinition, err error) {
	var def WindowDefinition

	// Otherwise parse the window definition.
	if p.peek() != LP {
		return &def, p.errorExpected(p.pos, p.tok, "left paren")
	}
	p.scan()

	// Read base window name.
	if tok := p.peek(); isIdentToken(tok) && tok != PARTITION && tok != ORDER && tok != RANGE && tok != ROWS && tok != GROUPS {
		_, tok, lit := p.scan()
		def.Base = &Ident{Name: lit, Quoted: tok == QIDENT}
	}

	// Parse "PARTITION BY expr, expr..."
	if p.peek() == PARTITION {
		p.scan()
		if p.peek() != BY {
			return &def, p.errorExpected(p.pos, p.tok, "BY")
		}
		p.scan()

		for {
			partition, err := p.ParseExpr()
			if err != nil {
				return &def, err
			}
			def.Partitions = append(def.Partitions, partition)

			if p.peek() != COMMA {
				break
			}
			p.scan()
		}
	}

	// Parse "ORDER BY ordering-term, ordering-term..."
	if p.peek() == ORDER {
		p.scan()
		if p.peek() != BY {
			return &def, p.errorExpected(p.pos, p.tok, "BY")
		}
		p.scan()

		for {
			term, err := p.parseOrderingTerm()
			if err != nil {
				return &def, err
			}
			def.OrderingTerms = append(def.OrderingTerms, term)

			if p.peek() != COMMA {
				break
			}
			p.scan()
		}
	}

	// Parse frame spec.
	if tok := p.peek(); tok == RANGE || tok == ROWS || tok == GROUPS {
		if def.Frame, err = p.parseFrameSpec(); err != nil {
			return &def, err
		}
	}

	// Parse final rparen.
	if p.peek() != RP {
		return &def, p.errorExpected(p.pos, p.tok, "right paren")
	}
	p.scan()

	return &def, nil
}

func (p *Parser) parseOrderingTerm() (_ *OrderingTerm, err error) {
	var term OrderingTerm
	if term.X, err = p.ParseExpr(); err != nil { // make sure not consume the COLLATE token
		return &term, err
	}

	// Parse optional sort direction ("ASC" or "DESC")
	switch p.peek() {
	case ASC:
		term.Asc = p.scanExpectedTok(ASC)
	case DESC:
		term.Desc = p.scanExpectedTok(DESC)
	}

	// Parse optional "NULLS FIRST" or "NULLS LAST"
	if p.peek() == NULLS {
		p.scan()
		switch p.peek() {
		case FIRST:
			term.NullsFirst = p.scanExpectedTok(FIRST)
		case LAST:
			term.NullsLast = p.scanExpectedTok(LAST)
		default:
			return &term, p.errorExpected(p.pos, p.tok, "FIRST or LAST")
		}
	}

	return &term, nil
}

func (p *Parser) parseFrameSpec() (_ *FrameSpec, err error) {
	assert(p.peek() == RANGE || p.peek() == ROWS || p.peek() == GROUPS)

	var spec FrameSpec

	switch p.peek() {
	case RANGE:
		spec.Range = p.scanExpectedTok(RANGE)
	case ROWS:
		spec.Rows = p.scanExpectedTok(ROWS)
	case GROUPS:
		spec.Groups = p.scanExpectedTok(GROUPS)
	}

	// Parsing BETWEEN indicates that two expressions are required.
	if p.peek() == BETWEEN {
		spec.Between = p.scanExpectedTok(BETWEEN)
	}

	// Parse X expression: "UNBOUNDED PRECEDING", "CURRENT ROW", "expr PRECEDING|FOLLOWING"
	if p.peek() == UNBOUNDED {
		spec.UnboundedX = p.scanExpectedTok(UNBOUNDED)
		if p.peek() != PRECEDING {
			return &spec, p.errorExpected(p.pos, p.tok, "PRECEDING")
		}
		spec.PrecedingX = p.scanExpectedTok(PRECEDING)
	} else if p.peek() == CURRENT {
		p.scan()
		if p.peek() != ROW {
			return &spec, p.errorExpected(p.pos, p.tok, "ROW")
		}
		spec.CurrentRowX = p.scanExpectedTok(ROW)
	} else {
		if spec.X, err = p.ParseExpr(); err != nil {
			return &spec, err
		}
		if p.peek() == PRECEDING {
			spec.PrecedingX = p.scanExpectedTok(PRECEDING)
		} else if p.peek() == FOLLOWING && spec.Between { // FOLLOWING only allowed with BETWEEN
			spec.FollowingX = p.scanExpectedTok(FOLLOWING)
		} else {
			if spec.Between {
				return &spec, p.errorExpected(p.pos, p.tok, "PRECEDING or FOLLOWING")
			}
			return &spec, p.errorExpected(p.pos, p.tok, "PRECEDING")
		}
	}

	// Read "AND y" if range is BETWEEN.
	if spec.Between {
		if p.peek() != AND {
			return &spec, p.errorExpected(p.pos, p.tok, "AND")
		}
		p.scan()

		// Parse Y expression: "UNBOUNDED FOLLOWING", "CURRENT ROW", "expr PRECEDING|FOLLOWING"
		if p.peek() == UNBOUNDED {
			spec.UnboundedY = p.scanExpectedTok(UNBOUNDED)
			if p.peek() != FOLLOWING {
				return &spec, p.errorExpected(p.pos, p.tok, "FOLLOWING")
			}
			spec.FollowingY = p.scanExpectedTok(FOLLOWING)
		} else if p.peek() == CURRENT {
			p.scan()
			if p.peek() != ROW {
				return &spec, p.errorExpected(p.pos, p.tok, "ROW")
			}
			spec.CurrentRowY = p.scanExpectedTok(ROW)
		} else {
			if spec.Y, err = p.ParseExpr(); err != nil {
				return &spec, err
			}
			if p.peek() == PRECEDING {
				spec.PrecedingY = p.scanExpectedTok(PRECEDING)
			} else if p.peek() == FOLLOWING {
				spec.FollowingY = p.scanExpectedTok(FOLLOWING)
			} else {
				return &spec, p.errorExpected(p.pos, p.tok, "PRECEDING or FOLLOWING")
			}
		}
	}

	// Parse optional EXCLUDE.
	if p.peek() == EXCLUDE {
		p.scan()

		switch p.peek() {
		case NO:
			p.scan()
			if p.peek() != OTHERS {
				return &spec, p.errorExpected(p.pos, p.tok, "OTHERS")
			}
			spec.ExcludeNoOthers = p.scanExpectedTok(OTHERS)
		case CURRENT:
			p.scan()
			if p.peek() != ROW {
				return &spec, p.errorExpected(p.pos, p.tok, "ROW")
			}
			spec.ExcludeCurrentRow = p.scanExpectedTok(ROW)
		case GROUP:
			spec.ExcludeGroup = p.scanExpectedTok(GROUP)
		case TIES:
			spec.ExcludeTies = p.scanExpectedTok(TIES)
		default:
			return &spec, p.errorExpected(p.pos, p.tok, "NO OTHERS, CURRENT ROW, GROUP, or TIES")
		}
	}

	return &spec, nil
}

func (p *Parser) parseParenExpr() (Expr, error) {
	p.scan()

	// Parse the first expression
	x, err := p.ParseExpr()
	if err != nil {
		return nil, err
	}

	// If there's no comma after the first expression, treat it as a normal parenthesized expression
	if p.peek() != COMMA {
		p.scan()
		return &ParenExpr{Expr: x}, nil
	}

	// If there's a comma, we're dealing with an expression list
	var list ExprList
	list.Exprs = append(list.Exprs, x)

	for p.peek() == COMMA {
		p.scan() // consume the comma

		expr, err := p.ParseExpr()
		if err != nil {
			return &list, err
		}
		list.Exprs = append(list.Exprs, expr)
	}

	if p.peek() != RP {
		return &list, p.errorExpected(p.pos, p.tok, "right paren")
	}
	p.scan()

	return &list, nil
}

func (p *Parser) parseCastExpr() (_ *CastExpr, err error) {
	assert(p.peek() == CAST)

	var expr CastExpr
	p.scan()

	if p.peek() != LP {
		return &expr, p.errorExpected(p.pos, p.tok, "left paren")
	}
	p.scan()

	if expr.X, err = p.ParseExpr(); err != nil {
		return &expr, err
	}

	if p.peek() != AS {
		return &expr, p.errorExpected(p.pos, p.tok, "AS")
	}
	p.scan()

	if expr.Type, err = p.parseType(); err != nil {
		return &expr, err
	}

	if p.peek() != RP {
		return &expr, p.errorExpected(p.pos, p.tok, "right paren")
	}
	p.scan()
	return &expr, nil
}

func (p *Parser) parseCaseExpr() (_ *CaseExpr, err error) {
	assert(p.peek() == CASE)

	var expr CaseExpr
	p.scan()

	// Parse optional expression if WHEN is not next.
	if p.peek() != WHEN {
		if expr.Operand, err = p.ParseExpr(); err != nil {
			return &expr, err
		}
	}

	// Parse one or more WHEN/THEN pairs.
	for {
		var blk CaseBlock
		if p.peek() != WHEN {
			return &expr, p.errorExpected(p.pos, p.tok, "WHEN")
		}
		p.scan()

		if blk.Condition, err = p.ParseExpr(); err != nil {
			return &expr, err
		}

		if p.peek() != THEN {
			return &expr, p.errorExpected(p.pos, p.tok, "THEN")
		}
		p.scan()

		if blk.Body, err = p.ParseExpr(); err != nil {
			return &expr, err
		}

		expr.Blocks = append(expr.Blocks, &blk)

		if tok := p.peek(); tok == ELSE || tok == END {
			break
		} else if tok != WHEN {
			return &expr, p.errorExpected(p.pos, p.tok, "WHEN, ELSE or END")
		}
	}

	// Parse optional ELSE block.
	if p.peek() == ELSE {
		p.scan()
		if expr.ElseExpr, err = p.ParseExpr(); err != nil {
			return &expr, err
		}
	}

	if p.peek() != END {
		return &expr, p.errorExpected(p.pos, p.tok, "END")
	}
	p.scan()

	return &expr, nil
}

func (p *Parser) parseExists(not bool) (_ *Exists, err error) {
	assert(p.peek() == EXISTS)

	var expr Exists
	expr.Not = not

	if p.peek() != EXISTS {
		return &expr, p.errorExpected(p.pos, p.tok, "EXISTS")
	}
	p.scan()

	if p.peek() != LP {
		return &expr, p.errorExpected(p.pos, p.tok, "left paren")
	}
	p.scan()

	if expr.Select, err = p.parseSelectStatement(false, nil); err != nil {
		return &expr, err
	}

	if p.peek() != RP {
		return &expr, p.errorExpected(p.pos, p.tok, "right paren")
	}
	p.scan()

	return &expr, nil
}

func (p *Parser) parseRaise() (_ *Raise, err error) {
	assert(p.peek() == RAISE)

	var expr Raise
	p.scan()

	if p.peek() != LP {
		return &expr, p.errorExpected(p.pos, p.tok, "left paren")
	}
	p.scan()

	// Parse either IGNORE, ROLLBACK, ABORT, or FAIL.
	// ROLLBACK also has an error message.
	if p.peek() == IGNORE {
		expr.Ignore = p.scanExpectedTok(IGNORE)
	} else {
		switch p.peek() {
		case ROLLBACK:
			expr.Rollback = p.scanExpectedTok(ROLLBACK)
		case ABORT:
			expr.Abort = p.scanExpectedTok(ABORT)
		case FAIL:
			expr.Fail = p.scanExpectedTok(FAIL)
		default:
			return &expr, p.errorExpected(p.pos, p.tok, "IGNORE, ROLLBACK, ABORT, or FAIL")
		}

		if p.peek() != COMMA {
			return &expr, p.errorExpected(p.pos, p.tok, "comma")
		}
		p.scan()

		if p.peek() != STRING {
			return &expr, p.errorExpected(p.pos, p.tok, "error message")
		}
		_, _, lit := p.scan()
		expr.Error = &StringLit{Value: lit}
	}

	if p.peek() != RP {
		return &expr, p.errorExpected(p.pos, p.tok, "right paren")
	}
	p.scan()

	return &expr, nil
}

func (p *Parser) parseSignedNumber(desc string) (*NumberLit, error) {
	_, tok, lit := p.scan()

	// Prepend "+" or "-" to the next number value.
	if tok == PLUS || tok == MINUS {
		prefix := lit
		_, tok, lit = p.scan()
		lit = prefix + lit
	}

	switch tok {
	case FLOAT, INTEGER:
		return &NumberLit{Value: lit}, nil
	default:
		return nil, p.errorExpected(p.pos, p.tok, desc)
	}
}

func (p *Parser) parseAlterTableStatement() (_ *AlterTableStatement, err error) {
	assert(p.peek() == ALTER)

	var stmt AlterTableStatement
	p.scan()
	if p.peek() != TABLE {
		return &stmt, p.errorExpected(p.pos, p.tok, "TABLE")
	}
	p.scan()

	stmt.Name, err = p.parseQualifiedName(true, false, false, false, false)
	if err != nil {
		return &stmt, err
	}

	switch p.peek() {
	case RENAME:
		p.scan()

		// Parse "RENAME TO new-table-name".
		if p.peek() == TO {
			p.scan()
			if stmt.NewName, err = p.parseIdent("new table name"); err != nil {
				return &stmt, err
			}
			return &stmt, nil
		}

		// Otherwise parse "RENAME [COLUMN] column-name TO new-column-name".
		if p.peek() == COLUMN {
			p.scan()
		} else if !isIdentToken(p.peek()) {
			return &stmt, p.errorExpected(p.pos, p.tok, "COLUMN keyword or column name")
		}
		if stmt.ColumnName, err = p.parseIdent("column name"); err != nil {
			return &stmt, err
		}
		if p.peek() != TO {
			return &stmt, p.errorExpected(p.pos, p.tok, "TO")
		}
		p.scan()
		if stmt.NewColumnName, err = p.parseIdent("new column name"); err != nil {
			return &stmt, err
		}

		return &stmt, nil
	case ADD:
		p.scan()
		if p.peek() == COLUMN {
			p.scan()
		} else if !isIdentToken(p.peek()) {
			return &stmt, p.errorExpected(p.pos, p.tok, "COLUMN keyword or column name")
		}
		if stmt.ColumnDef, err = p.parseColumnDefinition(); err != nil {
			return &stmt, err
		}
		return &stmt, nil
	default:
		return &stmt, p.errorExpected(p.pos, p.tok, "ADD or RENAME")
	}
}

func (p *Parser) parsePragmaStatement() (_ *PragmaStatement, err error) {
	assert(p.peek() == PRAGMA)

	var stmt PragmaStatement
	p.scan()

	lit, err := p.parseIdent("schema name")
	if err != nil {
		return &stmt, err
	}

	// Handle <schema>.<pragma-name>
	if p.peek() == DOT {
		stmt.Schema = lit
		p.scan()
		if lit, err = p.parseIdent("pragma name"); err != nil {
			return &stmt, err
		}
	}

	switch p.peek() {
	case EQ:
		// Parse as binary expression: pragma-name = value
		p.scan()

		rhs, err := p.ParseExpr()
		if err != nil {
			return &stmt, err
		}

		stmt.Expr = &BinaryExpr{
			X:  lit,
			Op: OP_EQ,
			Y:  rhs,
		}
	case LP:
		// Parse as function call: pragma-name(args)
		call, err := p.parseCall(lit)
		if err != nil {
			return &stmt, err
		}
		stmt.Expr = call
	default:
		stmt.Expr = lit
	}

	return &stmt, nil
}

func (p *Parser) parseAnalyzeStatement() (_ *AnalyzeStatement, err error) {
	assert(p.peek() == ANALYZE)

	var stmt AnalyzeStatement
	p.scan()

	if isIdentToken(p.peek()) {
		stmt.Name, err = p.parseQualifiedName(true, false, false, false, false)
		if err != nil {
			return nil, err
		}
	}

	return &stmt, nil
}

func (p *Parser) parseReindexStatement() (_ *ReindexStatement, err error) {
	assert(p.peek() == REINDEX)

	var stmt ReindexStatement
	p.scan()

	// handle case with index, table or collation name
	if tok := p.peek(); isIdentToken(tok) {
		stmt.Name, err = p.parseQualifiedName(true, false, false, false, false)
		if err != nil {
			return &stmt, err
		}
	}

	return &stmt, nil
}

func (p *Parser) scanExpectedTok(tok Token) bool {
	_, t, _ := p.scan()
	assert(t == tok)
	return true
}

func (p *Parser) scan() (Pos, Token, string) {
	if p.full {
		p.full = false
		return p.pos, p.tok, p.lit
	}

	// Continue scanning until we find a non-comment token.
	for {
		if pos, tok, lit := p.s.Scan(); tok != COMMENT {
			p.pos, p.tok, p.lit = pos, tok, lit
			return p.pos, p.tok, p.lit
		}
	}
}

// scanBinaryOp performs a scan but combines multi-word operations into a single token.
func (p *Parser) scanBinaryOp() (Pos, OpType, error) {
	pos, tok, _ := p.scan()
	switch tok {
	case PLUS:
		return pos, OP_PLUS, nil
	case MINUS:
		return pos, OP_MINUS, nil
	case STAR:
		return pos, OP_MULTIPLY, nil
	case SLASH:
		return pos, OP_DIVIDE, nil
	case REM:
		return pos, OP_MODULO, nil
	case CONCAT:
		return pos, OP_CONCAT, nil
	case BETWEEN:
		return pos, OP_BETWEEN, nil
	case LSHIFT:
		return pos, OP_LSHIFT, nil
	case RSHIFT:
		return pos, OP_RSHIFT, nil
	case BITAND:
		return pos, OP_BITAND, nil
	case BITOR:
		return pos, OP_BITOR, nil
	case LT:
		return pos, OP_LT, nil
	case LE:
		return pos, OP_LE, nil
	case GT:
		return pos, OP_GT, nil
	case GE:
		return pos, OP_GE, nil
	case EQ:
		return pos, OP_EQ, nil
	case NE:
		return pos, OP_NE, nil
	case JSON_EXTRACT_JSON:
		return pos, OP_JSON_EXTRACT_JSON, nil
	case JSON_EXTRACT_SQL:
		return pos, OP_JSON_EXTRACT_SQL, nil
	case IN:
		return pos, OP_IN, nil
	case LIKE:
		return pos, OP_LIKE, nil
	case GLOB:
		return pos, OP_GLOB, nil
	case MATCH:
		return pos, OP_MATCH, nil
	case REGEXP:
		return pos, OP_REGEXP, nil
	case AND:
		return pos, OP_AND, nil
	case OR:
		return pos, OP_OR, nil
	case ISNULL:
		return pos, OP_ISNULL, nil
	case NOTNULL:
		return pos, OP_NOTNULL, nil
	case ESCAPE:
		return pos, OP_ESCAPE, nil
	case COLLATE:
		return pos, OP_COLLATE, nil
	case IS:
		if p.peek() == NOT {
			p.scan()

			if p.peek() != DISTINCT {
				return pos, OP_IS_NOT, nil
			}

			p.scan()
			if p.peek() != FROM {
				return pos, OP_ILLEGAL, p.errorExpected(p.pos, p.tok, "FROM")
			}

			p.scan()
			return pos, OP_IS_NOT_DISTINCT_FROM, nil
		} else if p.peek() == NULL {
			p.scan()
			return pos, OP_ISNULL, nil
		} else if p.peek() == DISTINCT {
			p.scan()

			if p.peek() != FROM {
				return pos, OP_ILLEGAL, p.errorExpected(p.pos, p.tok, "FROM")
			}

			p.scan()
			return pos, OP_IS_DISTINCT_FROM, nil
		}

		return pos, OP_IS, nil
	case NOT:
		switch p.peek() {
		case IN:
			p.scan()
			return pos, OP_NOT_IN, nil
		case LIKE:
			p.scan()
			return pos, OP_NOT_LIKE, nil
		case GLOB:
			p.scan()
			return pos, OP_NOT_GLOB, nil
		case REGEXP:
			p.scan()
			return pos, OP_NOT_REGEXP, nil
		case MATCH:
			p.scan()
			return pos, OP_NOT_MATCH, nil
		case BETWEEN:
			p.scan()
			return pos, OP_NOT_BETWEEN, nil
		case NULL:
			p.scan()
			return pos, OP_NOTNULL, nil
		default:
			return pos, OP_ILLEGAL, p.errorExpected(p.pos, p.tok, "IN, LIKE, GLOB, REGEXP, MATCH, BETWEEN, IS/NOT NULL")
		}
	default:
		return pos, OP_ILLEGAL, p.errorExpected(pos, tok, "binary operator")
	}
}

func (p *Parser) parseAttachStatement() (_ *AttachStatement, err error) {
	assert(p.peek() == ATTACH)
	var stmt AttachStatement

	p.scan()
	if p.peek() == DATABASE {
		p.scan()
	}

	if stmt.Expr, err = p.parseIdent("attach expr"); err != nil {
		return &stmt, err
	}

	if p.peek() != AS {
		return &stmt, p.errorExpected(p.pos, p.tok, "AS")
	}

	p.scan()
	if stmt.Schema, err = p.parseIdent("schema name"); err != nil {
		return &stmt, err
	}

	return &stmt, nil
}

func (p *Parser) parseDetachStatement() (_ *DetachStatement, err error) {
	assert(p.peek() == DETACH)
	var stmt DetachStatement
	p.scan()
	if p.peek() == DATABASE {
		p.scan()
	}

	if stmt.Schema, err = p.parseIdent("schema name"); err != nil {
		return &stmt, err
	}

	return &stmt, nil
}

func (p *Parser) parseVacuumStatement() (_ *VacuumStatement, err error) {
	assert(p.peek() == VACUUM)
	var stmt VacuumStatement
	p.scan()

	switch p.peek() {
	case INTO:
	case EOF, SEMI:
		return &stmt, nil
	default:
		if stmt.Schema, err = p.parseIdent("schema name"); err != nil {
			return &stmt, err
		}
	}

	// If the next token is "INTO", parse it.
	if p.peek() == INTO {
		p.scan()
		if stmt.Expr, err = p.parseIdent("vacuum expr"); err != nil {
			return &stmt, err
		}
	}

	return &stmt, nil
}

func (p *Parser) parseConflictClause() (_ *ConflictClause, err error) {
	assert(p.peek() == ON)
	var clause ConflictClause
	p.scan()

	if p.peek() != CONFLICT {
		return &clause, p.errorExpected(p.pos, p.tok, "CONFLICT")
	}

	p.scan()

	switch p.peek() {
	case ROLLBACK:
		clause.Rollback = p.scanExpectedTok(ROLLBACK)
	case ABORT:
		clause.Abort = p.scanExpectedTok(ABORT)
	case FAIL:
		clause.Fail = p.scanExpectedTok(FAIL)
	case IGNORE:
		clause.Ignore = p.scanExpectedTok(IGNORE)
	case REPLACE:
		clause.Replace = p.scanExpectedTok(REPLACE)
	default:
		return &clause, p.errorExpected(p.pos, p.tok, "ROLLBACK, ABORT, FAIL, IGNORE or REPLACE")
	}
	return &clause, nil
}

func (p *Parser) parseFunctionArg() (_ *FunctionArg, err error) {
	var arg FunctionArg

	if arg.Expr, err = p.ParseExpr(); err != nil {
		return &arg, err
	}

	if p.peek() == ORDER {
		p.scan()
		if p.peek() != BY {
			return &arg, p.errorExpected(p.pos, p.tok, "BY")
		}
		p.scan()

		for {
			term, err := p.parseOrderingTerm()
			if err != nil {
				return &arg, err
			}
			arg.OrderingTerms = append(arg.OrderingTerms, term)

			if p.peek() != COMMA {
				break
			}
			p.scan()
		}
	}

	return &arg, nil
}

func (p *Parser) peek() Token {
	if !p.full {
		p.scan()
		p.unscan()
	}
	return p.tok
}

func (p *Parser) unscan() {
	assert(!p.full)
	p.full = true
}

func (p *Parser) errorExpected(pos Pos, _ Token, msg string) error {
	msg = "expected " + msg
	if pos == p.pos {
		if isLiteralToken(p.tok) || p.tok == QIDENT || p.tok == IDENT {
			msg += ", found " + p.lit
		} else {
			msg += ", found '" + p.tok.String() + "'"
		}
	}
	return &Error{Pos: pos, Msg: msg}
}

// Error represents a parse error.
type Error struct {
	Pos Pos
	Msg string
}

// Error implements the error interface.
func (e Error) Error() string {
	return e.Pos.String() + ": " + e.Msg
}

// isConstraintStartToken returns true if tok is the initial token of a constraint.
func isConstraintStartToken(tok Token, isTable bool) bool {
	switch tok {
	case CONSTRAINT, PRIMARY, UNIQUE, CHECK:
		return true // table & column
	case FOREIGN:
		return isTable // table only
	case NOT, DEFAULT, REFERENCES, GENERATED, AS, COLLATE:
		return !isTable // column only
	default:
		return false
	}
}

// isLiteralToken returns true if token represents a literal value.
func isLiteralToken(tok Token) bool {
	switch tok {
	case FLOAT, INTEGER, STRING, BLOB, TRUE, FALSE, NULL,
		CURRENT_TIME, CURRENT_DATE, CURRENT_TIMESTAMP:
		return true
	default:
		return false
	}
}
