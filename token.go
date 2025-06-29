package sql

import (
	"strconv"
)

var bareTokensMap = make(map[Token]struct{})

func init() {
	for _, tok := range bareTokens {
		bareTokensMap[tok] = struct{}{}
	}
}

// Token is the set of lexical tokens of the Go programming language.
type Token int

// The list of tokens.
const (
	// Special tokens
	ILLEGAL Token = iota
	EOF
	COMMENT
	SPACE

	// literals
	IDENT   // IDENT
	QIDENT  // "IDENT"
	STRING  // 'string'
	BLOB    // ???
	FLOAT   // 123.45
	INTEGER // 123
	NULL    // NULL
	TRUE    // true
	FALSE   // false
	BIND    //? or ?NNN or :VVV or @VVV or $VVV

	// operators and delimiters
	SEMI              // ;
	LP                // (
	RP                // )
	COMMA             // ,
	NE                // !=
	EQ                // =
	LE                // <=
	LT                // <
	GT                // >
	GE                // >=
	BITAND            // &
	BITOR             // |
	BITNOT            // ~
	LSHIFT            // <<
	RSHIFT            // >>
	PLUS              // +
	MINUS             // -
	STAR              // *
	SLASH             // /
	REM               // %
	CONCAT            // ||
	DOT               // .
	JSON_EXTRACT_JSON // ->
	JSON_EXTRACT_SQL  // ->>

	// keywords
	ABORT
	ACTION
	ADD
	AFTER
	ALL
	ALTER
	ALWAYS
	ANALYZE
	AND
	AS
	ASC
	ATTACH
	AUTOINCREMENT
	BEFORE
	BEGIN
	BETWEEN
	BY
	CASCADE
	CASE
	CAST
	CHECK
	COLLATE
	COLUMN
	COMMIT
	CONFLICT
	CONSTRAINT
	CREATE
	CROSS
	CURRENT
	CURRENT_DATE
	CURRENT_TIME
	CURRENT_TIMESTAMP
	DATABASE
	DEFAULT
	DEFERRABLE
	DEFERRED
	DELETE
	DESC
	DETACH
	DISTINCT
	DO
	DROP
	EACH
	ELSE
	END
	ESCAPE
	EXCEPT
	EXCLUDE
	EXCLUSIVE
	EXISTS
	EXPLAIN
	FAIL
	FILTER
	FIRST
	FOLLOWING
	FOR
	FOREIGN
	FROM
	FULL
	GENERATED
	GLOB
	GROUP
	GROUPS
	HAVING
	IF
	IGNORE
	IMMEDIATE
	IN
	INDEX
	INDEXED
	INITIALLY
	INNER
	INSERT
	INSTEAD
	INTERSECT
	INTO
	IS
	ISNULL
	JOIN
	KEY
	LAST
	LEFT
	LIKE
	LIMIT
	MATCH
	MATERIALIZED
	NATURAL
	NO
	NOT
	NOTHING
	NOTNULL
	NULLS
	OF
	OFFSET
	ON
	OR
	ORDER
	OTHERS
	OUTER
	OVER
	PARTITION
	PLAN
	PRAGMA
	PRECEDING
	PRIMARY
	QUERY
	RAISE
	RANGE
	RECURSIVE
	REFERENCES
	REGEXP
	REINDEX
	RELEASE
	RENAME
	REPLACE
	RESTRICT
	RETURNING
	RIGHT
	ROLLBACK
	ROW
	ROWS
	SAVEPOINT
	SELECT
	SET
	TABLE
	TEMP
	TEMPORARY
	THEN
	TIES
	TO
	TRANSACTION
	TRIGGER
	UNBOUNDED
	UNION
	UNIQUE
	UPDATE
	USING
	VACUUM
	VALUES
	VIEW
	VIRTUAL
	WHEN
	WHERE
	WINDOW
	WITH
	WITHOUT

	// sepcial keywords not in https://sqlite.org/lang_keywords.html
	STRICT
	ROWID
	STORED
)

var tokens = [...]string{
	// Special tokens
	ILLEGAL: "ILLEGAL",
	EOF:     "EOF",
	COMMENT: "COMMENT",
	SPACE:   "SPACE",

	// literals
	IDENT:   "IDENT",   // IDENT
	QIDENT:  "QIDENT",  // "IDENT"
	STRING:  "STRING",  // 'string'
	BLOB:    "BLOB",    // ???
	FLOAT:   "FLOAT",   // 123.45
	INTEGER: "INTEGER", // 123
	NULL:    "NULL",    // NULL
	TRUE:    "TRUE",    // true
	FALSE:   "FALSE",   // false
	BIND:    "BIND",    //? or ?NNN or :VVV or @VVV or $VVV

	// operators and delimiters
	SEMI:              ";",   // ;
	LP:                "(",   // (
	RP:                ")",   // )
	COMMA:             ",",   // ,
	NE:                "!=",  // !=
	EQ:                "=",   // =
	LE:                "<=",  // <=
	LT:                "<",   // <
	GT:                ">",   // >
	GE:                ">=",  // >=
	BITAND:            "&",   // &
	BITOR:             "|",   // |
	BITNOT:            "~",   // ~
	LSHIFT:            "<<",  // <<
	RSHIFT:            ">>",  // >>
	PLUS:              "+",   // +
	MINUS:             "-",   // -
	STAR:              "*",   // *
	SLASH:             "/",   // /
	REM:               "%",   // %
	CONCAT:            "||",  // ||
	DOT:               ".",   // .
	JSON_EXTRACT_JSON: "->",  // ->
	JSON_EXTRACT_SQL:  "->>", // ->>

	// keywords
	ABORT:             "ABORT",
	ACTION:            "ACTION",
	ADD:               "ADD",
	AFTER:             "AFTER",
	ALL:               "ALL",
	ALTER:             "ALTER",
	ALWAYS:            "ALWAYS",
	ANALYZE:           "ANALYZE",
	AND:               "AND",
	AS:                "AS",
	ASC:               "ASC",
	ATTACH:            "ATTACH",
	AUTOINCREMENT:     "AUTOINCREMENT",
	BEFORE:            "BEFORE",
	BEGIN:             "BEGIN",
	BETWEEN:           "BETWEEN",
	BY:                "BY",
	CASCADE:           "CASCADE",
	CASE:              "CASE",
	CAST:              "CAST",
	CHECK:             "CHECK",
	COLLATE:           "COLLATE",
	COLUMN:            "COLUMN",
	COMMIT:            "COMMIT",
	CONFLICT:          "CONFLICT",
	CONSTRAINT:        "CONSTRAINT",
	CREATE:            "CREATE",
	CROSS:             "CROSS",
	CURRENT:           "CURRENT",
	CURRENT_DATE:      "CURRENT_DATE",
	CURRENT_TIME:      "CURRENT_TIME",
	CURRENT_TIMESTAMP: "CURRENT_TIMESTAMP",
	DATABASE:          "DATABASE",
	DEFAULT:           "DEFAULT",
	DEFERRABLE:        "DEFERRABLE",
	DEFERRED:          "DEFERRED",
	DELETE:            "DELETE",
	DESC:              "DESC",
	DETACH:            "DETACH",
	DISTINCT:          "DISTINCT",
	DO:                "DO",
	DROP:              "DROP",
	EACH:              "EACH",
	ELSE:              "ELSE",
	END:               "END",
	ESCAPE:            "ESCAPE",
	EXCEPT:            "EXCEPT",
	EXCLUDE:           "EXCLUDE",
	EXCLUSIVE:         "EXCLUSIVE",
	EXISTS:            "EXISTS",
	EXPLAIN:           "EXPLAIN",
	FAIL:              "FAIL",
	FILTER:            "FILTER",
	FIRST:             "FIRST",
	FOLLOWING:         "FOLLOWING",
	FOR:               "FOR",
	FOREIGN:           "FOREIGN",
	FROM:              "FROM",
	FULL:              "FULL",
	GENERATED:         "GENERATED",
	GLOB:              "GLOB",
	GROUP:             "GROUP",
	GROUPS:            "GROUPS",
	HAVING:            "HAVING",
	IF:                "IF",
	IGNORE:            "IGNORE",
	IMMEDIATE:         "IMMEDIATE",
	IN:                "IN",
	INDEX:             "INDEX",
	INDEXED:           "INDEXED",
	INITIALLY:         "INITIALLY",
	INNER:             "INNER",
	INSERT:            "INSERT",
	INSTEAD:           "INSTEAD",
	INTERSECT:         "INTERSECT",
	INTO:              "INTO",
	IS:                "IS",
	ISNULL:            "ISNULL",
	JOIN:              "JOIN",
	KEY:               "KEY",
	LAST:              "LAST",
	LEFT:              "LEFT",
	LIKE:              "LIKE",
	LIMIT:             "LIMIT",
	MATCH:             "MATCH",
	MATERIALIZED:      "MATERIALIZED",
	NATURAL:           "NATURAL",
	NO:                "NO",
	NOT:               "NOT",
	NOTHING:           "NOTHING",
	NOTNULL:           "NOTNULL",
	NULLS:             "NULLS",
	OF:                "OF",
	OFFSET:            "OFFSET",
	ON:                "ON",
	OR:                "OR",
	ORDER:             "ORDER",
	OTHERS:            "OTHERS",
	OUTER:             "OUTER",
	OVER:              "OVER",
	PARTITION:         "PARTITION",
	PLAN:              "PLAN",
	PRAGMA:            "PRAGMA",
	PRECEDING:         "PRECEDING",
	PRIMARY:           "PRIMARY",
	QUERY:             "QUERY",
	RAISE:             "RAISE",
	RANGE:             "RANGE",
	RECURSIVE:         "RECURSIVE",
	REFERENCES:        "REFERENCES",
	REGEXP:            "REGEXP",
	REINDEX:           "REINDEX",
	RELEASE:           "RELEASE",
	RENAME:            "RENAME",
	REPLACE:           "REPLACE",
	RESTRICT:          "RESTRICT",
	RETURNING:         "RETURNING",
	RIGHT:             "RIGHT",
	ROLLBACK:          "ROLLBACK",
	ROW:               "ROW",
	ROWS:              "ROWS",
	SAVEPOINT:         "SAVEPOINT",
	SELECT:            "SELECT",
	SET:               "SET",
	TABLE:             "TABLE",
	TEMP:              "TEMP",
	TEMPORARY:         "TEMPORARY",
	THEN:              "THEN",
	TIES:              "TIES",
	TO:                "TO",
	TRANSACTION:       "TRANSACTION",
	TRIGGER:           "TRIGGER",
	UNBOUNDED:         "UNBOUNDED",
	UNION:             "UNION",
	UNIQUE:            "UNIQUE",
	UPDATE:            "UPDATE",
	USING:             "USING",
	VACUUM:            "VACUUM",
	VALUES:            "VALUES",
	VIEW:              "VIEW",
	VIRTUAL:           "VIRTUAL",
	WHEN:              "WHEN",
	WHERE:             "WHERE",
	WINDOW:            "WINDOW",
	WITH:              "WITH",
	WITHOUT:           "WITHOUT",

	// sepcial keywords not in https://sqlite.org/lang_keywords.html
	STRICT: "STRICT",
	ROWID:  "ROWID",
	STORED: "STORED",
}

// A list of keywords that can be used as unquoted identifiers.
var bareTokens = [...]Token{
	ABORT, ACTION, AFTER, ALWAYS, ANALYZE, ASC, ATTACH, BEFORE, BEGIN, BY,
	CASCADE, CAST, COLUMN, CONFLICT, CROSS, CURRENT, CURRENT_DATE,
	CURRENT_TIME, CURRENT_TIMESTAMP, DATABASE, DEFERRED, DESC, DETACH, DO,
	EACH, END, EXCLUDE, EXCLUSIVE, EXPLAIN, FAIL, FILTER, FIRST, FOLLOWING,
	FOR, GENERATED, GLOB, GROUPS, IF, IGNORE, IMMEDIATE, INDEXED, INITIALLY,
	INNER, INSTEAD, KEY, LAST, LEFT, LIKE, MATCH, NATURAL, NO, NULLS, OF,
	OFFSET, OTHERS, OUTER, OVER, PARTITION, PLAN, PRAGMA, PRECEDING, QUERY,
	RAISE, RANGE, RECURSIVE, REGEXP, REINDEX, RELEASE, RENAME, REPLACE,
	RESTRICT, ROLLBACK, ROW, ROWS, SAVEPOINT, TEMP, TEMPORARY, TIES, TRIGGER,
	UNBOUNDED, VACUUM, VIEW, VIRTUAL, WINDOW, WITH, WITHOUT, STRICT, ROWID, STORED,
}

func (tok Token) String() string {
	s := ""
	if 0 <= tok && tok < Token(len(tokens)) {
		s = tokens[tok]
	}
	if s == "" {
		s = "token(" + strconv.Itoa(int(tok)) + ")"
	}
	return s
}

// isBareToken returns true if keyword token can be used as an identifier.
func isBareToken(tok Token) bool {
	_, ok := bareTokensMap[tok]
	return ok
}

func isIdentToken(tok Token) bool {
	return tok == IDENT || tok == QIDENT || tok == STRING || isBareToken(tok)
}

// isExprIdentToken returns true if tok can be used as an identifier in an expression.
// It includes IDENT, QIDENT, and certain keywords.
func isExprIdentToken(tok Token) bool {
	switch tok {
	// List keywords that can be used as identifiers in expressions for pragma
	case ON, FULL, DELETE:
		return true
	// Add any other non-reserved keywords here
	default:
		return isIdentToken(tok)
	}
}

func assert(condition bool) {
	if !condition {
		panic("assert failed")
	}
}

type OpType int

const (
	OP_ILLEGAL OpType = iota
	OP_OR
	OP_AND
	OP_NOT
	OP_ISNULL
	OP_NOTNULL
	OP_IN
	OP_NOT_IN
	OP_MATCH
	OP_NOT_MATCH
	OP_LIKE
	OP_NOT_LIKE
	OP_REGEXP
	OP_NOT_REGEXP
	OP_GLOB
	OP_NOT_GLOB
	OP_BETWEEN
	OP_NOT_BETWEEN
	OP_IS_DISTINCT_FROM
	OP_IS_NOT_DISTINCT_FROM
	OP_EQ
	OP_NE
	OP_IS
	OP_IS_NOT
	OP_LT
	OP_LE
	OP_GT
	OP_GE
	OP_ESCAPE
	OP_BITAND
	OP_BITOR
	OP_LSHIFT
	OP_RSHIFT
	OP_PLUS
	OP_MINUS
	OP_MULTIPLY
	OP_DIVIDE
	OP_MODULO
	OP_CONCAT
	OP_JSON_EXTRACT_JSON
	OP_JSON_EXTRACT_SQL
	OP_COLLATE
	OP_BITNOT
)

func (op OpType) Precedence() int {
	switch {
	case op < OP_OR || op > OP_BITNOT:
		return 0
	case op == OP_OR:
		return 1
	case op == OP_AND:
		return 2
	case op <= OP_IS_NOT:
		return 3
	case op <= OP_GE:
		return 4
	case op <= OP_ESCAPE:
		return 5
	case op <= OP_RSHIFT:
		return 6
	case op <= OP_MINUS:
		return 7
	case op <= OP_MODULO:
		return 8
	case op <= OP_JSON_EXTRACT_SQL:
		return 9
	case op <= OP_COLLATE:
		return 10
	default:
		return 11
	}
}

func precedenceByStartBinaryOp(tok Token) int {
	switch tok {
	case PLUS:
		return OP_PLUS.Precedence()
	case MINUS:
		return OP_MINUS.Precedence()
	case STAR:
		return OP_MULTIPLY.Precedence()
	case SLASH:
		return OP_DIVIDE.Precedence()
	case REM:
		return OP_MODULO.Precedence()
	case CONCAT:
		return OP_CONCAT.Precedence()
	case BETWEEN:
		return OP_BETWEEN.Precedence()
	case LSHIFT:
		return OP_LSHIFT.Precedence()
	case RSHIFT:
		return OP_RSHIFT.Precedence()
	case BITAND:
		return OP_BITAND.Precedence()
	case BITOR:
		return OP_BITOR.Precedence()
	case LT:
		return OP_LT.Precedence()
	case LE:
		return OP_LE.Precedence()
	case GT:
		return OP_GT.Precedence()
	case GE:
		return OP_GE.Precedence()
	case EQ:
		return OP_EQ.Precedence()
	case NE:
		return OP_NE.Precedence()
	case JSON_EXTRACT_JSON:
		return OP_JSON_EXTRACT_JSON.Precedence()
	case JSON_EXTRACT_SQL:
		return OP_JSON_EXTRACT_SQL.Precedence()
	case IN:
		return OP_IN.Precedence()
	case LIKE:
		return OP_LIKE.Precedence()
	case GLOB:
		return OP_GLOB.Precedence()
	case MATCH:
		return OP_MATCH.Precedence()
	case REGEXP:
		return OP_REGEXP.Precedence()
	case AND:
		return OP_AND.Precedence()
	case OR:
		return OP_OR.Precedence()
	case ISNULL:
		return OP_ISNULL.Precedence()
	case NOTNULL:
		return OP_NOTNULL.Precedence()
	case ESCAPE:
		return OP_ESCAPE.Precedence()
	case COLLATE:
		return OP_COLLATE.Precedence()
	case IS:
		return OP_IS.Precedence()
	case NOT:
		return OP_NOTNULL.Precedence()
	default:
		return 0
	}
}
