package sql_test

import (
	"testing"

	"github.com/TcMits/sql"
)

func TestAlterTableStatement_String(t *testing.T) {
	AssertStatementStringer(t, &sql.AlterTableStatement{
		Name:    &sql.QualifiedName{Name: &sql.Ident{Name: "foo"}},
		NewName: &sql.Ident{Name: "bar"},
	}, `ALTER TABLE "foo" RENAME TO "bar"`)

	AssertStatementStringer(t, &sql.AlterTableStatement{
		Name:          &sql.QualifiedName{Name: &sql.Ident{Name: "foo"}},
		ColumnName:    &sql.Ident{Name: "col1"},
		NewColumnName: &sql.Ident{Name: "col2"},
	}, `ALTER TABLE "foo" RENAME COLUMN "col1" TO "col2"`)

	AssertStatementStringer(t, &sql.AlterTableStatement{
		Name: &sql.QualifiedName{Name: &sql.Ident{Name: "foo"}},
		ColumnDef: &sql.ColumnDefinition{
			Name: &sql.Ident{Name: "bar"},
			Type: &sql.Type{Name: &sql.Ident{Name: "INTEGER"}},
		},
	}, `ALTER TABLE "foo" ADD COLUMN "bar" INTEGER`)
}

func TestAnalyzeStatement_String(t *testing.T) {
	AssertStatementStringer(t, &sql.AnalyzeStatement{Name: &sql.QualifiedName{Name: &sql.Ident{Name: "foo"}}}, `ANALYZE "foo"`)
}

func TestBeginStatement_String(t *testing.T) {
	AssertStatementStringer(t, &sql.BeginStatement{}, `BEGIN`)
	AssertStatementStringer(t, &sql.BeginStatement{Deferred: pos(0)}, `BEGIN DEFERRED`)
	AssertStatementStringer(t, &sql.BeginStatement{Immediate: pos(0)}, `BEGIN IMMEDIATE`)
	AssertStatementStringer(t, &sql.BeginStatement{Exclusive: pos(0)}, `BEGIN EXCLUSIVE`)
	AssertStatementStringer(t, &sql.BeginStatement{Immediate: pos(0)}, `BEGIN IMMEDIATE`)
}

func TestCommitStatement_String(t *testing.T) {
	AssertStatementStringer(t, &sql.CommitStatement{}, `COMMIT`)
}

func TestCreateIndexStatement_String(t *testing.T) {
	AssertStatementStringer(t, &sql.CreateIndexStatement{
		Name:    &sql.QualifiedName{Name: &sql.Ident{Name: "foo"}},
		Table:   &sql.Ident{Name: "bar"},
		Columns: []*sql.IndexedColumn{{X: &sql.Ident{Name: "baz"}}},
	}, `CREATE INDEX "foo" ON "bar" ("baz")`)

	AssertStatementStringer(t, &sql.CreateIndexStatement{
		Unique:      pos(0),
		IfNotExists: pos(0),
		Name:        &sql.QualifiedName{Name: &sql.Ident{Name: "foo"}},
		Table:       &sql.Ident{Name: "bar"},
		Columns: []*sql.IndexedColumn{
			{X: &sql.Ident{Name: "baz"}},
			{X: &sql.Ident{Name: "bat"}},
		},
		WhereExpr: &sql.BoolLit{Value: true},
	}, `CREATE UNIQUE INDEX IF NOT EXISTS "foo" ON "bar" ("baz", "bat") WHERE TRUE`)
}

func TestCreateTableStatement_String(t *testing.T) {
	AssertStatementStringer(t, &sql.CreateTableStatement{
		Name:        &sql.QualifiedName{Name: &sql.Ident{Name: "foo"}},
		IfNotExists: pos(0),
		Columns: []*sql.ColumnDefinition{
			{
				Name: &sql.Ident{Name: "bar"},
				Type: &sql.Type{Name: &sql.Ident{Name: "INTEGER"}},
			},
			{
				Name: &sql.Ident{Name: "baz"},
				Type: &sql.Type{Name: &sql.Ident{Name: "TEXT"}},
			},
			{
				Name: &sql.Ident{Name: "no_type"},
			},
		},
	}, `CREATE TABLE IF NOT EXISTS "foo" ("bar" INTEGER, "baz" TEXT, "no_type")`)

	AssertStatementStringer(t, &sql.CreateTableStatement{
		Name: &sql.QualifiedName{Name: &sql.Ident{Name: "foo"}},
		Columns: []*sql.ColumnDefinition{{
			Name: &sql.Ident{Name: "bar"},
			Type: &sql.Type{Name: &sql.Ident{Name: "INTEGER"}},
			Constraints: []sql.Constraint{
				&sql.PrimaryKeyConstraint{Autoincrement: pos(0)},
				&sql.NotNullConstraint{Name: &sql.Ident{Name: "nn"}},
				&sql.DefaultConstraint{Name: &sql.Ident{Name: "def"}, Expr: &sql.NumberLit{Value: "123"}},
				&sql.DefaultConstraint{Expr: &sql.NumberLit{Value: "456"}},
				&sql.UniqueConstraint{},
			},
		}},
	}, `CREATE TABLE "foo" ("bar" INTEGER PRIMARY KEY AUTOINCREMENT CONSTRAINT "nn" NOT NULL CONSTRAINT "def" DEFAULT 123 DEFAULT 456 UNIQUE)`)

	AssertStatementStringer(t, &sql.CreateTableStatement{
		Name: &sql.QualifiedName{Name: &sql.Ident{Name: "foo"}},
		Columns: []*sql.ColumnDefinition{{
			Name: &sql.Ident{Name: "bar"},
			Type: &sql.Type{Name: &sql.Ident{Name: "INTEGER"}},
			Constraints: []sql.Constraint{
				&sql.ForeignKeyConstraint{
					ForeignTable:   &sql.Ident{Name: "x"},
					ForeignColumns: []*sql.Ident{{Name: "y"}},
					Args: []*sql.ForeignKeyArg{
						{OnDelete: pos(0), SetNull: pos(0)},
						{OnUpdate: pos(0), SetDefault: pos(0)},
						{OnUpdate: pos(0), Cascade: pos(0)},
						{OnUpdate: pos(0), Restrict: pos(0)},
						{OnUpdate: pos(0), NoAction: pos(0)},
					},
				},
			},
		}},
	}, `CREATE TABLE "foo" ("bar" INTEGER REFERENCES "x" ("y") ON DELETE SET NULL ON UPDATE SET DEFAULT ON UPDATE CASCADE ON UPDATE RESTRICT ON UPDATE NO ACTION)`)

	AssertStatementStringer(t, &sql.CreateTableStatement{
		Name: &sql.QualifiedName{Name: &sql.Ident{Name: "foo"}},
		Columns: []*sql.ColumnDefinition{{
			Name: &sql.Ident{Name: "bar"},
			Type: &sql.Type{Name: &sql.Ident{Name: "INTEGER"}},
			Constraints: []sql.Constraint{
				&sql.ForeignKeyConstraint{
					ForeignTable:      &sql.Ident{Name: "x"},
					ForeignColumns:    []*sql.Ident{{Name: "y"}},
					Deferrable:        pos(0),
					InitiallyDeferred: pos(0),
				},
			},
		}},
	}, `CREATE TABLE "foo" ("bar" INTEGER REFERENCES "x" ("y") DEFERRABLE INITIALLY DEFERRED)`)

	AssertStatementStringer(t, &sql.CreateTableStatement{
		Name: &sql.QualifiedName{Name: &sql.Ident{Name: "foo"}},
		Columns: []*sql.ColumnDefinition{{
			Name: &sql.Ident{Name: "bar"},
			Type: &sql.Type{Name: &sql.Ident{Name: "INTEGER"}},
			Constraints: []sql.Constraint{
				&sql.ForeignKeyConstraint{
					ForeignTable:       &sql.Ident{Name: "x"},
					ForeignColumns:     []*sql.Ident{{Name: "y"}},
					NotDeferrable:      pos(0),
					InitiallyImmediate: pos(0),
				},
			},
		}},
	}, `CREATE TABLE "foo" ("bar" INTEGER REFERENCES "x" ("y") NOT DEFERRABLE INITIALLY IMMEDIATE)`)

	AssertStatementStringer(t, &sql.CreateTableStatement{
		Name: &sql.QualifiedName{Name: &sql.Ident{Name: "foo"}},
		Columns: []*sql.ColumnDefinition{{
			Name: &sql.Ident{Name: "bar"},
			Type: &sql.Type{Name: &sql.Ident{Name: "DECIMAL"}, Precision: &sql.NumberLit{Value: "100"}},
		}},
		Constraints: []sql.Constraint{
			&sql.PrimaryKeyConstraint{
				Name: &sql.Ident{Name: "pk"},
				Columns: []*sql.Ident{
					{Name: "x"},
					{Name: "y"},
				},
			},
			&sql.UniqueConstraint{
				Name: &sql.Ident{Name: "uniq"},
				Columns: []*sql.IndexedColumn{
					{X: &sql.Ident{Name: "x"}},
					{X: &sql.Ident{Name: "y"}},
				},
			},
			&sql.CheckConstraint{
				Name: &sql.Ident{Name: "chk"},
				Expr: &sql.BoolLit{Value: true},
			},
		},
	}, `CREATE TABLE "foo" ("bar" DECIMAL(100), CONSTRAINT "pk" PRIMARY KEY ("x", "y"), CONSTRAINT "uniq" UNIQUE ("x", "y"), CONSTRAINT "chk" CHECK (TRUE))`)

	AssertStatementStringer(t, &sql.CreateTableStatement{
		Name: &sql.QualifiedName{Name: &sql.Ident{Name: "foo"}},
		Columns: []*sql.ColumnDefinition{{
			Name: &sql.Ident{Name: "bar"},
			Type: &sql.Type{Name: &sql.Ident{Name: "DECIMAL"}, Precision: &sql.NumberLit{Value: "100"}, Scale: &sql.NumberLit{Value: "200"}},
		}},
		Constraints: []sql.Constraint{
			&sql.ForeignKeyConstraint{
				Name:           &sql.Ident{Name: "fk"},
				Columns:        []*sql.Ident{{Name: "a"}, {Name: "b"}},
				ForeignTable:   &sql.Ident{Name: "x"},
				ForeignColumns: []*sql.Ident{{Name: "y"}, {Name: "z"}},
			},
		},
	}, `CREATE TABLE "foo" ("bar" DECIMAL(100,200), CONSTRAINT "fk" FOREIGN KEY ("a", "b") REFERENCES "x" ("y", "z"))`)

	AssertStatementStringer(t, &sql.CreateTableStatement{
		Name: &sql.QualifiedName{Name: &sql.Ident{Name: "foo"}},
		Select: &sql.SelectStatement{
			Columns: []*sql.ResultColumn{{Star: pos(0)}},
		},
	}, `CREATE TABLE "foo" AS SELECT *`)
}

func TestCreateTriggerStatement_String(t *testing.T) {
	AssertStatementStringer(t, &sql.CreateTriggerStatement{
		Name:   &sql.QualifiedName{Name: &sql.Ident{Name: "trig"}},
		Insert: pos(0),
		Table:  &sql.Ident{Name: "tbl"},
		Body: []sql.Statement{
			&sql.DeleteStatement{Table: &sql.QualifiedName{Name: &sql.Ident{Name: "tbl2"}}},
		},
	}, `CREATE TRIGGER "trig" INSERT ON "tbl" BEGIN DELETE FROM "tbl2"; END`)

	AssertStatementStringer(t, &sql.CreateTriggerStatement{
		Name:       &sql.QualifiedName{Name: &sql.Ident{Name: "trig"}},
		Before:     pos(0),
		Delete:     pos(0),
		ForEachRow: pos(0),
		Table:      &sql.Ident{Name: "tbl"},
		Body: []sql.Statement{
			&sql.DeleteStatement{Table: &sql.QualifiedName{Name: &sql.Ident{Name: "x"}}},
		},
	}, `CREATE TRIGGER "trig" BEFORE DELETE ON "tbl" FOR EACH ROW BEGIN DELETE FROM "x"; END`)

	AssertStatementStringer(t, &sql.CreateTriggerStatement{
		IfNotExists: pos(0),
		Name:        &sql.QualifiedName{Name: &sql.Ident{Name: "trig"}},
		After:       pos(0),
		Update:      pos(0),
		Table:       &sql.Ident{Name: "tbl"},
		WhenExpr:    &sql.BoolLit{Value: true},
		Body: []sql.Statement{
			&sql.DeleteStatement{Table: &sql.QualifiedName{Name: &sql.Ident{Name: "x"}}},
		},
	}, `CREATE TRIGGER IF NOT EXISTS "trig" AFTER UPDATE ON "tbl" WHEN TRUE BEGIN DELETE FROM "x"; END`)

	AssertStatementStringer(t, &sql.CreateTriggerStatement{
		Name:            &sql.QualifiedName{Name: &sql.Ident{Name: "trig"}},
		InsteadOf:       pos(0),
		Update:          pos(0),
		UpdateOfColumns: []*sql.Ident{{Name: "x"}, {Name: "y"}},
		Table:           &sql.Ident{Name: "tbl"},
		Body: []sql.Statement{
			&sql.DeleteStatement{Table: &sql.QualifiedName{Name: &sql.Ident{Name: "x"}}},
		},
	}, `CREATE TRIGGER "trig" INSTEAD OF UPDATE OF "x", "y" ON "tbl" BEGIN DELETE FROM "x"; END`)
}

func TestCreateViewStatement_String(t *testing.T) {
	AssertStatementStringer(t, &sql.CreateViewStatement{
		Name: &sql.QualifiedName{Name: &sql.Ident{Name: "vw"}},
		Columns: []*sql.Ident{
			{Name: "x"},
			{Name: "y"},
		},
		Select: &sql.SelectStatement{
			Columns: []*sql.ResultColumn{{Star: pos(0)}},
		},
	}, `CREATE VIEW "vw" ("x", "y") AS SELECT *`)

	AssertStatementStringer(t, &sql.CreateViewStatement{
		IfNotExists: pos(0),
		Name:        &sql.QualifiedName{Name: &sql.Ident{Name: "vw"}},
		Select: &sql.SelectStatement{
			Columns: []*sql.ResultColumn{{Star: pos(0)}},
		},
	}, `CREATE VIEW IF NOT EXISTS "vw" AS SELECT *`)
}

func TestDeleteStatement_String(t *testing.T) {
	AssertStatementStringer(t, &sql.DeleteStatement{
		Table: &sql.QualifiedName{Name: &sql.Ident{Name: "tbl"}, Alias: &sql.Ident{Name: "tbl2"}},
	}, `DELETE FROM "tbl" AS "tbl2"`)

	AssertStatementStringer(t, &sql.DeleteStatement{
		Table: &sql.QualifiedName{Name: &sql.Ident{Name: "tbl"}, Index: &sql.Ident{Name: "idx"}},
	}, `DELETE FROM "tbl" INDEXED BY "idx"`)

	AssertStatementStringer(t, &sql.DeleteStatement{
		Table: &sql.QualifiedName{Name: &sql.Ident{Name: "tbl"}, NotIndexed: pos(0)},
	}, `DELETE FROM "tbl" NOT INDEXED`)

	AssertStatementStringer(t, &sql.DeleteStatement{
		Table:     &sql.QualifiedName{Name: &sql.Ident{Name: "tbl"}},
		WhereExpr: &sql.BoolLit{Value: true},
		OrderingTerms: []*sql.OrderingTerm{
			{X: &sql.Ident{Name: "x"}},
			{X: &sql.Ident{Name: "y"}},
		},
		LimitExpr:  &sql.NumberLit{Value: "10"},
		OffsetExpr: &sql.NumberLit{Value: "5"},
	}, `DELETE FROM "tbl" WHERE TRUE ORDER BY "x", "y" LIMIT 10 OFFSET 5`)

	AssertStatementStringer(t, &sql.DeleteStatement{
		WithClause: &sql.WithClause{
			Recursive: pos(0),
			CTEs: []*sql.CTE{{
				TableName: &sql.Ident{Name: "cte"},
				Select: &sql.SelectStatement{
					Columns: []*sql.ResultColumn{{Star: pos(0)}},
				},
			}},
		},
		Table: &sql.QualifiedName{Name: &sql.Ident{Name: "tbl"}},
	}, `WITH RECURSIVE "cte" AS (SELECT *) DELETE FROM "tbl"`)
}

func TestDropIndexStatement_String(t *testing.T) {
	AssertStatementStringer(t, &sql.DropIndexStatement{
		Name: &sql.QualifiedName{Name: &sql.Ident{Name: "idx"}},
	}, `DROP INDEX "idx"`)

	AssertStatementStringer(t, &sql.DropIndexStatement{
		IfExists: pos(0),
		Name:     &sql.QualifiedName{Name: &sql.Ident{Name: "idx"}},
	}, `DROP INDEX IF EXISTS "idx"`)
}

func TestDropTableStatement_String(t *testing.T) {
	AssertStatementStringer(t, &sql.DropTableStatement{
		Name: &sql.QualifiedName{Name: &sql.Ident{Name: "tbl"}},
	}, `DROP TABLE "tbl"`)

	AssertStatementStringer(t, &sql.DropTableStatement{
		IfExists: pos(0),
		Name:     &sql.QualifiedName{Name: &sql.Ident{Name: "tbl"}},
	}, `DROP TABLE IF EXISTS "tbl"`)
}

func TestDropTriggerStatement_String(t *testing.T) {
	AssertStatementStringer(t, &sql.DropTriggerStatement{
		Name: &sql.QualifiedName{Name: &sql.Ident{Name: "trig"}},
	}, `DROP TRIGGER "trig"`)

	AssertStatementStringer(t, &sql.DropTriggerStatement{
		IfExists: pos(0),
		Name:     &sql.QualifiedName{Name: &sql.Ident{Name: "trig"}},
	}, `DROP TRIGGER IF EXISTS "trig"`)
}

func TestDropViewStatement_String(t *testing.T) {
	AssertStatementStringer(t, &sql.DropViewStatement{
		Name: &sql.QualifiedName{Name: &sql.Ident{Name: "vw"}},
	}, `DROP VIEW "vw"`)

	AssertStatementStringer(t, &sql.DropViewStatement{
		IfExists: pos(0),
		Name:     &sql.QualifiedName{Name: &sql.Ident{Name: "vw"}},
	}, `DROP VIEW IF EXISTS "vw"`)
}

func TestExplainStatement_String(t *testing.T) {
	AssertStatementStringer(t, &sql.ExplainStatement{
		Stmt: &sql.DropViewStatement{
			Name: &sql.QualifiedName{Name: &sql.Ident{Name: "vw"}},
		},
	}, `EXPLAIN DROP VIEW "vw"`)

	AssertStatementStringer(t, &sql.ExplainStatement{
		QueryPlan: pos(0),
		Stmt: &sql.DropViewStatement{
			Name: &sql.QualifiedName{Name: &sql.Ident{Name: "vw"}},
		},
	}, `EXPLAIN QUERY PLAN DROP VIEW "vw"`)
}

func TestInsertStatement_String(t *testing.T) {
	AssertStatementStringer(t, &sql.InsertStatement{
		Table:         &sql.QualifiedName{Name: &sql.Ident{Name: "tbl"}},
		DefaultValues: pos(0),
	}, `INSERT INTO "tbl" DEFAULT VALUES`)

	AssertStatementStringer(t, &sql.InsertStatement{
		Table: &sql.QualifiedName{
			Name:  &sql.Ident{Name: "tbl"},
			Alias: &sql.Ident{Name: "x"},
		},
		DefaultValues: pos(0),
	}, `INSERT INTO "tbl" AS "x" DEFAULT VALUES`)

	AssertStatementStringer(t, &sql.InsertStatement{
		InsertOrReplace: pos(0),
		Table:           &sql.QualifiedName{Name: &sql.Ident{Name: "tbl"}},
		DefaultValues:   pos(0),
	}, `INSERT OR REPLACE INTO "tbl" DEFAULT VALUES`)

	AssertStatementStringer(t, &sql.InsertStatement{
		InsertOrRollback: pos(0),
		Table:            &sql.QualifiedName{Name: &sql.Ident{Name: "tbl"}},
		DefaultValues:    pos(0),
	}, `INSERT OR ROLLBACK INTO "tbl" DEFAULT VALUES`)

	AssertStatementStringer(t, &sql.InsertStatement{
		InsertOrAbort: pos(0),
		Table:         &sql.QualifiedName{Name: &sql.Ident{Name: "tbl"}},
		DefaultValues: pos(0),
	}, `INSERT OR ABORT INTO "tbl" DEFAULT VALUES`)

	AssertStatementStringer(t, &sql.InsertStatement{
		InsertOrFail:  pos(0),
		Table:         &sql.QualifiedName{Name: &sql.Ident{Name: "tbl"}},
		DefaultValues: pos(0),
	}, `INSERT OR FAIL INTO "tbl" DEFAULT VALUES`)

	AssertStatementStringer(t, &sql.InsertStatement{
		InsertOrIgnore: pos(0),
		Table:          &sql.QualifiedName{Name: &sql.Ident{Name: "tbl"}},
		DefaultValues:  pos(0),
	}, `INSERT OR IGNORE INTO "tbl" DEFAULT VALUES`)

	AssertStatementStringer(t, &sql.InsertStatement{
		Replace:       pos(0),
		Table:         &sql.QualifiedName{Name: &sql.Ident{Name: "tbl"}},
		DefaultValues: pos(0),
	}, `REPLACE INTO "tbl" DEFAULT VALUES`)

	AssertStatementStringer(t, &sql.InsertStatement{
		Table: &sql.QualifiedName{Name: &sql.Ident{Name: "tbl"}},
		Select: &sql.SelectStatement{
			Columns: []*sql.ResultColumn{{Star: pos(0)}},
		},
	}, `INSERT INTO "tbl" SELECT *`)

	AssertStatementStringer(t, &sql.InsertStatement{
		Table: &sql.QualifiedName{Name: &sql.Ident{Name: "tbl"}},
		Columns: []*sql.Ident{
			{Name: "x"},
			{Name: "y"},
		},
		ValueLists: []*sql.ExprList{
			{Exprs: []sql.Expr{&sql.NullLit{}, &sql.NullLit{}}},
			{Exprs: []sql.Expr{&sql.NullLit{}, &sql.NullLit{}}},
		},
	}, `INSERT INTO "tbl" ("x", "y") VALUES (NULL, NULL), (NULL, NULL)`)

	AssertStatementStringer(t, &sql.InsertStatement{
		Table: &sql.QualifiedName{Name: &sql.Ident{Name: "tbl"}},
		Columns: []*sql.Ident{
			{Name: "x"},
			{Name: "y"},
		},
		ValueLists: []*sql.ExprList{
			{Exprs: []sql.Expr{&sql.NumberLit{Value: "1"}, &sql.NumberLit{Value: "2"}}},
		},
		ReturningColumns: []*sql.ResultColumn{{Star: pos(41)}},
	}, `INSERT INTO "tbl" ("x", "y") VALUES (1, 2) RETURNING *`)

	AssertStatementStringer(t, &sql.InsertStatement{
		WithClause: &sql.WithClause{
			CTEs: []*sql.CTE{
				{
					TableName: &sql.Ident{Name: "cte"},
					Select: &sql.SelectStatement{
						Columns: []*sql.ResultColumn{{Star: pos(0)}},
					},
				},
				{
					TableName: &sql.Ident{Name: "cte2"},
					Select: &sql.SelectStatement{
						Columns: []*sql.ResultColumn{{Star: pos(0)}},
					},
				},
			},
		},
		Table:         &sql.QualifiedName{Name: &sql.Ident{Name: "tbl"}},
		DefaultValues: pos(0),
	}, `WITH "cte" AS (SELECT *), "cte2" AS (SELECT *) INSERT INTO "tbl" DEFAULT VALUES`)

	AssertStatementStringer(t, &sql.InsertStatement{
		Table:         &sql.QualifiedName{Name: &sql.Ident{Name: "tbl"}},
		DefaultValues: pos(0),
		UpsertClause: &sql.UpsertClause{
			DoNothing: pos(0),
		},
	}, `INSERT INTO "tbl" DEFAULT VALUES ON CONFLICT DO NOTHING`)

	AssertStatementStringer(t, &sql.InsertStatement{
		Table:         &sql.QualifiedName{Name: &sql.Ident{Name: "tbl"}},
		DefaultValues: pos(0),
		UpsertClause: &sql.UpsertClause{
			Columns: []*sql.IndexedColumn{
				{X: &sql.Ident{Name: "x"}, Asc: pos(0)},
				{X: &sql.Ident{Name: "y"}, Desc: pos(0)},
			},
			WhereExpr: &sql.BoolLit{Value: true},
			Assignments: []*sql.Assignment{
				{Columns: []*sql.Ident{{Name: "x"}}, Expr: &sql.NumberLit{Value: "100"}},
				{Columns: []*sql.Ident{{Name: "y"}, {Name: "z"}}, Expr: &sql.NumberLit{Value: "200"}},
			},
			UpdateWhereExpr: &sql.BoolLit{Value: false},
		},
	}, `INSERT INTO "tbl" DEFAULT VALUES ON CONFLICT ("x" ASC, "y" DESC) WHERE TRUE DO UPDATE SET "x" = 100, ("y", "z") = 200 WHERE FALSE`)
}

func TestReleaseStatement_String(t *testing.T) {
	AssertStatementStringer(t, &sql.ReleaseStatement{Name: &sql.Ident{Name: "x"}}, `RELEASE "x"`)
}

func TestRollbackStatement_String(t *testing.T) {
	AssertStatementStringer(t, &sql.RollbackStatement{}, `ROLLBACK`)
	AssertStatementStringer(t, &sql.RollbackStatement{SavepointName: &sql.Ident{Name: "x"}}, `ROLLBACK TO "x"`)
}

func TestSavepointStatement_String(t *testing.T) {
	AssertStatementStringer(t, &sql.SavepointStatement{Name: &sql.Ident{Name: "x"}}, `SAVEPOINT "x"`)
}

func TestSelectStatement_String(t *testing.T) {
	AssertStatementStringer(t, &sql.SelectStatement{
		Columns: []*sql.ResultColumn{
			{Expr: &sql.Ident{Name: "x"}, Alias: &sql.Ident{Name: "y"}},
			{Expr: &sql.Ident{Name: "z"}},
		},
	}, `SELECT "x" AS "y", "z"`)

	AssertStatementStringer(t, &sql.SelectStatement{
		Distinct: pos(0),
		Columns: []*sql.ResultColumn{
			{Expr: &sql.Ident{Name: "x"}},
		},
	}, `SELECT DISTINCT "x"`)

	AssertStatementStringer(t, &sql.SelectStatement{
		All: pos(0),
		Columns: []*sql.ResultColumn{
			{Expr: &sql.Ident{Name: "x"}},
		},
	}, `SELECT ALL "x"`)

	AssertStatementStringer(t, &sql.SelectStatement{
		Columns:      []*sql.ResultColumn{{Star: pos(0)}},
		Source:       &sql.QualifiedName{Name: &sql.Ident{Name: "tbl"}},
		WhereExpr:    &sql.BoolLit{Value: true},
		GroupByExprs: []sql.Expr{&sql.Ident{Name: "x"}, &sql.Ident{Name: "y"}},
		HavingExpr:   &sql.Ident{Name: "z"},
	}, `SELECT * FROM "tbl" WHERE TRUE GROUP BY "x", "y" HAVING "z"`)

	AssertStatementStringer(t, &sql.SelectStatement{
		Columns: []*sql.ResultColumn{{Star: pos(0)}},
		Source: &sql.ParenSource{
			X:     &sql.SelectStatement{Columns: []*sql.ResultColumn{{Star: pos(0)}}},
			Alias: &sql.Ident{Name: "tbl"},
		},
	}, `SELECT * FROM (SELECT *) AS "tbl"`)

	AssertStatementStringer(t, &sql.SelectStatement{
		Columns: []*sql.ResultColumn{{Star: pos(0)}},
		Source: &sql.ParenSource{
			X: &sql.SelectStatement{Columns: []*sql.ResultColumn{{Star: pos(0)}}},
		},
	}, `SELECT * FROM (SELECT *)`)

	AssertStatementStringer(t, &sql.SelectStatement{
		Columns: []*sql.ResultColumn{{Star: pos(0)}},
		Source: &sql.QualifiedName{
			Name:         &sql.Ident{Name: "generate_series"},
			FunctionCall: true,
			FunctionArgs: []*sql.FunctionArg{
				{Expr: &sql.NumberLit{Value: "1"}},
				{Expr: &sql.NumberLit{Value: "3"}},
			},
		},
	}, `SELECT * FROM "generate_series"(1, 3)`)

	AssertStatementStringer(t, &sql.SelectStatement{
		Columns: []*sql.ResultColumn{{Star: pos(0)}},
		Source: &sql.QualifiedName{
			Name:         &sql.Ident{Name: "generate_series"},
			FunctionCall: true,
			FunctionArgs: []*sql.FunctionArg{
				{Expr: &sql.NumberLit{Value: "1"}},
				{Expr: &sql.NumberLit{Value: "3"}},
			},
			Alias: &sql.Ident{Name: "x"},
		},
	}, `SELECT * FROM "generate_series"(1, 3) AS "x"`)

	AssertStatementStringer(t, &sql.SelectStatement{
		Columns: []*sql.ResultColumn{{Star: pos(0)}},
		Source:  &sql.QualifiedName{Name: &sql.Ident{Name: "tbl"}},
		Windows: []*sql.Window{
			{
				Name: &sql.Ident{Name: "win1"},
				Definition: &sql.WindowDefinition{
					Base:       &sql.Ident{Name: "base"},
					Partitions: []sql.Expr{&sql.Ident{Name: "x"}, &sql.Ident{Name: "y"}},
					OrderingTerms: []*sql.OrderingTerm{
						{X: &sql.Ident{Name: "x"}, Asc: pos(0), NullsFirst: pos(0)},
						{X: &sql.Ident{Name: "y"}, Desc: pos(0), NullsLast: pos(0)},
					},
					Frame: &sql.FrameSpec{
						Range:      pos(0),
						UnboundedX: pos(0),
						PrecedingX: pos(0),
					},
				},
			},
			{
				Name: &sql.Ident{Name: "win2"},
				Definition: &sql.WindowDefinition{
					Base: &sql.Ident{Name: "base2"},
				},
			},
		},
	}, `SELECT * FROM "tbl" WINDOW "win1" AS ("base" PARTITION BY "x", "y" ORDER BY "x" ASC NULLS FIRST, "y" DESC NULLS LAST RANGE UNBOUNDED PRECEDING), "win2" AS ("base2")`)

	AssertStatementStringer(t, &sql.SelectStatement{
		WithClause: &sql.WithClause{
			CTEs: []*sql.CTE{{
				TableName: &sql.Ident{Name: "cte"},
				Columns: []*sql.Ident{
					{Name: "x"},
					{Name: "y"},
				},
				Select: &sql.SelectStatement{
					Columns: []*sql.ResultColumn{{Star: pos(0)}},
				},
			}},
		},
		ValueLists: []*sql.ExprList{
			{Exprs: []sql.Expr{&sql.NumberLit{Value: "1"}, &sql.NumberLit{Value: "2"}}},
			{Exprs: []sql.Expr{&sql.NumberLit{Value: "3"}, &sql.NumberLit{Value: "4"}}},
		},
	}, `WITH "cte" ("x", "y") AS (SELECT *) VALUES (1, 2), (3, 4)`)

	AssertStatementStringer(t, &sql.SelectStatement{
		Columns: []*sql.ResultColumn{{Star: pos(0)}},
		Compound: &sql.SelectStatement{
			Columns: []*sql.ResultColumn{{Star: pos(0)}},
		},
	}, `SELECT * UNION SELECT *`)

	AssertStatementStringer(t, &sql.SelectStatement{
		Columns:  []*sql.ResultColumn{{Star: pos(0)}},
		UnionAll: pos(0),
		Compound: &sql.SelectStatement{
			Columns: []*sql.ResultColumn{{Star: pos(0)}},
		},
	}, `SELECT * UNION ALL SELECT *`)

	AssertStatementStringer(t, &sql.SelectStatement{
		Columns:   []*sql.ResultColumn{{Star: pos(0)}},
		Intersect: pos(0),
		Compound: &sql.SelectStatement{
			Columns: []*sql.ResultColumn{{Star: pos(0)}},
		},
	}, `SELECT * INTERSECT SELECT *`)

	AssertStatementStringer(t, &sql.SelectStatement{
		Columns: []*sql.ResultColumn{{Star: pos(0)}},
		Except:  pos(0),
		Compound: &sql.SelectStatement{
			Columns: []*sql.ResultColumn{{Star: pos(0)}},
		},
	}, `SELECT * EXCEPT SELECT *`)

	AssertStatementStringer(t, &sql.SelectStatement{
		Columns: []*sql.ResultColumn{{Star: pos(0)}},
		OrderingTerms: []*sql.OrderingTerm{
			{X: &sql.Ident{Name: "x"}},
			{X: &sql.Ident{Name: "y"}},
		},
	}, `SELECT * ORDER BY "x", "y"`)

	AssertStatementStringer(t, &sql.SelectStatement{
		Columns:    []*sql.ResultColumn{{Star: pos(0)}},
		LimitExpr:  &sql.NumberLit{Value: "1"},
		OffsetExpr: &sql.NumberLit{Value: "2"},
	}, `SELECT * LIMIT 1 OFFSET 2`)

	AssertStatementStringer(t, &sql.SelectStatement{
		Columns: []*sql.ResultColumn{{Star: pos(0)}},
		Source: &sql.JoinClause{
			X:        &sql.QualifiedName{Name: &sql.Ident{Name: "x"}},
			Operator: &sql.JoinOperator{},
			Y:        &sql.QualifiedName{Name: &sql.Ident{Name: "y"}},
		},
	}, `SELECT * FROM "x", "y"`)

	AssertStatementStringer(t, &sql.SelectStatement{
		Columns: []*sql.ResultColumn{{Star: pos(0)}},
		Source: &sql.JoinClause{
			X:          &sql.QualifiedName{Name: &sql.Ident{Name: "x"}},
			Operator:   &sql.JoinOperator{},
			Y:          &sql.QualifiedName{Name: &sql.Ident{Name: "y"}},
			Constraint: &sql.OnConstraint{X: &sql.BoolLit{Value: true}},
		},
	}, `SELECT * FROM "x", "y" ON TRUE`)

	AssertStatementStringer(t, &sql.SelectStatement{
		Columns: []*sql.ResultColumn{{Star: pos(0)}},
		Source: &sql.JoinClause{
			X:        &sql.QualifiedName{Name: &sql.Ident{Name: "x"}},
			Operator: &sql.JoinOperator{Natural: pos(0), Inner: pos(0)},
			Y:        &sql.QualifiedName{Name: &sql.Ident{Name: "y"}},
			Constraint: &sql.UsingConstraint{
				Columns: []*sql.Ident{{Name: "a"}, {Name: "b"}},
			},
		},
	}, `SELECT * FROM "x" NATURAL INNER JOIN "y" USING ("a", "b")`)

	AssertStatementStringer(t, &sql.SelectStatement{
		Columns: []*sql.ResultColumn{{Star: pos(0)}},
		Source: &sql.JoinClause{
			X:        &sql.QualifiedName{Name: &sql.Ident{Name: "x"}},
			Operator: &sql.JoinOperator{Left: pos(0)},
			Y:        &sql.QualifiedName{Name: &sql.Ident{Name: "y"}},
		},
	}, `SELECT * FROM "x" LEFT JOIN "y"`)

	AssertStatementStringer(t, &sql.SelectStatement{
		Columns: []*sql.ResultColumn{{Star: pos(0)}},
		Source: &sql.JoinClause{
			X:        &sql.QualifiedName{Name: &sql.Ident{Name: "x"}},
			Operator: &sql.JoinOperator{Left: pos(0), Outer: pos(0)},
			Y:        &sql.QualifiedName{Name: &sql.Ident{Name: "y"}},
		},
	}, `SELECT * FROM "x" LEFT OUTER JOIN "y"`)

	AssertStatementStringer(t, &sql.SelectStatement{
		Columns: []*sql.ResultColumn{{Star: pos(0)}},
		Source: &sql.JoinClause{
			X:        &sql.QualifiedName{Name: &sql.Ident{Name: "x"}},
			Operator: &sql.JoinOperator{Cross: pos(0)},
			Y:        &sql.QualifiedName{Name: &sql.Ident{Name: "y"}},
		},
	}, `SELECT * FROM "x" CROSS JOIN "y"`)

	AssertStatementStringer(t, &sql.SelectStatement{
		Columns: []*sql.ResultColumn{
			{Star: pos(7)},
		},
		Source: &sql.JoinClause{
			X: &sql.QualifiedName{
				Name:  &sql.Ident{Name: "X"},
				Alias: &sql.Ident{Name: "a"},
			},
			Operator: &sql.JoinOperator{},
			Y: &sql.JoinClause{
				X: &sql.QualifiedName{
					Name:  &sql.Ident{Name: "Y"},
					Alias: &sql.Ident{Name: "b"},
				},
				Operator: &sql.JoinOperator{},
				Y: &sql.QualifiedName{
					Name:  &sql.Ident{Name: "Z"},
					Alias: &sql.Ident{Name: "c"},
				},
				Constraint: &sql.OnConstraint{
					X: &sql.BinaryExpr{
						X: &sql.QualifiedRef{
							Table:  &sql.QualifiedName{Name: &sql.Ident{Name: "b"}},
							Column: &sql.Ident{Name: "id"},
						},
						Op: sql.OP_EQ,
						Y: &sql.QualifiedRef{
							Table:  &sql.QualifiedName{Name: &sql.Ident{Name: "c"}},
							Column: &sql.Ident{Name: "id"},
						},
					},
				},
			},
			Constraint: &sql.OnConstraint{
				X: &sql.BinaryExpr{
					X: &sql.QualifiedRef{
						Table:  &sql.QualifiedName{Name: &sql.Ident{Name: "a"}},
						Column: &sql.Ident{Name: "id"},
					},
					Op: sql.OP_EQ,
					Y: &sql.QualifiedRef{
						Table:  &sql.QualifiedName{Name: &sql.Ident{Name: "b"}},
						Column: &sql.Ident{Name: "id"},
					},
				},
			},
		},
	}, `SELECT * FROM "X" AS "a", "Y" AS "b" ON "a"."id" = "b"."id", "Z" AS "c" ON "b"."id" = "c"."id"`)
}

func TestUpdateStatement_String(t *testing.T) {
	AssertStatementStringer(t, &sql.UpdateStatement{
		Table: &sql.QualifiedName{Name: &sql.Ident{Name: "tbl"}},
		Assignments: []*sql.Assignment{
			{Columns: []*sql.Ident{{Name: "x"}}, Expr: &sql.NumberLit{Value: "100"}},
			{Columns: []*sql.Ident{{Name: "y"}}, Expr: &sql.NumberLit{Value: "200"}},
		},
		WhereExpr: &sql.BoolLit{Value: true},
	}, `UPDATE "tbl" SET "x" = 100, "y" = 200 WHERE TRUE`)

	AssertStatementStringer(t, &sql.UpdateStatement{
		UpdateOrRollback: pos(0),
		Table:            &sql.QualifiedName{Name: &sql.Ident{Name: "tbl"}},
		Assignments: []*sql.Assignment{
			{Columns: []*sql.Ident{{Name: "x"}}, Expr: &sql.NumberLit{Value: "100"}},
		},
	}, `UPDATE OR ROLLBACK "tbl" SET "x" = 100`)

	AssertStatementStringer(t, &sql.UpdateStatement{
		UpdateOrAbort: pos(0),
		Table:         &sql.QualifiedName{Name: &sql.Ident{Name: "tbl"}},
		Assignments: []*sql.Assignment{
			{Columns: []*sql.Ident{{Name: "x"}}, Expr: &sql.NumberLit{Value: "100"}},
		},
	}, `UPDATE OR ABORT "tbl" SET "x" = 100`)

	AssertStatementStringer(t, &sql.UpdateStatement{
		UpdateOrReplace: pos(0),
		Table:           &sql.QualifiedName{Name: &sql.Ident{Name: "tbl"}},
		Assignments: []*sql.Assignment{
			{Columns: []*sql.Ident{{Name: "x"}}, Expr: &sql.NumberLit{Value: "100"}},
		},
	}, `UPDATE OR REPLACE "tbl" SET "x" = 100`)

	AssertStatementStringer(t, &sql.UpdateStatement{
		UpdateOrFail: pos(0),
		Table:        &sql.QualifiedName{Name: &sql.Ident{Name: "tbl"}},
		Assignments: []*sql.Assignment{
			{Columns: []*sql.Ident{{Name: "x"}}, Expr: &sql.NumberLit{Value: "100"}},
		},
	}, `UPDATE OR FAIL "tbl" SET "x" = 100`)

	AssertStatementStringer(t, &sql.UpdateStatement{
		UpdateOrIgnore: pos(0),
		Table:          &sql.QualifiedName{Name: &sql.Ident{Name: "tbl"}},
		Assignments: []*sql.Assignment{
			{Columns: []*sql.Ident{{Name: "x"}}, Expr: &sql.NumberLit{Value: "100"}},
		},
	}, `UPDATE OR IGNORE "tbl" SET "x" = 100`)

	AssertStatementStringer(t, &sql.UpdateStatement{
		WithClause: &sql.WithClause{
			CTEs: []*sql.CTE{{
				TableName: &sql.Ident{Name: "cte"},
				Select: &sql.SelectStatement{
					Columns: []*sql.ResultColumn{{Star: pos(0)}},
				},
			}},
		},
		Table: &sql.QualifiedName{Name: &sql.Ident{Name: "tbl"}},
		Assignments: []*sql.Assignment{
			{Columns: []*sql.Ident{{Name: "x"}}, Expr: &sql.NumberLit{Value: "100"}},
		},
	}, `WITH "cte" AS (SELECT *) UPDATE "tbl" SET "x" = 100`)
}

func TestIdent_String(t *testing.T) {
	AssertExprStringer(t, &sql.Ident{Name: "foo"}, `"foo"`)
	AssertExprStringer(t, &sql.Ident{Name: "foo \" bar"}, `"foo "" bar"`)
}

func TestStringLit_String(t *testing.T) {
	AssertExprStringer(t, &sql.StringLit{Value: "foo"}, `'foo'`)
	AssertExprStringer(t, &sql.StringLit{Value: "foo ' bar"}, `'foo '' bar'`)
}

func TestNumberLit_String(t *testing.T) {
	AssertExprStringer(t, &sql.NumberLit{Value: "123.45"}, `123.45`)
}

func TestBlobLit_String(t *testing.T) {
	AssertExprStringer(t, &sql.BlobLit{Value: "0123abcd"}, `x'0123abcd'`)
}

func TestBoolLit_String(t *testing.T) {
	AssertExprStringer(t, &sql.BoolLit{Value: true}, `TRUE`)
	AssertExprStringer(t, &sql.BoolLit{Value: false}, `FALSE`)
}

func TestNullLit_String(t *testing.T) {
	AssertExprStringer(t, &sql.NullLit{}, `NULL`)
}

func TestBindExpr_String(t *testing.T) {
	AssertExprStringer(t, &sql.BindExpr{Name: "foo"}, `foo`)
}

func TestParenExpr_String(t *testing.T) {
	AssertExprStringer(t, &sql.NullLit{}, `NULL`)
}

func TestUnaryExpr_String(t *testing.T) {
	AssertExprStringer(t, &sql.UnaryExpr{Op: sql.OP_PLUS, X: &sql.NumberLit{Value: "100"}}, `+100`)
	AssertExprStringer(t, &sql.UnaryExpr{Op: sql.OP_MINUS, X: &sql.NumberLit{Value: "100"}}, `-100`)
	AssertExprStringer(t, &sql.UnaryExpr{Op: sql.OP_NOT, X: &sql.NumberLit{Value: "100"}}, `NOT 100`)
	AssertExprStringer(t, &sql.UnaryExpr{Op: sql.OP_BITNOT, X: &sql.NumberLit{Value: "100"}}, `~100`)
	AssertNodeStringerPanic(t, &sql.UnaryExpr{X: &sql.NumberLit{Value: "100"}}, `invalid op`)
}

func TestBinaryExpr_String(t *testing.T) {
	AssertExprStringer(t, &sql.BinaryExpr{Op: sql.OP_PLUS, X: &sql.NumberLit{Value: "1"}, Y: &sql.NumberLit{Value: "2"}}, `1 + 2`)
	AssertExprStringer(t, &sql.BinaryExpr{Op: sql.OP_MINUS, X: &sql.NumberLit{Value: "1"}, Y: &sql.NumberLit{Value: "2"}}, `1 - 2`)
	AssertExprStringer(t, &sql.BinaryExpr{Op: sql.OP_MULTIPLY, X: &sql.NumberLit{Value: "1"}, Y: &sql.NumberLit{Value: "2"}}, `1 * 2`)
	AssertExprStringer(t, &sql.BinaryExpr{Op: sql.OP_DIVIDE, X: &sql.NumberLit{Value: "1"}, Y: &sql.NumberLit{Value: "2"}}, `1 / 2`)
	AssertExprStringer(t, &sql.BinaryExpr{Op: sql.OP_MODULO, X: &sql.NumberLit{Value: "1"}, Y: &sql.NumberLit{Value: "2"}}, `1 % 2`)
	AssertExprStringer(t, &sql.BinaryExpr{Op: sql.OP_CONCAT, X: &sql.NumberLit{Value: "1"}, Y: &sql.NumberLit{Value: "2"}}, `1 || 2`)
	AssertExprStringer(t, &sql.BinaryExpr{Op: sql.OP_BETWEEN, X: &sql.NumberLit{Value: "1"}, Y: &sql.BinaryExpr{X: &sql.NumberLit{Value: "2"}, Op: sql.OP_AND, Y: &sql.NumberLit{Value: "3"}}}, `1 BETWEEN 2 AND 3`)
	AssertExprStringer(t, &sql.BinaryExpr{Op: sql.OP_NOT_BETWEEN, X: &sql.NumberLit{Value: "1"}, Y: &sql.BinaryExpr{Op: sql.OP_AND, X: &sql.NumberLit{Value: "2"}, Y: &sql.NumberLit{Value: "3"}}}, `1 NOT BETWEEN 2 AND 3`)
	AssertExprStringer(t, &sql.BinaryExpr{Op: sql.OP_LSHIFT, X: &sql.NumberLit{Value: "1"}, Y: &sql.NumberLit{Value: "2"}}, `1 << 2`)
	AssertExprStringer(t, &sql.BinaryExpr{Op: sql.OP_RSHIFT, X: &sql.NumberLit{Value: "1"}, Y: &sql.NumberLit{Value: "2"}}, `1 >> 2`)
	AssertExprStringer(t, &sql.BinaryExpr{Op: sql.OP_BITAND, X: &sql.NumberLit{Value: "1"}, Y: &sql.NumberLit{Value: "2"}}, `1 & 2`)
	AssertExprStringer(t, &sql.BinaryExpr{Op: sql.OP_BITOR, X: &sql.NumberLit{Value: "1"}, Y: &sql.NumberLit{Value: "2"}}, `1 | 2`)
	AssertExprStringer(t, &sql.BinaryExpr{Op: sql.OP_LT, X: &sql.NumberLit{Value: "1"}, Y: &sql.NumberLit{Value: "2"}}, `1 < 2`)
	AssertExprStringer(t, &sql.BinaryExpr{Op: sql.OP_LE, X: &sql.NumberLit{Value: "1"}, Y: &sql.NumberLit{Value: "2"}}, `1 <= 2`)
	AssertExprStringer(t, &sql.BinaryExpr{Op: sql.OP_GT, X: &sql.NumberLit{Value: "1"}, Y: &sql.NumberLit{Value: "2"}}, `1 > 2`)
	AssertExprStringer(t, &sql.BinaryExpr{Op: sql.OP_GE, X: &sql.NumberLit{Value: "1"}, Y: &sql.NumberLit{Value: "2"}}, `1 >= 2`)
	AssertExprStringer(t, &sql.BinaryExpr{Op: sql.OP_EQ, X: &sql.NumberLit{Value: "1"}, Y: &sql.NumberLit{Value: "2"}}, `1 = 2`)
	AssertExprStringer(t, &sql.BinaryExpr{Op: sql.OP_NE, X: &sql.NumberLit{Value: "1"}, Y: &sql.NumberLit{Value: "2"}}, `1 != 2`)
	AssertExprStringer(t, &sql.BinaryExpr{Op: sql.OP_IS, X: &sql.NumberLit{Value: "1"}, Y: &sql.NumberLit{Value: "2"}}, `1 IS 2`)
	AssertExprStringer(t, &sql.BinaryExpr{Op: sql.OP_IS_NOT, X: &sql.NumberLit{Value: "1"}, Y: &sql.NumberLit{Value: "2"}}, `1 IS NOT 2`)
	AssertExprStringer(t, &sql.InExpr{Op: sql.OP_IN, X: &sql.NumberLit{Value: "1"}, Values: &sql.ExprList{Exprs: []sql.Expr{&sql.NumberLit{Value: "2"}}}}, `1 IN (2)`)
	AssertExprStringer(t, &sql.InExpr{Op: sql.OP_NOT_IN, X: &sql.NumberLit{Value: "1"}, Values: &sql.ExprList{Exprs: []sql.Expr{&sql.NumberLit{Value: "2"}}}}, `1 NOT IN (2)`)
	AssertExprStringer(t, &sql.BinaryExpr{Op: sql.OP_LIKE, X: &sql.NumberLit{Value: "1"}, Y: &sql.NumberLit{Value: "2"}}, `1 LIKE 2`)
	AssertExprStringer(t, &sql.BinaryExpr{Op: sql.OP_NOT_LIKE, X: &sql.NumberLit{Value: "1"}, Y: &sql.NumberLit{Value: "2"}}, `1 NOT LIKE 2`)
	AssertExprStringer(t, &sql.BinaryExpr{Op: sql.OP_GLOB, X: &sql.NumberLit{Value: "1"}, Y: &sql.NumberLit{Value: "2"}}, `1 GLOB 2`)
	AssertExprStringer(t, &sql.BinaryExpr{Op: sql.OP_NOT_GLOB, X: &sql.NumberLit{Value: "1"}, Y: &sql.NumberLit{Value: "2"}}, `1 NOT GLOB 2`)
	AssertExprStringer(t, &sql.BinaryExpr{Op: sql.OP_MATCH, X: &sql.NumberLit{Value: "1"}, Y: &sql.NumberLit{Value: "2"}}, `1 MATCH 2`)
	AssertExprStringer(t, &sql.BinaryExpr{Op: sql.OP_NOT_MATCH, X: &sql.NumberLit{Value: "1"}, Y: &sql.NumberLit{Value: "2"}}, `1 NOT MATCH 2`)
	AssertExprStringer(t, &sql.BinaryExpr{Op: sql.OP_REGEXP, X: &sql.NumberLit{Value: "1"}, Y: &sql.NumberLit{Value: "2"}}, `1 REGEXP 2`)
	AssertExprStringer(t, &sql.BinaryExpr{Op: sql.OP_NOT_REGEXP, X: &sql.NumberLit{Value: "1"}, Y: &sql.NumberLit{Value: "2"}}, `1 NOT REGEXP 2`)
	AssertExprStringer(t, &sql.BinaryExpr{Op: sql.OP_AND, X: &sql.NumberLit{Value: "1"}, Y: &sql.NumberLit{Value: "2"}}, `1 AND 2`)
	AssertExprStringer(t, &sql.BinaryExpr{Op: sql.OP_OR, X: &sql.NumberLit{Value: "1"}, Y: &sql.NumberLit{Value: "2"}}, `1 OR 2`)
	AssertExprStringer(t, &sql.BinaryExpr{Op: sql.OP_JSON_EXTRACT_JSON, X: &sql.NumberLit{Value: "1"}, Y: &sql.NumberLit{Value: "2"}}, `1 -> 2`)
	AssertExprStringer(t, &sql.BinaryExpr{Op: sql.OP_JSON_EXTRACT_SQL, X: &sql.NumberLit{Value: "1"}, Y: &sql.NumberLit{Value: "2"}}, `1 ->> 2`)
	AssertExprStringer(t, &sql.BinaryExpr{Op: sql.OP_IS_DISTINCT_FROM, X: &sql.NumberLit{Value: "1"}, Y: &sql.NumberLit{Value: "2"}}, `1 IS DISTINCT FROM 2`)
	AssertExprStringer(t, &sql.BinaryExpr{Op: sql.OP_IS_NOT_DISTINCT_FROM, X: &sql.NumberLit{Value: "1"}, Y: &sql.NumberLit{Value: "2"}}, `1 IS NOT DISTINCT FROM 2`)
	AssertExprStringer(t, &sql.BinaryExpr{Op: sql.OP_ESCAPE, X: &sql.BinaryExpr{Op: sql.OP_LIKE, X: &sql.StringLit{Value: "x"}, Y: &sql.StringLit{Value: "y"}}, Y: &sql.StringLit{Value: "z"}}, `'x' LIKE 'y' ESCAPE 'z'`)
	AssertExprStringer(t, &sql.BinaryExpr{Op: sql.OP_COLLATE, X: &sql.StringLit{Value: "x"}, Y: &sql.StringLit{Value: "y"}}, `'x' COLLATE 'y'`)
	AssertNodeStringerPanic(t, &sql.BinaryExpr{}, `invalid op`)
}

func TestCastExpr_String(t *testing.T) {
	AssertExprStringer(t, &sql.CastExpr{X: &sql.NumberLit{Value: "1"}, Type: &sql.Type{Name: &sql.Ident{Name: "INTEGER"}}}, `CAST(1 AS INTEGER)`)
}

func TestCaseExpr_String(t *testing.T) {
	AssertExprStringer(t, &sql.CaseExpr{
		Operand: &sql.Ident{Name: "foo"},
		Blocks: []*sql.CaseBlock{
			{Condition: &sql.NumberLit{Value: "1"}, Body: &sql.BoolLit{Value: true}},
			{Condition: &sql.NumberLit{Value: "2"}, Body: &sql.BoolLit{Value: false}},
		},
		ElseExpr: &sql.NullLit{},
	}, `CASE "foo" WHEN 1 THEN TRUE WHEN 2 THEN FALSE ELSE NULL END`)

	AssertExprStringer(t, &sql.CaseExpr{
		Blocks: []*sql.CaseBlock{
			{Condition: &sql.NumberLit{Value: "1"}, Body: &sql.BoolLit{Value: true}},
		},
	}, `CASE WHEN 1 THEN TRUE END`)
}

func TestExprList_String(t *testing.T) {
	AssertExprStringer(t, &sql.ExprList{Exprs: []sql.Expr{&sql.NullLit{}}}, `(NULL)`)
	AssertExprStringer(t, &sql.ExprList{Exprs: []sql.Expr{&sql.NullLit{}, &sql.NullLit{}}}, `(NULL, NULL)`)
}

func TestQualifiedRef_String(t *testing.T) {
	AssertExprStringer(t, &sql.QualifiedRef{Table: &sql.QualifiedName{Name: &sql.Ident{Name: "tbl"}}, Column: &sql.Ident{Name: "col"}}, `"tbl"."col"`)
	AssertExprStringer(t, &sql.QualifiedRef{Table: &sql.QualifiedName{Name: &sql.Ident{Name: "tbl"}}, Star: pos(0)}, `"tbl".*`)
}

func TestCall_String(t *testing.T) {
	AssertExprStringer(t, &sql.Call{Name: &sql.QualifiedName{Name: &sql.Ident{Name: "foo"}, FunctionCall: true}}, `"foo"()`)
	AssertExprStringer(t, &sql.Call{Name: &sql.QualifiedName{Name: &sql.Ident{Name: "foo"}, FunctionCall: true, FunctionStar: true}}, `"foo"(*)`)

	AssertExprStringer(t, &sql.Call{
		Name: &sql.QualifiedName{
			Name:             &sql.Ident{Name: "foo"},
			FunctionCall:     true,
			FunctionDistinct: true,
			FunctionArgs: []*sql.FunctionArg{
				{Expr: &sql.NullLit{}},
				{Expr: &sql.NullLit{}},
			},
		},
	}, `"foo"(DISTINCT NULL, NULL)`)

	AssertExprStringer(t, &sql.Call{
		Name:   &sql.QualifiedName{Name: &sql.Ident{Name: "foo"}, FunctionCall: true},
		Filter: &sql.BoolLit{Value: true},
	}, `"foo"() FILTER (WHERE TRUE)`)

	AssertExprStringer(t, &sql.Call{
		Name:     &sql.QualifiedName{Name: &sql.Ident{Name: "foo"}, FunctionCall: true},
		OverName: &sql.Ident{Name: "win"},
	}, `"foo"() OVER "win"`)

	t.Run("FrameSpec", func(t *testing.T) {
		AssertExprStringer(t, &sql.Call{
			Name: &sql.QualifiedName{Name: &sql.Ident{Name: "foo"}, FunctionCall: true},
			OverWindow: &sql.WindowDefinition{
				Frame: &sql.FrameSpec{
					Rows:            pos(0),
					X:               &sql.NullLit{},
					PrecedingX:      pos(0),
					ExcludeNoOthers: pos(0),
				},
			},
		}, `"foo"() OVER (ROWS NULL PRECEDING EXCLUDE NO OTHERS)`)

		AssertExprStringer(t, &sql.Call{
			Name: &sql.QualifiedName{Name: &sql.Ident{Name: "foo"}, FunctionCall: true},
			OverWindow: &sql.WindowDefinition{
				Frame: &sql.FrameSpec{
					Groups:            pos(0),
					CurrentRowX:       pos(0),
					ExcludeCurrentRow: pos(0),
				},
			},
		}, `"foo"() OVER (GROUPS CURRENT ROW EXCLUDE CURRENT ROW)`)

		AssertExprStringer(t, &sql.Call{
			Name: &sql.QualifiedName{Name: &sql.Ident{Name: "foo"}, FunctionCall: true},
			OverWindow: &sql.WindowDefinition{
				Frame: &sql.FrameSpec{
					Rows:        pos(0),
					UnboundedX:  pos(0),
					PrecedingX:  pos(0),
					Between:     pos(0),
					CurrentRowY: pos(0),
				},
			},
		}, `"foo"() OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)`)

		AssertExprStringer(t, &sql.Call{
			Name: &sql.QualifiedName{Name: &sql.Ident{Name: "foo"}, FunctionCall: true},
			OverWindow: &sql.WindowDefinition{
				Frame: &sql.FrameSpec{
					Rows:        pos(0),
					X:           &sql.NullLit{},
					PrecedingX:  pos(0),
					Between:     pos(0),
					CurrentRowY: pos(0),
				},
			},
		}, `"foo"() OVER (ROWS BETWEEN NULL PRECEDING AND CURRENT ROW)`)

		AssertExprStringer(t, &sql.Call{
			Name: &sql.QualifiedName{Name: &sql.Ident{Name: "foo"}, FunctionCall: true},
			OverWindow: &sql.WindowDefinition{
				Frame: &sql.FrameSpec{
					Range:        pos(0),
					X:            &sql.NullLit{},
					FollowingX:   pos(0),
					Between:      pos(0),
					Y:            &sql.BoolLit{Value: true},
					PrecedingY:   pos(0),
					ExcludeGroup: pos(0),
				},
			},
		}, `"foo"() OVER (RANGE BETWEEN NULL FOLLOWING AND TRUE PRECEDING EXCLUDE GROUP)`)

		AssertExprStringer(t, &sql.Call{
			Name: &sql.QualifiedName{Name: &sql.Ident{Name: "foo"}, FunctionCall: true},
			OverWindow: &sql.WindowDefinition{
				Frame: &sql.FrameSpec{
					Range:       pos(0),
					CurrentRowX: pos(0),
					Between:     pos(0),
					Y:           &sql.BoolLit{Value: true},
					FollowingY:  pos(0),
					ExcludeTies: pos(0),
				},
			},
		}, `"foo"() OVER (RANGE BETWEEN CURRENT ROW AND TRUE FOLLOWING EXCLUDE TIES)`)

		AssertExprStringer(t, &sql.Call{
			Name: &sql.QualifiedName{Name: &sql.Ident{Name: "foo"}, FunctionCall: true},
			OverWindow: &sql.WindowDefinition{
				Frame: &sql.FrameSpec{
					Range:       pos(0),
					CurrentRowX: pos(0),
					Between:     pos(0),
					CurrentRowY: pos(0),
				},
			},
		}, `"foo"() OVER (RANGE BETWEEN CURRENT ROW AND CURRENT ROW)`)

		AssertExprStringer(t, &sql.Call{
			Name: &sql.QualifiedName{Name: &sql.Ident{Name: "foo"}, FunctionCall: true},
			OverWindow: &sql.WindowDefinition{
				Frame: &sql.FrameSpec{
					Range:       pos(0),
					CurrentRowX: pos(0),
					Between:     pos(0),
					UnboundedY:  pos(0),
					FollowingY:  pos(0),
				},
			},
		}, `"foo"() OVER (RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)`)
	})
}

func TestRaise_String(t *testing.T) {
	AssertExprStringer(t, &sql.Raise{Rollback: pos(0), Error: &sql.StringLit{Value: "err"}}, `RAISE(ROLLBACK, 'err')`)
	AssertExprStringer(t, &sql.Raise{Abort: pos(0), Error: &sql.StringLit{Value: "err"}}, `RAISE(ABORT, 'err')`)
	AssertExprStringer(t, &sql.Raise{Fail: pos(0), Error: &sql.StringLit{Value: "err"}}, `RAISE(FAIL, 'err')`)
	AssertExprStringer(t, &sql.Raise{Ignore: pos(0)}, `RAISE(IGNORE)`)
}

func TestExists_String(t *testing.T) {
	AssertExprStringer(t, &sql.Exists{
		Select: &sql.SelectStatement{
			Columns: []*sql.ResultColumn{
				{Star: pos(0)},
			},
		},
	}, `EXISTS (SELECT *)`)

	AssertExprStringer(t, &sql.Exists{
		Not: pos(0),
		Select: &sql.SelectStatement{
			Columns: []*sql.ResultColumn{
				{Star: pos(0)},
			},
		},
	}, `NOT EXISTS (SELECT *)`)
}

func AssertExprStringer(tb testing.TB, expr sql.Expr, s string) {
	tb.Helper()
	if str := expr.String(); str != s {
		tb.Fatalf("String()=%s, expected %s", str, s)
	} else if _, err := sql.ParseExprString(str); err != nil {
		tb.Fatalf("cannot parse string: %s; err=%s", str, err)
	}
}

func AssertStatementStringer(tb testing.TB, stmt sql.Statement, s string) {
	tb.Helper()
	if str := stmt.String(); str != s {
		tb.Fatalf("String()=%s, expected %s", str, s)
	} else if _, err := sql.ParseStmtString(str); err != nil {
		tb.Fatalf("cannot parse string: %s; err=%s", str, err)
	}
}

func AssertNodeStringerPanic(tb testing.TB, node sql.Node, msg string) {
	tb.Helper()
	var r any
	func() {
		defer func() { r = recover() }()
		_ = node.String()
	}()
	if r == nil {
		tb.Fatal("expected node stringer to panic")
	} else if r != msg {
		tb.Fatalf("recover()=%s, want %s", r, msg)
	}
}

// pos is a helper function for generating positions based on offset for one-line parsing.
func pos(_ int) bool {
	return true
}
