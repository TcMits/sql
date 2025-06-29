//go:build debugpos
// +build debugpos

package sql_test

import (
	"strings"
	"testing"

	"github.com/TcMits/sql"
	"github.com/go-test/deep"
)

func TestParser_ParseStatement(t *testing.T) {
	t.Run("ErrNoStatement", func(t *testing.T) {
		AssertParseStatementError(t, `123`, `1:1: expected statement, found 123`)
	})

	t.Run("Pragma", func(t *testing.T) {
		AssertParseStatement(t, `PRAGMA pragma_name`, &sql.PragmaStatement{
			Expr: &sql.Ident{Name: "pragma_name"},
		})
		AssertParseStatement(t, `PRAGMA pragma_name=true`, &sql.PragmaStatement{
			Expr: &sql.BinaryExpr{
				X:  &sql.Ident{Name: "pragma_name"},
				Op: sql.OP_EQ,
				Y:  &sql.BoolLit{Value: true},
			},
		})
		AssertParseStatement(t, `PRAGMA pragma_name(N)`, &sql.PragmaStatement{
			Expr: &sql.Call{
				Name: &sql.QualifiedName{
					Name: &sql.Ident{
						Name: "pragma_name",
					},
					FunctionCall: true,
					FunctionArgs: []*sql.FunctionArg{
						{Expr: &sql.Ident{Name: "N"}},
					},
				},
			},
		})
		AssertParseStatement(t, `PRAGMA schema.pragma_name`, &sql.PragmaStatement{
			Schema: &sql.Ident{Name: "schema"},
			Expr:   &sql.Ident{Name: "pragma_name"},
		})

		AssertParseStatementError(t, `PRAGMA schema.`, "1:15: expected pragma name, found 'EOF'")
		AssertParseStatementError(t, `PRAGMA .name`, "1:8: expected schema name, found '.'")
		AssertParseStatementError(t, `PRAGMA schema.name=`, "1:20: expected expression, found 'EOF'")
		AssertParseStatementError(t, `PRAGMA schema.name(`, "1:20: expected expression, found 'EOF'")
		AssertParseStatementError(t, `PRAGMA schema.name(arg`, "1:23: expected comma or right paren, found 'EOF'")
	})

	t.Run("Explain", func(t *testing.T) {
		t.Run("", func(t *testing.T) {
			AssertParseStatement(t, `EXPLAIN BEGIN`, &sql.ExplainStatement{
				Explain: pos(0),
				Stmt:    &sql.BeginStatement{},
			})
		})
		t.Run("QueryPlan", func(t *testing.T) {
			AssertParseStatement(t, `EXPLAIN QUERY PLAN BEGIN`, &sql.ExplainStatement{
				Explain:   pos(0),
				QueryPlan: pos(14),
				Stmt:      &sql.BeginStatement{},
			})
		})
		t.Run("ErrNoPlan", func(t *testing.T) {
			AssertParseStatementError(t, `EXPLAIN QUERY`, `1:14: expected PLAN, found 'EOF'`)
		})
		t.Run("ErrStmt", func(t *testing.T) {
			AssertParseStatementError(t, `EXPLAIN CREATE`, `1:9: expected TABLE, VIEW, INDEX, TRIGGER`)
		})
	})

	t.Run("Begin", func(t *testing.T) {
		t.Run("", func(t *testing.T) {
			AssertParseStatement(t, `BEGIN`, &sql.BeginStatement{})
		})
		t.Run("Transaction", func(t *testing.T) {
			AssertParseStatement(t, `BEGIN TRANSACTION`, &sql.BeginStatement{})
		})
		t.Run("DeferredTransaction", func(t *testing.T) {
			AssertParseStatement(t, `BEGIN DEFERRED TRANSACTION`, &sql.BeginStatement{Deferred: pos(6)})
		})
		t.Run("Immediate", func(t *testing.T) {
			AssertParseStatement(t, `BEGIN IMMEDIATE;`, &sql.BeginStatement{Immediate: pos(6)})
		})
		t.Run("Exclusive", func(t *testing.T) {
			AssertParseStatement(t, `BEGIN EXCLUSIVE`, &sql.BeginStatement{Exclusive: pos(6)})
		})
		t.Run("ErrOverrun", func(t *testing.T) {
			AssertParseStatementError(t, `BEGIN COMMIT`, `1:7: expected semicolon or EOF, found 'COMMIT'`)
		})
	})

	t.Run("Commit", func(t *testing.T) {
		t.Run("", func(t *testing.T) {
			AssertParseStatement(t, `COMMIT`, &sql.CommitStatement{})
		})
		t.Run("Transaction", func(t *testing.T) {
			AssertParseStatement(t, `COMMIT TRANSACTION`, &sql.CommitStatement{})
		})
	})

	t.Run("End", func(t *testing.T) {
		t.Run("", func(t *testing.T) {
			AssertParseStatement(t, `END`, &sql.CommitStatement{})
		})
		t.Run("Transaction", func(t *testing.T) {
			AssertParseStatement(t, `END TRANSACTION`, &sql.CommitStatement{})
		})
	})

	t.Run("Rollback", func(t *testing.T) {
		t.Run("", func(t *testing.T) {
			AssertParseStatement(t, `ROLLBACK`, &sql.RollbackStatement{})
		})
		t.Run("Transaction", func(t *testing.T) {
			AssertParseStatement(t, `ROLLBACK TRANSACTION`, &sql.RollbackStatement{})
		})
		t.Run("To", func(t *testing.T) {
			AssertParseStatement(t, `ROLLBACK TO svpt`, &sql.RollbackStatement{
				SavepointName: &sql.Ident{
					Name: "svpt",
				},
			})
		})
		t.Run("TransactionToSavepoint", func(t *testing.T) {
			AssertParseStatement(t, `ROLLBACK TRANSACTION TO SAVEPOINT "svpt"`, &sql.RollbackStatement{
				SavepointName: &sql.Ident{
					Name:   "svpt",
					Quoted: true,
				},
			})
		})
		t.Run("ErrSavepointName", func(t *testing.T) {
			AssertParseStatementError(t, `ROLLBACK TO SAVEPOINT 123`, `1:23: expected savepoint name, found 123`)
		})
	})

	t.Run("Savepoint", func(t *testing.T) {
		t.Run("Ident", func(t *testing.T) {
			AssertParseStatement(t, `SAVEPOINT svpt`, &sql.SavepointStatement{
				Name: &sql.Ident{
					Name: "svpt",
				},
			})
		})
		t.Run("String", func(t *testing.T) {
			AssertParseStatement(t, `SAVEPOINT "svpt"`, &sql.SavepointStatement{
				Name: &sql.Ident{
					Name:   "svpt",
					Quoted: true,
				},
			})
		})
		t.Run("ErrSavepointName", func(t *testing.T) {
			AssertParseStatementError(t, `SAVEPOINT 123`, `1:11: expected savepoint name, found 123`)
		})
	})

	t.Run("Release", func(t *testing.T) {
		t.Run("Ident", func(t *testing.T) {
			AssertParseStatement(t, `RELEASE svpt`, &sql.ReleaseStatement{
				Name: &sql.Ident{
					Name: "svpt",
				},
			})
		})
		t.Run("String", func(t *testing.T) {
			AssertParseStatement(t, `RELEASE "svpt"`, &sql.ReleaseStatement{
				Name: &sql.Ident{
					Name:   "svpt",
					Quoted: true,
				},
			})
		})
		t.Run("SavepointIdent", func(t *testing.T) {
			AssertParseStatement(t, `RELEASE SAVEPOINT svpt`, &sql.ReleaseStatement{
				Name: &sql.Ident{
					Name: "svpt",
				},
			})
		})
		t.Run("ErrSavepointName", func(t *testing.T) {
			AssertParseStatementError(t, `RELEASE 123`, `1:9: expected savepoint name, found 123`)
		})
	})

	t.Run("CreateTable", func(t *testing.T) {
		AssertParseStatement(t, `CREATE TABLE tbl (col1 TEXT, col2 DECIMAL(10,5))`, &sql.CreateTableStatement{
			Name: &sql.QualifiedName{Name: &sql.Ident{
				Name: "tbl",
			}},
			Columns: []*sql.ColumnDefinition{
				{
					Name: &sql.Ident{Name: "col1"},
					Type: &sql.Type{
						Name: &sql.Ident{Name: "TEXT"},
					},
				},
				{
					Name: &sql.Ident{Name: "col2"},
					Type: &sql.Type{
						Name:      &sql.Ident{Name: "DECIMAL"},
						Precision: &sql.NumberLit{Value: "10"},
						Scale:     &sql.NumberLit{Value: "5"},
					},
				},
			},
		})

		// No column type
		AssertParseStatement(t, `CREATE TABLE tbl (col1, col2)`, &sql.CreateTableStatement{
			Name: &sql.QualifiedName{Name: &sql.Ident{
				Name: "tbl",
			}},
			Columns: []*sql.ColumnDefinition{
				{
					Name: &sql.Ident{Name: "col1"},
				},
				{
					Name: &sql.Ident{Name: "col2"},
				},
			},
		})

		// Column name as a bare keyword
		AssertParseStatement(t, `CREATE TABLE tbl (key)`, &sql.CreateTableStatement{
			Name: &sql.QualifiedName{Name: &sql.Ident{
				Name: "tbl",
			}},
			Columns: []*sql.ColumnDefinition{
				{
					Name: &sql.Ident{Name: "key"},
				},
			},
		})

		// With comments
		AssertParseStatement(t, "CREATE TABLE tbl ( -- comment\n\tcol1 TEXT, -- comment\n\t  col2 TEXT)", &sql.CreateTableStatement{
			Name: &sql.QualifiedName{Name: &sql.Ident{
				Name: "tbl",
			}},
			Columns: []*sql.ColumnDefinition{
				{
					Name: &sql.Ident{Name: "col1"},
					Type: &sql.Type{
						Name: &sql.Ident{Name: "TEXT"},
					},
				},
				{
					Name: &sql.Ident{Name: "col2"},
					Type: &sql.Type{
						Name: &sql.Ident{Name: "TEXT"},
					},
				},
			},
		})

		AssertParseStatement(t, `CREATE TABLE tbl (col1 TEXT) WITHOUT ROWID`, &sql.CreateTableStatement{
			Name: &sql.QualifiedName{Name: &sql.Ident{
				Name: "tbl",
			}},
			Columns: []*sql.ColumnDefinition{
				{
					Name: &sql.Ident{Name: "col1"},
					Type: &sql.Type{
						Name: &sql.Ident{Name: "TEXT"},
					},
				},
			},
			WithoutRowID: true,
		})

		AssertParseStatement(t, `CREATE TABLE tbl (col1 TEXT) STRICT`, &sql.CreateTableStatement{
			Name: &sql.QualifiedName{Name: &sql.Ident{
				Name: "tbl",
			}},
			Columns: []*sql.ColumnDefinition{
				{
					Name: &sql.Ident{Name: "col1"},
					Type: &sql.Type{
						Name: &sql.Ident{Name: "TEXT"},
					},
				},
			},
			Strict: true,
		})

		AssertParseStatement(t, `CREATE TABLE tbl (col1 TEXT) WITHOUT ROWID, STRICT`, &sql.CreateTableStatement{
			Name: &sql.QualifiedName{Name: &sql.Ident{
				Name: "tbl",
			}},
			Columns: []*sql.ColumnDefinition{
				{
					Name: &sql.Ident{Name: "col1"},
					Type: &sql.Type{
						Name: &sql.Ident{Name: "TEXT"},
					},
				},
			},
			Strict:       true,
			WithoutRowID: true,
		})

		AssertParseStatementError(t, `CREATE TABLE`, `1:13: expected qualified name, found 'EOF'`)
		AssertParseStatementError(t, `CREATE TABLE tbl `, `1:18: expected AS or left paren, found 'EOF'`)
		AssertParseStatementError(t, `CREATE TABLE tbl (`, `1:19: expected column name, CONSTRAINT, or right paren, found 'EOF'`)
		AssertParseStatementError(t, `CREATE TABLE tbl (col1 TEXT`, `1:28: expected column name, CONSTRAINT, or right paren, found 'EOF'`)
		AssertParseStatementError(t, `CREATE TABLE tbl (col1 TEXT) WITHOUT`, `1:37: expected ROWID, found 'EOF'`)
		AssertParseStatementError(t, `CREATE TABLE tbl (col1 TEXT) WITHOUT ROWID,`, `1:44: expected STRICT or WITHOUT ROWID, found 'EOF'`)
		AssertParseStatementError(t, `CREATE TABLE tbl (col1 TEXT) STRICT,`, `1:37: expected STRICT or WITHOUT ROWID, found 'EOF'`)

		AssertParseStatement(t, `CREATE TABLE IF NOT EXISTS tbl (col1 TEXT)`, &sql.CreateTableStatement{
			IfNotExists: pos(20),
			Name: &sql.QualifiedName{Name: &sql.Ident{
				Name: "tbl",
			}},
			Columns: []*sql.ColumnDefinition{
				{
					Name: &sql.Ident{
						Name: "col1",
					},
					Type: &sql.Type{
						Name: &sql.Ident{
							Name: "TEXT",
						},
					},
				},
			},
		})

		AssertParseStatement(t, `CREATE TABLE tbl (col1 TEXT, ts DATETIME DEFAULT CURRENT_TIMESTAMP)`, &sql.CreateTableStatement{
			Name: &sql.QualifiedName{Name: &sql.Ident{
				Name: "tbl",
			}},
			Columns: []*sql.ColumnDefinition{
				{
					Name: &sql.Ident{
						Name: "col1",
					},
					Type: &sql.Type{
						Name: &sql.Ident{
							Name: "TEXT",
						},
					},
				},
				{
					Name: &sql.Ident{
						Name: "ts",
					},
					Type: &sql.Type{
						Name: &sql.Ident{
							Name: "DATETIME",
						},
					},
					Constraints: []sql.Constraint{
						&sql.DefaultConstraint{
							Expr: &sql.TimestampLit{Value: "CURRENT_TIMESTAMP"},
						},
					},
				},
			},
		})

		AssertParseStatement(t, "CREATE TABLE t (c1 CHARACTER VARYING, c2 UUID, c3 TIMESTAMP)", &sql.CreateTableStatement{
			Name: &sql.QualifiedName{Name: &sql.Ident{
				Name: "t",
			}},
			Columns: []*sql.ColumnDefinition{
				{
					Name: &sql.Ident{
						Name: "c1",
					},
					Type: &sql.Type{
						Name: &sql.Ident{
							Name: "CHARACTER VARYING",
						},
					},
				},
				{
					Name: &sql.Ident{
						Name: "c2",
					},
					Type: &sql.Type{
						Name: &sql.Ident{
							Name: "UUID",
						},
					},
				},
				{
					Name: &sql.Ident{
						Name: "c3",
					},
					Type: &sql.Type{
						Name: &sql.Ident{
							Name: "TIMESTAMP",
						},
					},
				},
			},
		})

		AssertParseStatement(t, "CREATE TABLE t (c1 NULL)", &sql.CreateTableStatement{
			Name: &sql.QualifiedName{Name: &sql.Ident{
				Name: "t",
			}},
			Columns: []*sql.ColumnDefinition{
				{
					Name: &sql.Ident{
						Name: "c1",
					},
					Type: &sql.Type{
						Name: &sql.Ident{
							Name: "NULL",
						},
					},
				},
			},
		})

		AssertParseStatementError(t, `CREATE TABLE IF`, `1:16: expected NOT, found 'EOF'`)
		AssertParseStatementError(t, `CREATE TABLE IF NOT`, `1:20: expected EXISTS, found 'EOF'`)
		AssertParseStatementError(t, `CREATE TABLE tbl (col1`, `1:23: expected column name, CONSTRAINT, or right paren, found 'EOF'`)
		AssertParseStatementError(t, `CREATE TABLE tbl (col1 DECIMAL(`, `1:32: expected precision, found 'EOF'`)
		AssertParseStatementError(t, `CREATE TABLE tbl (col1 DECIMAL(-12,`, `1:36: expected scale, found 'EOF'`)
		AssertParseStatementError(t, `CREATE TABLE tbl (col1 DECIMAL(1,2`, `1:35: expected right paren, found 'EOF'`)
		AssertParseStatementError(t, `CREATE TABLE tbl (col1 DECIMAL(1`, `1:33: expected right paren, found 'EOF'`)
		AssertParseStatementError(t, `CREATE TABLE tbl (col1 TEXT CONSTRAINT`, `1:39: expected constraint name, found 'EOF'`)

		AssertParseStatement(t, `CREATE TABLE tbl AS SELECT foo`, &sql.CreateTableStatement{
			Name: &sql.QualifiedName{Name: &sql.Ident{
				Name: "tbl",
			}},
			Select: &sql.SelectStatement{
				Columns: []*sql.ResultColumn{
					{Expr: &sql.Ident{Name: "foo"}},
				},
			},
		})
		AssertParseStatement(t, `CREATE TABLE tbl AS WITH cte (x) AS (SELECT y) SELECT foo`, &sql.CreateTableStatement{
			Name: &sql.QualifiedName{Name: &sql.Ident{
				Name: "tbl",
			}},
			Select: &sql.SelectStatement{
				WithClause: &sql.WithClause{
					CTEs: []*sql.CTE{
						{
							TableName: &sql.Ident{Name: "cte"},
							Columns: []*sql.Ident{
								{Name: "x"},
							},
							Select: &sql.SelectStatement{
								Columns: []*sql.ResultColumn{
									{Expr: &sql.Ident{Name: "y"}},
								},
							},
						},
					},
				},
				Columns: []*sql.ResultColumn{
					{Expr: &sql.Ident{Name: "foo"}},
				},
			},
		})
		AssertParseStatementError(t, `CREATE TABLE tbl AS`, `1:20: expected SELECT or VALUES, found 'EOF'`)
		AssertParseStatementError(t, `CREATE TABLE tbl AS WITH`, `1:25: expected table name, found 'EOF'`)

		t.Run("WithSchema", func(t *testing.T) {
			t.Run("Basic", func(t *testing.T) {
				AssertParseStatement(t, `CREATE TABLE main.tbl (col1 TEXT PRIMARY KEY, col2 INTEGER)`, &sql.CreateTableStatement{
					Name: &sql.QualifiedName{
						Schema: &sql.Ident{Name: "main"},
						Name:   &sql.Ident{Name: "tbl"},
					},
					Columns: []*sql.ColumnDefinition{
						{
							Name: &sql.Ident{Name: "col1"},
							Type: &sql.Type{
								Name: &sql.Ident{Name: "TEXT"},
							},
							Constraints: []sql.Constraint{
								&sql.PrimaryKeyConstraint{},
							},
						},
						{
							Name: &sql.Ident{Name: "col2"},
							Type: &sql.Type{
								Name: &sql.Ident{Name: "INTEGER"},
							},
						},
					},
				})
			})

			AssertParseStatementError(t, `CREATE TABLE main. (col1 TEXT PRIMARY KEY, col2 INTEGER)`, `1:20: expected qualified name, found '('`)
		})

		t.Run("WithComment", func(t *testing.T) {
			t.Run("SingleLine", func(t *testing.T) {
				AssertParseStatement(t, "CREATE TABLE tbl\n\t-- test one two\n\t(col1 TEXT)", &sql.CreateTableStatement{
					Name: &sql.QualifiedName{Name: &sql.Ident{Name: "tbl"}},
					Columns: []*sql.ColumnDefinition{
						{
							Name: &sql.Ident{Name: "col1"},
							Type: &sql.Type{
								Name: &sql.Ident{Name: "TEXT"},
							},
						},
					},
				})
			})
			t.Run("MultiLine", func(t *testing.T) {
				AssertParseStatement(t, "CREATE TABLE tbl\n\t/* test one\ntwo*/ (col1 TEXT)", &sql.CreateTableStatement{
					Name: &sql.QualifiedName{Name: &sql.Ident{Name: "tbl"}},
					Columns: []*sql.ColumnDefinition{
						{
							Name: &sql.Ident{Name: "col1"},
							Type: &sql.Type{
								Name: &sql.Ident{Name: "TEXT"},
							},
						},
					},
				})
			})
		})

		t.Run("ColumnConstraint", func(t *testing.T) {
			t.Run("PrimaryKey", func(t *testing.T) {
				t.Run("Simple", func(t *testing.T) {
					AssertParseStatement(t, `CREATE TABLE tbl (col1 TEXT PRIMARY KEY)`, &sql.CreateTableStatement{
						Name: &sql.QualifiedName{Name: &sql.Ident{Name: "tbl"}},
						Columns: []*sql.ColumnDefinition{
							{
								Name: &sql.Ident{Name: "col1"},
								Type: &sql.Type{
									Name: &sql.Ident{Name: "TEXT"},
								},
								Constraints: []sql.Constraint{
									&sql.PrimaryKeyConstraint{},
								},
							},
						},
					})
				})
				t.Run("Full", func(t *testing.T) {
					stmt := ParseStatementOrFail(t, `CREATE TABLE tbl (col1 TEXT CONSTRAINT cons1 PRIMARY KEY AUTOINCREMENT)`).(*sql.CreateTableStatement)
					if diff := deep.Equal(stmt.Columns[0].Constraints[0], &sql.PrimaryKeyConstraint{
						Name:          &sql.Ident{Name: "cons1"},
						Autoincrement: pos(57),
					}); diff != nil {
						t.Fatal(diff)
					}
				})
				t.Run("ErrNoKey", func(t *testing.T) {
					AssertParseStatementError(t, `CREATE TABLE tbl (col1 TEXT PRIMARY`, `1:36: expected KEY, found 'EOF'`)
				})
			})

			t.Run("NotNull", func(t *testing.T) {
				t.Run("ErrNoKey", func(t *testing.T) {
					AssertParseStatementError(t, `CREATE TABLE tbl (col1 TEXT NOT`, `1:32: expected NULL, found 'EOF'`)
				})
				t.Run("Simple", func(t *testing.T) {
					AssertParseStatement(t, `CREATE TABLE tbl (col1 TEXT CONSTRAINT con1 NOT NULL)`, &sql.CreateTableStatement{
						Name: &sql.QualifiedName{Name: &sql.Ident{Name: "tbl"}},
						Columns: []*sql.ColumnDefinition{
							{
								Name: &sql.Ident{Name: "col1"},
								Type: &sql.Type{
									Name: &sql.Ident{Name: "TEXT"},
								},
								Constraints: []sql.Constraint{
									&sql.NotNullConstraint{
										Name: &sql.Ident{Name: "con1"},
									},
								},
							},
						},
					})
				})
			})

			t.Run("Unique", func(t *testing.T) {
				t.Run("Simple", func(t *testing.T) {
					AssertParseStatement(t, `CREATE TABLE tbl (col1 TEXT CONSTRAINT con1 UNIQUE)`, &sql.CreateTableStatement{
						Name: &sql.QualifiedName{Name: &sql.Ident{Name: "tbl"}},
						Columns: []*sql.ColumnDefinition{
							{
								Name: &sql.Ident{Name: "col1"},
								Type: &sql.Type{
									Name: &sql.Ident{Name: "TEXT"},
								},
								Constraints: []sql.Constraint{
									&sql.UniqueConstraint{
										Name: &sql.Ident{Name: "con1"},
									},
								},
							},
						},
					})
				})
			})
			t.Run("Check", func(t *testing.T) {
				AssertParseStatement(t, `CREATE TABLE tbl (col1 TEXT CHECK (col1 > 1))`, &sql.CreateTableStatement{
					Name: &sql.QualifiedName{Name: &sql.Ident{Name: "tbl"}},
					Columns: []*sql.ColumnDefinition{
						{
							Name: &sql.Ident{Name: "col1"},
							Type: &sql.Type{
								Name: &sql.Ident{Name: "TEXT"},
							},
							Constraints: []sql.Constraint{
								&sql.CheckConstraint{
									Expr: &sql.BinaryExpr{
										X:  &sql.Ident{Name: "col1"},
										Op: sql.OP_GT,
										Y:  &sql.NumberLit{Value: "1"},
									},
								},
							},
						},
					},
				})
			})
			t.Run("Default", func(t *testing.T) {
				t.Run("Expr", func(t *testing.T) {
					AssertParseStatement(t, `CREATE TABLE tbl (col1 TEXT DEFAULT (1))`, &sql.CreateTableStatement{
						Name: &sql.QualifiedName{Name: &sql.Ident{Name: "tbl"}},
						Columns: []*sql.ColumnDefinition{
							{
								Name: &sql.Ident{Name: "col1"},
								Type: &sql.Type{
									Name: &sql.Ident{Name: "TEXT"},
								},
								Constraints: []sql.Constraint{
									&sql.DefaultConstraint{
										Expr: &sql.NumberLit{Value: "1"},
									},
								},
							},
						},
					})
				})
				t.Run("String", func(t *testing.T) {
					stmt := ParseStatementOrFail(t, `CREATE TABLE tbl (col1 TEXT DEFAULT 'foo')`).(*sql.CreateTableStatement)
					if diff := deepEqual(stmt.Columns[0].Constraints[0], &sql.DefaultConstraint{
						Expr: &sql.StringLit{Value: "foo"},
					}); diff != "" {
						t.Fatal(diff)
					}
				})
				t.Run("DoubleQuotedIdent", func(t *testing.T) {
					stmt := ParseStatementOrFail(t, `CREATE TABLE tbl (col1 TEXT DEFAULT "foo")`).(*sql.CreateTableStatement)
					if diff := deepEqual(stmt.Columns[0].Constraints[0], &sql.DefaultConstraint{
						Expr: &sql.StringLit{Value: "foo"},
					}); diff != "" {
						t.Fatal(diff)
					}
				})
				t.Run("Blob", func(t *testing.T) {
					stmt := ParseStatementOrFail(t, `CREATE TABLE tbl (col1 TEXT DEFAULT x'0F0F')`).(*sql.CreateTableStatement)
					if diff := deepEqual(stmt.Columns[0].Constraints[0], &sql.DefaultConstraint{
						Expr: &sql.BlobLit{Value: "0F0F"},
					}); diff != "" {
						t.Fatal(diff)
					}
				})
				t.Run("Number", func(t *testing.T) {
					stmt := ParseStatementOrFail(t, `CREATE TABLE tbl (col1 TEXT DEFAULT 1)`).(*sql.CreateTableStatement)
					if diff := deepEqual(stmt.Columns[0].Constraints[0], &sql.DefaultConstraint{
						Expr: &sql.NumberLit{Value: "1"},
					}); diff != "" {
						t.Fatal(diff)
					}
				})
				t.Run("Null", func(t *testing.T) {
					stmt := ParseStatementOrFail(t, `CREATE TABLE tbl (col1 TEXT DEFAULT NULL)`).(*sql.CreateTableStatement)
					if diff := deepEqual(stmt.Columns[0].Constraints[0], &sql.DefaultConstraint{
						Expr: &sql.NullLit{},
					}); diff != "" {
						t.Fatal(diff)
					}
				})
				t.Run("Bool", func(t *testing.T) {
					stmt := ParseStatementOrFail(t, `CREATE TABLE tbl (col1 TEXT DEFAULT true)`).(*sql.CreateTableStatement)
					if diff := deepEqual(stmt.Columns[0].Constraints[0], &sql.DefaultConstraint{
						Expr: &sql.BoolLit{Value: true},
					}); diff != "" {
						t.Fatal(diff)
					}
				})

				AssertParseStatementError(t, `CREATE TABLE tbl (col1 TEXT DEFAULT +`, `1:38: expected signed number, found 'EOF'`)
				AssertParseStatementError(t, `CREATE TABLE tbl (col1 TEXT DEFAULT -`, `1:38: expected signed number, found 'EOF'`)
				AssertParseStatementError(t, `CREATE TABLE tbl (col1 TEXT DEFAULT `, `1:37: expected literal value or left paren, found 'EOF'`)
				AssertParseStatementError(t, `CREATE TABLE tbl (col1 TEXT DEFAULT (TABLE`, `1:38: expected expression, found 'TABLE'`)
				AssertParseStatementError(t, `CREATE TABLE tbl (col1 TEXT DEFAULT (true`, `1:42: expected right paren, found 'EOF'`)
			})

			t.Run("Generated", func(t *testing.T) {
				AssertParseStatement(t, `CREATE TABLE tbl (col1 TEXT GENERATED ALWAYS AS (1))`, &sql.CreateTableStatement{
					Name: &sql.QualifiedName{Name: &sql.Ident{Name: "tbl"}},
					Columns: []*sql.ColumnDefinition{
						{
							Name: &sql.Ident{Name: "col1"},
							Type: &sql.Type{
								Name: &sql.Ident{Name: "TEXT"},
							},
							Constraints: []sql.Constraint{
								&sql.GeneratedConstraint{
									Expr: &sql.NumberLit{Value: "1"},
								},
							},
						},
					},
				})

				AssertParseStatement(t, `CREATE TABLE tbl (col1 TEXT AS (1) STORED)`, &sql.CreateTableStatement{
					Name: &sql.QualifiedName{Name: &sql.Ident{Name: "tbl"}},
					Columns: []*sql.ColumnDefinition{
						{
							Name: &sql.Ident{Name: "col1"},
							Type: &sql.Type{
								Name: &sql.Ident{Name: "TEXT"},
							},
							Constraints: []sql.Constraint{
								&sql.GeneratedConstraint{
									Expr:   &sql.NumberLit{Value: "1"},
									Stored: pos(35),
								},
							},
						},
					},
				})

				AssertParseStatement(t, `CREATE TABLE tbl (col1 TEXT AS (1) VIRTUAL)`, &sql.CreateTableStatement{
					Name: &sql.QualifiedName{Name: &sql.Ident{Name: "tbl"}},
					Columns: []*sql.ColumnDefinition{
						{
							Name: &sql.Ident{Name: "col1"},
							Type: &sql.Type{
								Name: &sql.Ident{Name: "TEXT"},
							},
							Constraints: []sql.Constraint{
								&sql.GeneratedConstraint{
									Expr:    &sql.NumberLit{Value: "1"},
									Virtual: pos(35),
								},
							},
						},
					},
				})

				AssertParseStatementError(t, `CREATE TABLE tbl (col1 TEXT GENERATED`, `1:38: expected ALWAYS, found 'EOF'`)
				AssertParseStatementError(t, `CREATE TABLE tbl (col1 TEXT GENERATED ALWAYS`, `1:45: expected AS, found 'EOF'`)
				AssertParseStatementError(t, `CREATE TABLE tbl (col1 TEXT AS `, `1:32: expected left paren, found 'EOF'`)
				AssertParseStatementError(t, `CREATE TABLE tbl (col1 TEXT AS (`, `1:33: expected expression, found 'EOF'`)
				AssertParseStatementError(t, `CREATE TABLE tbl (col1 TEXT AS (1`, `1:34: expected right paren, found 'EOF'`)
			})

			t.Run("Collate", func(t *testing.T) {
				AssertParseStatement(t, `CREATE TABLE tbl (col1 TEXT COLLATE NOCASE)`, &sql.CreateTableStatement{
					Name: &sql.QualifiedName{Name: &sql.Ident{Name: "tbl"}},
					Columns: []*sql.ColumnDefinition{
						{
							Name: &sql.Ident{Name: "col1"},
							Type: &sql.Type{
								Name: &sql.Ident{Name: "TEXT"},
							},
							Constraints: []sql.Constraint{
								&sql.CollateConstraint{
									Collation: &sql.Ident{Name: "NOCASE"},
								},
							},
						},
					},
				})

				AssertParseStatementError(t, `CREATE TABLE tbl (col1 TEXT COLLATE`, `1:36: expected collation name, found 'EOF'`)
			})

			t.Run("ForeignKey", func(t *testing.T) {
				t.Run("Simple", func(t *testing.T) {
					AssertParseStatement(t, `CREATE TABLE tbl (col1 TEXT REFERENCES foo (col2))`, &sql.CreateTableStatement{
						Name: &sql.QualifiedName{Name: &sql.Ident{Name: "tbl"}},
						Columns: []*sql.ColumnDefinition{
							{
								Name: &sql.Ident{Name: "col1"},
								Type: &sql.Type{
									Name: &sql.Ident{Name: "TEXT"},
								},
								Constraints: []sql.Constraint{
									&sql.ForeignKeyConstraint{
										ForeignTable: &sql.Ident{Name: "foo"},
										ForeignColumns: []*sql.Ident{
											{Name: "col2"},
										},
									},
								},
							},
						},
					})
				})
				t.Run("OnDeleteSetNull", func(t *testing.T) {
					stmt := ParseStatementOrFail(t, `CREATE TABLE tbl (col1 TEXT REFERENCES foo (col2) ON DELETE SET NULL)`).(*sql.CreateTableStatement)
					if diff := deepEqual(stmt.Columns[0].Constraints[0], &sql.ForeignKeyConstraint{
						ForeignTable: &sql.Ident{Name: "foo"},
						ForeignColumns: []*sql.Ident{
							{Name: "col2"},
						},
						Args: []*sql.ForeignKeyArg{
							{
								OnDelete: pos(53),
								SetNull:  pos(64),
							},
						},
					}); diff != "" {
						t.Fatal(diff)
					}
				})
				t.Run("OnDeleteSetDefault", func(t *testing.T) {
					stmt := ParseStatementOrFail(t, `CREATE TABLE tbl (col1 TEXT REFERENCES foo (col2) ON DELETE SET DEFAULT)`).(*sql.CreateTableStatement)
					if diff := deepEqual(stmt.Columns[0].Constraints[0], &sql.ForeignKeyConstraint{
						ForeignTable: &sql.Ident{Name: "foo"},
						ForeignColumns: []*sql.Ident{
							{Name: "col2"},
						},
						Args: []*sql.ForeignKeyArg{
							{
								OnDelete:   pos(53),
								SetDefault: pos(64),
							},
						},
					}); diff != "" {
						t.Fatal(diff)
					}
				})
				t.Run("OnDeleteSetDefault", func(t *testing.T) {
					stmt := ParseStatementOrFail(t, `CREATE TABLE tbl (col1 TEXT REFERENCES foo (col2) ON DELETE CASCADE)`).(*sql.CreateTableStatement)
					if diff := deepEqual(stmt.Columns[0].Constraints[0], &sql.ForeignKeyConstraint{
						ForeignTable: &sql.Ident{Name: "foo"},
						ForeignColumns: []*sql.Ident{
							{Name: "col2"},
						},
						Args: []*sql.ForeignKeyArg{
							{
								OnDelete: pos(53),
								Cascade:  pos(60),
							},
						},
					}); diff != "" {
						t.Fatal(diff)
					}
				})
				t.Run("OnDeleteSetRestrict", func(t *testing.T) {
					stmt := ParseStatementOrFail(t, `CREATE TABLE tbl (col1 TEXT REFERENCES foo (col2) ON DELETE RESTRICT)`).(*sql.CreateTableStatement)
					if diff := deepEqual(stmt.Columns[0].Constraints[0], &sql.ForeignKeyConstraint{
						ForeignTable: &sql.Ident{Name: "foo"},
						ForeignColumns: []*sql.Ident{
							{Name: "col2"},
						},
						Args: []*sql.ForeignKeyArg{
							{
								OnDelete: pos(53),
								Restrict: pos(60),
							},
						},
					}); diff != "" {
						t.Fatal(diff)
					}
				})
				t.Run("OnDeleteSetNoAction", func(t *testing.T) {
					stmt := ParseStatementOrFail(t, `CREATE TABLE tbl (col1 TEXT REFERENCES foo (col2) ON DELETE NO ACTION)`).(*sql.CreateTableStatement)
					if diff := deepEqual(stmt.Columns[0].Constraints[0], &sql.ForeignKeyConstraint{
						ForeignTable: &sql.Ident{Name: "foo"},
						ForeignColumns: []*sql.Ident{
							{Name: "col2"},
						},
						Args: []*sql.ForeignKeyArg{
							{
								OnDelete: pos(53),
								NoAction: pos(63),
							},
						},
					}); diff != "" {
						t.Fatal(diff)
					}
				})
				t.Run("Multiple", func(t *testing.T) {
					stmt := ParseStatementOrFail(t, `CREATE TABLE tbl (col1 TEXT REFERENCES foo (col2) ON DELETE CASCADE ON UPDATE RESTRICT)`).(*sql.CreateTableStatement)
					if diff := deepEqual(stmt.Columns[0].Constraints[0], &sql.ForeignKeyConstraint{
						ForeignTable: &sql.Ident{Name: "foo"},
						ForeignColumns: []*sql.Ident{
							{Name: "col2"},
						},
						Args: []*sql.ForeignKeyArg{
							{
								OnDelete: pos(53),
								Cascade:  pos(60),
							},
							{
								OnUpdate: pos(71),
								Restrict: pos(78),
							},
						},
					}); diff != "" {
						t.Fatal(diff)
					}
				})
				t.Run("Deferrable", func(t *testing.T) {
					stmt := ParseStatementOrFail(t, `CREATE TABLE tbl (col1 TEXT REFERENCES foo (col2) DEFERRABLE)`).(*sql.CreateTableStatement)
					if diff := deepEqual(stmt.Columns[0].Constraints[0], &sql.ForeignKeyConstraint{
						ForeignTable: &sql.Ident{Name: "foo"},
						ForeignColumns: []*sql.Ident{
							{Name: "col2"},
						},
						Deferrable: pos(50),
					}); diff != "" {
						t.Fatal(diff)
					}
				})
				t.Run("NotDeferrable", func(t *testing.T) {
					stmt := ParseStatementOrFail(t, `CREATE TABLE tbl (col1 TEXT REFERENCES foo (col2) NOT DEFERRABLE)`).(*sql.CreateTableStatement)
					if diff := deepEqual(stmt.Columns[0].Constraints[0], &sql.ForeignKeyConstraint{
						ForeignTable: &sql.Ident{Name: "foo"},
						ForeignColumns: []*sql.Ident{
							{Name: "col2"},
						},
						NotDeferrable: pos(54),
					}); diff != "" {
						t.Fatal(diff)
					}
				})
				t.Run("InitiallyDeferred", func(t *testing.T) {
					stmt := ParseStatementOrFail(t, `CREATE TABLE tbl (col1 TEXT REFERENCES foo (col2) DEFERRABLE INITIALLY DEFERRED)`).(*sql.CreateTableStatement)
					if diff := deepEqual(stmt.Columns[0].Constraints[0], &sql.ForeignKeyConstraint{
						ForeignTable: &sql.Ident{Name: "foo"},
						ForeignColumns: []*sql.Ident{
							{Name: "col2"},
						},
						Deferrable:        pos(50),
						InitiallyDeferred: pos(71),
					}); diff != "" {
						t.Fatal(diff)
					}
				})
				t.Run("InitiallyImmediate", func(t *testing.T) {
					stmt := ParseStatementOrFail(t, `CREATE TABLE tbl (col1 TEXT REFERENCES foo (col2) DEFERRABLE INITIALLY IMMEDIATE)`).(*sql.CreateTableStatement)
					if diff := deepEqual(stmt.Columns[0].Constraints[0], &sql.ForeignKeyConstraint{
						ForeignTable: &sql.Ident{Name: "foo"},
						ForeignColumns: []*sql.Ident{
							{Name: "col2"},
						},
						Deferrable:         pos(50),
						InitiallyImmediate: pos(71),
					}); diff != "" {
						t.Fatal(diff)
					}
				})
			})
		})

		t.Run("TableConstraint", func(t *testing.T) {
			t.Run("PrimaryKey", func(t *testing.T) {
				AssertParseStatement(t, `CREATE TABLE tbl (col1 TEXT, PRIMARY KEY (col1, col2))`, &sql.CreateTableStatement{
					Name: &sql.QualifiedName{Name: &sql.Ident{Name: "tbl"}},
					Columns: []*sql.ColumnDefinition{
						{
							Name: &sql.Ident{Name: "col1"},
							Type: &sql.Type{
								Name: &sql.Ident{Name: "TEXT"},
							},
						},
					},
					Constraints: []sql.Constraint{
						&sql.PrimaryKeyConstraint{
							Columns: []*sql.Ident{
								{Name: "col1"},
								{Name: "col2"},
							},
						},
					},
				})

				AssertParseStatementError(t, `CREATE TABLE tbl (col1 TEXT, PRIMARY`, `1:37: expected KEY, found 'EOF'`)
				AssertParseStatementError(t, `CREATE TABLE tbl (col1 TEXT, PRIMARY KEY`, `1:41: expected left paren, found 'EOF'`)
				AssertParseStatementError(t, `CREATE TABLE tbl (col1 TEXT, PRIMARY KEY (col1)`, `1:48: expected right paren, found 'EOF'`)
				AssertParseStatementError(t, `CREATE TABLE tbl (col1 TEXT, PRIMARY KEY (1`, `1:43: expected column name, found 1`)
				AssertParseStatementError(t, `CREATE TABLE tbl (col1 TEXT, PRIMARY KEY (foo x`, `1:47: expected comma or right paren, found x`)
			})
			t.Run("Unique", func(t *testing.T) {
				AssertParseStatement(t, `CREATE TABLE tbl (col1 TEXT, CONSTRAINT con1 UNIQUE (col1, col2 COLLATE NOCASE))`, &sql.CreateTableStatement{
					Name: &sql.QualifiedName{Name: &sql.Ident{Name: "tbl"}},
					Columns: []*sql.ColumnDefinition{
						{
							Name: &sql.Ident{Name: "col1"},
							Type: &sql.Type{
								Name: &sql.Ident{Name: "TEXT"},
							},
						},
					},
					Constraints: []sql.Constraint{
						&sql.UniqueConstraint{
							Name: &sql.Ident{Name: "con1"},
							Columns: []*sql.IndexedColumn{
								{X: &sql.Ident{Name: "col1"}},
								{
									X: &sql.BinaryExpr{
										X:  &sql.Ident{Name: "col2"},
										Op: sql.OP_COLLATE,
										Y:  &sql.Ident{Name: "NOCASE"},
									},
								},
							},
						},
					},
				})
				AssertParseStatementError(t, `CREATE TABLE tbl (col1 TEXT, UNIQUE`, `1:36: expected left paren, found 'EOF'`)
				AssertParseStatementError(t, `CREATE TABLE tbl (col1 TEXT, UNIQUE (1`, `1:39: expected comma or right paren, found 'EOF'`)
				AssertParseStatementError(t, `CREATE TABLE tbl (col1 TEXT, UNIQUE (x y`, `1:40: expected comma or right paren, found y`)
			})
			t.Run("Check", func(t *testing.T) {
				AssertParseStatement(t, `CREATE TABLE tbl (col1 TEXT, CHECK(foo = bar))`, &sql.CreateTableStatement{
					Name: &sql.QualifiedName{Name: &sql.Ident{Name: "tbl"}},
					Columns: []*sql.ColumnDefinition{
						{
							Name: &sql.Ident{Name: "col1"},
							Type: &sql.Type{
								Name: &sql.Ident{Name: "TEXT"},
							},
						},
					},
					Constraints: []sql.Constraint{
						&sql.CheckConstraint{
							Expr: &sql.BinaryExpr{
								X:  &sql.Ident{Name: "foo"},
								Op: sql.OP_EQ,
								Y:  &sql.Ident{Name: "bar"},
							},
						},
					},
				})

				AssertParseStatementError(t, `CREATE TABLE tbl (col1 TEXT, CHECK`, `1:35: expected left paren, found 'EOF'`)
				AssertParseStatementError(t, `CREATE TABLE tbl (col1 TEXT, CHECK (TABLE`, `1:37: expected expression, found 'TABLE'`)
				AssertParseStatementError(t, `CREATE TABLE tbl (col1 TEXT, CHECK (true`, `1:41: expected right paren, found 'EOF'`)
			})
			t.Run("ForeignKey", func(t *testing.T) {
				AssertParseStatement(t, `CREATE TABLE tbl (col1 TEXT, FOREIGN KEY (col1, col2) REFERENCES tbl2 (x, y))`, &sql.CreateTableStatement{
					Name: &sql.QualifiedName{Name: &sql.Ident{Name: "tbl"}},
					Columns: []*sql.ColumnDefinition{
						{
							Name: &sql.Ident{Name: "col1"},
							Type: &sql.Type{
								Name: &sql.Ident{Name: "TEXT"},
							},
						},
					},
					Constraints: []sql.Constraint{
						&sql.ForeignKeyConstraint{
							Columns: []*sql.Ident{
								{Name: "col1"},
								{Name: "col2"},
							},
							ForeignTable: &sql.Ident{Name: "tbl2"},
							ForeignColumns: []*sql.Ident{
								{Name: "x"},
								{Name: "y"},
							},
						},
					},
				})

				AssertParseStatementError(t, `CREATE TABLE tbl (col1 TEXT, FOREIGN`, `1:37: expected KEY, found 'EOF'`)
				AssertParseStatementError(t, `CREATE TABLE tbl (col1 TEXT, FOREIGN KEY`, `1:41: expected left paren, found 'EOF'`)
				AssertParseStatementError(t, `CREATE TABLE tbl (col1 TEXT, FOREIGN KEY (`, `1:43: expected column name, found 'EOF'`)
				AssertParseStatementError(t, `CREATE TABLE tbl (col1 TEXT, FOREIGN KEY (x`, `1:44: expected comma or right paren, found 'EOF'`)
				AssertParseStatementError(t, `CREATE TABLE tbl (col1 TEXT, FOREIGN KEY (x)`, `1:45: expected REFERENCES, found 'EOF'`)
				AssertParseStatementError(t, `CREATE TABLE tbl (col1 TEXT, FOREIGN KEY (x) REFERENCES`, `1:56: expected foreign table name, found 'EOF'`)
				AssertParseStatementError(t, `CREATE TABLE tbl (col1 TEXT, FOREIGN KEY (x) REFERENCES tbl`, `1:60: expected right paren, found 'EOF'`)
				AssertParseStatementError(t, `CREATE TABLE tbl (col1 TEXT, FOREIGN KEY (x) REFERENCES tbl (`, `1:62: expected foreign column name, found 'EOF'`)
				AssertParseStatementError(t, `CREATE TABLE tbl (col1 TEXT, FOREIGN KEY (x) REFERENCES tbl (x`, `1:63: expected comma or right paren, found 'EOF'`)
				AssertParseStatementError(t, `CREATE TABLE tbl (col1 TEXT, FOREIGN KEY (x) REFERENCES tbl (x) ON`, `1:67: expected UPDATE or DELETE, found 'EOF'`)
				AssertParseStatementError(t, `CREATE TABLE tbl (col1 TEXT, FOREIGN KEY (x) REFERENCES tbl (x) ON UPDATE SET`, `1:78: expected NULL or DEFAULT, found 'EOF'`)
				AssertParseStatementError(t, `CREATE TABLE tbl (col1 TEXT, FOREIGN KEY (x) REFERENCES tbl (x) ON UPDATE NO`, `1:77: expected ACTION, found 'EOF'`)
				AssertParseStatementError(t, `CREATE TABLE tbl (col1 TEXT, FOREIGN KEY (x) REFERENCES tbl (x) ON UPDATE TABLE`, `1:75: expected SET NULL, SET DEFAULT, CASCADE, RESTRICT, or NO ACTION, found 'TABLE'`)
				AssertParseStatementError(t, `CREATE TABLE tbl (col1 TEXT, FOREIGN KEY (x) REFERENCES tbl (x) ON UPDATE CASCADE NOT`, `1:86: expected DEFERRABLE, found 'EOF'`)
			})
		})
	})

	t.Run("CreateVirtualTable", func(t *testing.T) {
		AssertParseStatement(t, `CREATE VIRTUAL TABLE vtbl USING mdl`, &sql.CreateVirtualTableStatement{
			Name:       &sql.QualifiedName{Name: &sql.Ident{Name: "vtbl"}},
			ModuleName: &sql.Ident{Name: "mdl"},
		})

		AssertParseStatementError(t, `CREATE VIRTUAL TABLE vtbl`, "1:26: expected USING, found 'EOF'")
		AssertParseStatementError(t, `CREATE VIRTUAL TABLE vtbl USING`, "1:32: expected module name, found 'EOF'")

		t.Run("WithSchemaQualifiedTable", func(t *testing.T) {
			AssertParseStatement(t, `CREATE VIRTUAL TABLE schm.vtbl USING mdl`, &sql.CreateVirtualTableStatement{
				Name: &sql.QualifiedName{
					Schema: &sql.Ident{Name: "schm"},
					Name:   &sql.Ident{Name: "vtbl"},
				},
				ModuleName: &sql.Ident{Name: "mdl"},
			})
			AssertParseStatementError(t, `CREATE VIRTUAL TABLE schm.`, "1:27: expected qualified name, found 'EOF'")
			AssertParseStatementError(t, `CREATE VIRTUAL TABLE schm.vtbl`, "1:31: expected USING, found 'EOF'")
		})

		t.Run("WithIfNotExists", func(t *testing.T) {
			AssertParseStatement(t, `CREATE VIRTUAL TABLE IF NOT EXISTS vtbl USING mdl`, &sql.CreateVirtualTableStatement{
				IfNotExists: pos(28),
				Name:        &sql.QualifiedName{Name: &sql.Ident{Name: "vtbl"}},
				ModuleName:  &sql.Ident{Name: "mdl"},
			})
			AssertParseStatementError(t, `CREATE VIRTUAL TABLE IF`, "1:24: expected NOT, found 'EOF'")
			AssertParseStatementError(t, `CREATE VIRTUAL TABLE IF NOT`, "1:28: expected EXISTS, found 'EOF'")
			AssertParseStatementError(t, `CREATE VIRTUAL TABLE IF NOT EXIST`, "1:29: expected EXISTS, found EXIST")
			AssertParseStatementError(t, `CREATE VIRTUAL TABLE IF NOT EXISTS`, "1:35: expected qualified name, found 'EOF'")
		})

		t.Run("WithArguments", func(t *testing.T) {
			AssertParseStatement(t, `CREATE VIRTUAL TABLE vtbl USING mdl(arg1)`, &sql.CreateVirtualTableStatement{
				Name:       &sql.QualifiedName{Name: &sql.Ident{Name: "vtbl"}},
				ModuleName: &sql.Ident{Name: "mdl"},
				Arguments: []*sql.ModuleArgument{
					{Name: &sql.Ident{Name: "arg1"}},
				},
			})
			AssertParseStatement(t, `CREATE VIRTUAL TABLE vtbl USING mdl(arg1,arg2='a',"arg3"=false)`, &sql.CreateVirtualTableStatement{
				Name:       &sql.QualifiedName{Name: &sql.Ident{Name: "vtbl"}},
				ModuleName: &sql.Ident{Name: "mdl"},
				Arguments: []*sql.ModuleArgument{
					{
						Name: &sql.Ident{Name: "arg1"},
					},
					{
						Name:    &sql.Ident{Name: "arg2"},
						Literal: &sql.StringLit{Value: "a"},
					},
					{
						Name:    &sql.Ident{Name: "arg3", Quoted: true},
						Literal: &sql.BoolLit{Value: false},
					},
				},
			})
			AssertParseStatement(t, `CREATE VIRTUAL TABLE vtbl USING mdl(arg1 TEXT)`, &sql.CreateVirtualTableStatement{
				Name:       &sql.QualifiedName{Name: &sql.Ident{Name: "vtbl"}},
				ModuleName: &sql.Ident{Name: "mdl"},
				Arguments: []*sql.ModuleArgument{
					{
						Name: &sql.Ident{Name: "arg1"},
						Type: &sql.Type{Name: &sql.Ident{Name: "TEXT"}},
					},
				},
			})

			AssertParseStatementError(t, `CREATE VIRTUAL TABLE vtbl USING mdl(`, "1:37: expected module argument name, found 'EOF'")
			AssertParseStatementError(t, `CREATE VIRTUAL TABLE vtbl USING mdl(arg1`, "1:41: expected comma or right paren, found 'EOF'")
			AssertParseStatementError(t, `CREATE VIRTUAL TABLE vtbl USING mdl(arg1=3`, "1:43: expected comma or right paren, found 'EOF'")
			AssertParseStatementError(t, `CREATE VIRTUAL TABLE vtbl USING mdl(arg1=3,`, "1:44: expected module argument name, found 'EOF'")
			AssertParseStatementError(t, `CREATE VIRTUAL TABLE vtbl USING mdl()`, "1:37: expected module argument name, found ')'")
			AssertParseStatementError(t, `CREATE VIRTUAL TABLE vtbl USING mdl(arg1 BLOB`, "1:46: expected comma or right paren, found 'EOF'")
			AssertParseStatementError(t, `CREATE VIRTUAL TABLE vtbl USING mdl(arg1 arg2)`, "1:42: expected comma or right paren, found arg2")
			AssertParseStatementError(t, `CREATE VIRTUAL TABLE vtbl USING mdl(arg1 TEXT=value)`, "1:46: expected comma or right paren, found '='")
			AssertParseStatementError(t, `CREATE VIRTUAL TABLE vtbl USING mdl(=)`, "1:37: expected module argument name, found '='")
			AssertParseStatementError(t, `CREATE VIRTUAL TABLE vtbl USING mdl(key=)`, "1:41: expected expression, found ')'")
			AssertParseStatementError(t, `CREATE VIRTUAL TABLE vtbl USING mdl(=value)`, "1:37: expected module argument name, found '='")
		})
	})

	t.Run("DropTable", func(t *testing.T) {
		AssertParseStatement(t, `DROP TABLE vw`, &sql.DropTableStatement{
			Name: &sql.QualifiedName{Name: &sql.Ident{Name: "vw"}},
		})
		AssertParseStatement(t, `DROP TABLE IF EXISTS vw`, &sql.DropTableStatement{
			IfExists: pos(14),
			Name:     &sql.QualifiedName{Name: &sql.Ident{Name: "vw"}},
		})
		AssertParseStatementError(t, `DROP TABLE`, `1:11: expected qualified name, found 'EOF'`)
		AssertParseStatementError(t, `DROP TABLE IF`, `1:14: expected EXISTS, found 'EOF'`)
		AssertParseStatementError(t, `DROP TABLE IF EXISTS`, `1:21: expected qualified name, found 'EOF'`)
	})

	t.Run("CreateView", func(t *testing.T) {
		AssertParseStatement(t, `CREATE VIEW vw (col1, col2) AS SELECT x, y`, &sql.CreateViewStatement{
			Name: &sql.QualifiedName{Name: &sql.Ident{Name: "vw"}},
			Columns: []*sql.Ident{
				{Name: "col1"},
				{Name: "col2"},
			},
			Select: &sql.SelectStatement{
				Columns: []*sql.ResultColumn{
					{Expr: &sql.Ident{Name: "x"}},
					{Expr: &sql.Ident{Name: "y"}},
				},
			},
		})
		AssertParseStatement(t, `CREATE VIEW vw AS SELECT x`, &sql.CreateViewStatement{
			Name: &sql.QualifiedName{Name: &sql.Ident{Name: "vw"}},
			Select: &sql.SelectStatement{
				Columns: []*sql.ResultColumn{
					{Expr: &sql.Ident{Name: "x"}},
				},
			},
		})
		AssertParseStatement(t, `CREATE VIEW IF NOT EXISTS vw AS SELECT x`, &sql.CreateViewStatement{
			IfNotExists: pos(19),
			Name:        &sql.QualifiedName{Name: &sql.Ident{Name: "vw"}},
			Select: &sql.SelectStatement{
				Columns: []*sql.ResultColumn{
					{Expr: &sql.Ident{Name: "x"}},
				},
			},
		})
		AssertParseStatementError(t, `CREATE VIEW`, `1:12: expected qualified name, found 'EOF'`)
		AssertParseStatementError(t, `CREATE VIEW IF`, `1:15: expected NOT, found 'EOF'`)
		AssertParseStatementError(t, `CREATE VIEW IF NOT`, `1:19: expected EXISTS, found 'EOF'`)
		AssertParseStatementError(t, `CREATE VIEW vw`, `1:15: expected AS, found 'EOF'`)
		AssertParseStatementError(t, `CREATE VIEW vw (`, `1:17: expected column name, found 'EOF'`)
		AssertParseStatementError(t, `CREATE VIEW vw (x`, `1:18: expected comma or right paren, found 'EOF'`)
		AssertParseStatementError(t, `CREATE VIEW vw AS`, `1:18: expected SELECT or VALUES, found 'EOF'`)
		AssertParseStatementError(t, `CREATE VIEW vw AS SELECT`, `1:25: expected expression, found 'EOF'`)
	})

	t.Run("DropView", func(t *testing.T) {
		AssertParseStatement(t, `DROP VIEW vw`, &sql.DropViewStatement{
			Name: &sql.QualifiedName{Name: &sql.Ident{Name: "vw"}},
		})
		AssertParseStatement(t, `DROP VIEW IF EXISTS vw`, &sql.DropViewStatement{
			IfExists: pos(13),
			Name:     &sql.QualifiedName{Name: &sql.Ident{Name: "vw"}},
		})
		AssertParseStatementError(t, `DROP`, `1:1: expected TABLE, VIEW, INDEX, or TRIGGER`)
		AssertParseStatementError(t, `DROP VIEW`, `1:10: expected qualified name, found 'EOF'`)
		AssertParseStatementError(t, `DROP VIEW IF`, `1:13: expected EXISTS, found 'EOF'`)
		AssertParseStatementError(t, `DROP VIEW IF EXISTS`, `1:20: expected qualified name, found 'EOF'`)
	})

	t.Run("CreateIndex", func(t *testing.T) {
		AssertParseStatement(t, `CREATE INDEX idx ON tbl (x ASC, y DESC, z)`, &sql.CreateIndexStatement{
			Name:  &sql.QualifiedName{Name: &sql.Ident{Name: "idx"}},
			Table: &sql.Ident{Name: "tbl"},
			Columns: []*sql.IndexedColumn{
				{X: &sql.Ident{Name: "x"}, Asc: pos(27)},
				{X: &sql.Ident{Name: "y"}, Desc: pos(34)},
				{X: &sql.Ident{Name: "z"}},
			},
		})
		AssertParseStatement(t, `CREATE UNIQUE INDEX idx ON tbl (x)`, &sql.CreateIndexStatement{
			Unique: pos(7),
			Name:   &sql.QualifiedName{Name: &sql.Ident{Name: "idx"}},
			Table:  &sql.Ident{Name: "tbl"},
			Columns: []*sql.IndexedColumn{
				{X: &sql.Ident{Name: "x"}},
			},
		})
		AssertParseStatement(t, `CREATE INDEX idx ON tbl (x) WHERE true`, &sql.CreateIndexStatement{
			Name:  &sql.QualifiedName{Name: &sql.Ident{Name: "idx"}},
			Table: &sql.Ident{Name: "tbl"},
			Columns: []*sql.IndexedColumn{
				{X: &sql.Ident{Name: "x"}},
			},
			WhereExpr: &sql.BoolLit{Value: true},
		})
		AssertParseStatement(t, `CREATE INDEX IF NOT EXISTS idx ON tbl (x)`, &sql.CreateIndexStatement{
			IfNotExists: pos(20),
			Name:        &sql.QualifiedName{Name: &sql.Ident{Name: "idx"}},
			Table:       &sql.Ident{Name: "tbl"},
			Columns: []*sql.IndexedColumn{
				{X: &sql.Ident{Name: "x"}},
			},
		})
		AssertParseStatement(t, `CREATE INDEX idx ON tbl (x COLLATE NOCASE)`, &sql.CreateIndexStatement{
			Name:  &sql.QualifiedName{Name: &sql.Ident{Name: "idx"}},
			Table: &sql.Ident{Name: "tbl"},
			Columns: []*sql.IndexedColumn{
				{
					X: &sql.BinaryExpr{
						X:  &sql.Ident{Name: "x"},
						Op: sql.OP_COLLATE,
						Y:  &sql.Ident{Name: "NOCASE"},
					},
				},
			},
		})
		AssertParseStatementError(t, `CREATE UNIQUE`, `1:14: expected INDEX, found 'EOF'`)
		AssertParseStatementError(t, `CREATE INDEX`, `1:13: expected qualified name, found 'EOF'`)
		AssertParseStatementError(t, `CREATE INDEX IF`, `1:16: expected NOT, found 'EOF'`)
		AssertParseStatementError(t, `CREATE INDEX IF NOT`, `1:20: expected EXISTS, found 'EOF'`)
		AssertParseStatementError(t, `CREATE INDEX idx`, `1:17: expected ON, found 'EOF'`)
		AssertParseStatementError(t, `CREATE INDEX idx ON`, `1:20: expected table name, found 'EOF'`)
		AssertParseStatementError(t, `CREATE INDEX idx ON tbl`, `1:24: expected left paren, found 'EOF'`)
		AssertParseStatementError(t, `CREATE INDEX idx ON tbl (`, `1:26: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `CREATE INDEX idx ON tbl (x`, `1:27: expected comma or right paren, found 'EOF'`)
		AssertParseStatementError(t, `CREATE INDEX idx ON tbl (x) WHERE`, `1:34: expected expression, found 'EOF'`)
	})

	t.Run("DropIndex", func(t *testing.T) {
		AssertParseStatement(t, `DROP INDEX idx`, &sql.DropIndexStatement{
			Name: &sql.QualifiedName{Name: &sql.Ident{Name: "idx"}},
		})
		AssertParseStatement(t, `DROP INDEX IF EXISTS idx`, &sql.DropIndexStatement{
			IfExists: pos(14),
			Name:     &sql.QualifiedName{Name: &sql.Ident{Name: "idx"}},
		})
		AssertParseStatementError(t, `DROP INDEX`, `1:11: expected qualified name, found 'EOF'`)
		AssertParseStatementError(t, `DROP INDEX IF`, `1:14: expected EXISTS, found 'EOF'`)
		AssertParseStatementError(t, `DROP INDEX IF EXISTS`, `1:21: expected qualified name, found 'EOF'`)
	})

	t.Run("CreateTrigger", func(t *testing.T) {
		AssertParseStatement(t, `CREATE TRIGGER IF NOT EXISTS trig BEFORE INSERT ON tbl BEGIN DELETE FROM new; END`, &sql.CreateTriggerStatement{
			IfNotExists: pos(22),
			Name:        &sql.QualifiedName{Name: &sql.Ident{Name: "trig"}},
			Before:      pos(34),
			Insert:      pos(41),
			Table:       &sql.Ident{Name: "tbl"},
			Body: []sql.Statement{
				&sql.DeleteStatement{
					Table: &sql.QualifiedName{
						Name: &sql.Ident{Name: "new"},
					},
				},
			},
		})
		AssertParseStatement(t, `CREATE TRIGGER trig INSTEAD OF UPDATE ON tbl BEGIN SELECT *; END`, &sql.CreateTriggerStatement{
			Name:      &sql.QualifiedName{Name: &sql.Ident{Name: "trig"}},
			InsteadOf: pos(28),
			Update:    pos(31),
			Table:     &sql.Ident{Name: "tbl"},
			Body: []sql.Statement{
				&sql.SelectStatement{
					Columns: []*sql.ResultColumn{{Star: pos(58)}},
				},
			},
		})
		AssertParseStatement(t, `CREATE TRIGGER trig INSTEAD OF UPDATE OF x, y ON tbl FOR EACH ROW WHEN true BEGIN SELECT *; END`, &sql.CreateTriggerStatement{
			Name:      &sql.QualifiedName{Name: &sql.Ident{Name: "trig"}},
			InsteadOf: pos(28),
			Update:    pos(31),
			UpdateOfColumns: []*sql.Ident{
				{Name: "x"},
				{Name: "y"},
			},
			Table:      &sql.Ident{Name: "tbl"},
			ForEachRow: pos(62),
			WhenExpr:   &sql.BoolLit{Value: true},
			Body: []sql.Statement{
				&sql.SelectStatement{
					Columns: []*sql.ResultColumn{{Star: pos(89)}},
				},
			},
		})
		AssertParseStatement(t, `CREATE TRIGGER trig AFTER UPDATE ON tbl BEGIN WITH cte (x) AS (SELECT y) SELECT *; END`, &sql.CreateTriggerStatement{
			Name:   &sql.QualifiedName{Name: &sql.Ident{Name: "trig"}},
			After:  pos(20),
			Update: pos(26),
			Table:  &sql.Ident{Name: "tbl"},
			Body: []sql.Statement{
				&sql.SelectStatement{
					WithClause: &sql.WithClause{
						CTEs: []*sql.CTE{
							{
								TableName: &sql.Ident{Name: "cte"},
								Columns: []*sql.Ident{
									{Name: "x"},
								},
								Select: &sql.SelectStatement{
									Columns: []*sql.ResultColumn{
										{Expr: &sql.Ident{Name: "y"}},
									},
								},
							},
						},
					},
					Columns: []*sql.ResultColumn{{Star: pos(80)}},
				},
			},
		})

		AssertParseStatementError(t, `CREATE TRIGGER`, `1:15: expected qualified name, found 'EOF'`)
		AssertParseStatementError(t, `CREATE TRIGGER IF`, `1:18: expected NOT, found 'EOF'`)
		AssertParseStatementError(t, `CREATE TRIGGER IF NOT`, `1:22: expected EXISTS, found 'EOF'`)
		AssertParseStatementError(t, `CREATE TRIGGER trig INSTEAD`, `1:28: expected OF, found 'EOF'`)
		AssertParseStatementError(t, `CREATE TRIGGER trig AFTER`, `1:26: expected DELETE, INSERT, or UPDATE, found 'EOF'`)
		AssertParseStatementError(t, `CREATE TRIGGER trig UPDATE OF`, `1:30: expected column name, found 'EOF'`)
		AssertParseStatementError(t, `CREATE TRIGGER trig UPDATE OF x,`, `1:33: expected column name, found 'EOF'`)
		AssertParseStatementError(t, `CREATE TRIGGER trig AFTER INSERT`, `1:33: expected ON, found 'EOF'`)
		AssertParseStatementError(t, `CREATE TRIGGER trig AFTER INSERT ON `, `1:37: expected table name, found 'EOF'`)
		AssertParseStatementError(t, `CREATE TRIGGER trig AFTER INSERT ON tbl FOR`, `1:44: expected EACH, found 'EOF'`)
		AssertParseStatementError(t, `CREATE TRIGGER trig AFTER INSERT ON tbl FOR EACH`, `1:49: expected ROW, found 'EOF'`)
		AssertParseStatementError(t, `CREATE TRIGGER trig AFTER INSERT ON tbl WHEN`, `1:45: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `CREATE TRIGGER trig AFTER INSERT ON tbl`, `1:40: expected BEGIN, found 'EOF'`)
		AssertParseStatementError(t, `CREATE TRIGGER trig AFTER INSERT ON tbl BEGIN`, `1:46: expected statement, found 'EOF'`)
		AssertParseStatementError(t, `CREATE TRIGGER trig AFTER INSERT ON tbl BEGIN SELECT`, `1:53: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `CREATE TRIGGER trig AFTER INSERT ON tbl BEGIN SELECT *`, `1:55: expected semicolon, found 'EOF'`)
		AssertParseStatementError(t, `CREATE TRIGGER trig AFTER INSERT ON tbl BEGIN SELECT *;`, `1:56: expected statement, found 'EOF'`)
		AssertParseStatementError(t, `CREATE TRIGGER trig DELETE ON tbl BEGIN INSERT INTO new DEFAULT VALUES; UPDATE new SET x = 1; END`, `1:57: expected non-DEFAULT VALUES, found 'DEFAULT'`)
		AssertParseStatementError(t, `CREATE TRIGGER trig AFTER INSERT ON foo BEGIN UPDATE baz AS b SET x = 1 WHERE NEW.id = 1; END;;`, `1:58: expected SET, found 'AS'`)
		AssertParseStatementError(t, `CREATE TRIGGER trig AFTER INSERT ON foo BEGIN UPDATE baz b SET x = 1 WHERE NEW.id = 1; END;;`, `1:58: expected SET, found b`)
		AssertParseStatementError(t, `CREATE TRIGGER trig AFTER INSERT ON foo BEGIN UPDATE baz INDEXED BY id SET x = 1 WHERE NEW.id = 1; END;;`, `1:58: expected SET, found 'INDEXED'`)
		AssertParseStatementError(t, `CREATE TRIGGER trig AFTER INSERT ON foo BEGIN DELETE FROM baz AS b WHERE NEW.id = 1; END;;`, `1:63: expected semicolon, found 'AS'`)
		AssertParseStatementError(t, `CREATE TRIGGER trig AFTER INSERT ON foo BEGIN DELETE FROM baz b WHERE NEW.id = 1; END;;`, `1:63: expected semicolon, found b`)
	})

	t.Run("DropTrigger", func(t *testing.T) {
		AssertParseStatement(t, `DROP TRIGGER trig`, &sql.DropTriggerStatement{
			Name: &sql.QualifiedName{Name: &sql.Ident{Name: "trig"}},
		})
		AssertParseStatement(t, `DROP TRIGGER IF EXISTS trig`, &sql.DropTriggerStatement{
			IfExists: pos(16),
			Name:     &sql.QualifiedName{Name: &sql.Ident{Name: "trig"}},
		})
		AssertParseStatementError(t, `DROP TRIGGER`, `1:13: expected qualified name, found 'EOF'`)
		AssertParseStatementError(t, `DROP TRIGGER IF`, `1:16: expected EXISTS, found 'EOF'`)
		AssertParseStatementError(t, `DROP TRIGGER IF EXISTS`, `1:23: expected qualified name, found 'EOF'`)
	})

	t.Run("Select", func(t *testing.T) {
		AssertParseStatement(t, `SELECT 5678`, &sql.SelectStatement{
			Columns: []*sql.ResultColumn{
				{
					Expr: &sql.NumberLit{Value: "5678"},
				},
			},
		})

		AssertParseStatement(t, `SELECT datetime('now')`, &sql.SelectStatement{
			Columns: []*sql.ResultColumn{
				{
					Expr: &sql.Call{
						Name: &sql.QualifiedName{
							Name: &sql.Ident{
								Name: "datetime",
							},
							FunctionCall: true,
							FunctionArgs: []*sql.FunctionArg{
								{Expr: &sql.StringLit{Value: "now"}},
							},
						},
					},
				},
			},
		})

		AssertParseStatement(t, `SELECT julianday('now')`, &sql.SelectStatement{
			Columns: []*sql.ResultColumn{
				{
					Expr: &sql.Call{
						Name: &sql.QualifiedName{
							Name:         &sql.Ident{Name: "julianday"},
							FunctionCall: true,
							FunctionArgs: []*sql.FunctionArg{
								{Expr: &sql.StringLit{Value: "now"}},
							},
						},
					},
				},
			},
		})

		AssertParseStatement(t, `SELECT date('now','start of month','+1 month','-1 day')`, &sql.SelectStatement{
			Columns: []*sql.ResultColumn{
				{
					Expr: &sql.Call{
						Name: &sql.QualifiedName{
							Name:         &sql.Ident{Name: "date"},
							FunctionCall: true,
							FunctionArgs: []*sql.FunctionArg{
								{Expr: &sql.StringLit{Value: "now"}},
								{Expr: &sql.StringLit{Value: "start of month"}},
								{Expr: &sql.StringLit{Value: "+1 month"}},
								{Expr: &sql.StringLit{Value: "-1 day"}},
							},
						},
					},
				},
			},
		})

		AssertParseStatement(t, `SELECT like(NULL, FALSE);`, &sql.SelectStatement{
			Columns: []*sql.ResultColumn{
				{
					Expr: &sql.Call{
						Name: &sql.QualifiedName{
							Name:         &sql.Ident{Name: "like"},
							FunctionCall: true,
							FunctionArgs: []*sql.FunctionArg{
								{Expr: &sql.NullLit{}},
								{Expr: &sql.BoolLit{Value: false}},
							},
						},
					},
				},
			},
		})

		AssertParseStatement(t, `SELECT glob('*.txt', 'file.txt');`, &sql.SelectStatement{
			Columns: []*sql.ResultColumn{
				{
					Expr: &sql.Call{
						Name: &sql.QualifiedName{
							Name:         &sql.Ident{Name: "glob"},
							FunctionCall: true,
							FunctionArgs: []*sql.FunctionArg{
								{Expr: &sql.StringLit{Value: "*.txt"}},
								{Expr: &sql.StringLit{Value: "file.txt"}},
							},
						},
					},
				},
			},
		})

		AssertParseStatement(t, `SELECT if(TRUE, 'a', 'b');`, &sql.SelectStatement{
			Columns: []*sql.ResultColumn{
				{
					Expr: &sql.Call{
						Name: &sql.QualifiedName{
							Name:         &sql.Ident{Name: "if"},
							FunctionCall: true,
							FunctionArgs: []*sql.FunctionArg{
								{Expr: &sql.BoolLit{Value: true}},
								{Expr: &sql.StringLit{Value: "a"}},
								{Expr: &sql.StringLit{Value: "b"}},
							},
						},
					},
				},
			},
		})

		AssertParseStatement(t, `SELECT replace(c0, 'a', 1);`, &sql.SelectStatement{
			Columns: []*sql.ResultColumn{
				{
					Expr: &sql.Call{
						Name: &sql.QualifiedName{
							Name:         &sql.Ident{Name: "replace"},
							FunctionCall: true,
							FunctionArgs: []*sql.FunctionArg{
								{Expr: &sql.Ident{Name: "c0"}},
								{Expr: &sql.StringLit{Value: "a"}},
								{Expr: &sql.NumberLit{Value: "1"}},
							},
						},
					},
				},
			},
		})

		AssertParseStatement(t, `SELECT 1 NOT NULL`, &sql.SelectStatement{
			Columns: []*sql.ResultColumn{
				{
					Expr: &sql.Null{
						X:  &sql.NumberLit{Value: "1"},
						Op: sql.OP_NOTNULL,
					},
				},
			},
		})
		AssertParseStatement(t, `SELECT 1 NOTNULL`, &sql.SelectStatement{
			Columns: []*sql.ResultColumn{
				{
					Expr: &sql.Null{
						X:  &sql.NumberLit{Value: "1"},
						Op: sql.OP_NOTNULL,
					},
				},
			},
		})
		AssertParseStatement(t, `SELECT 1 IS NULL`, &sql.SelectStatement{
			Columns: []*sql.ResultColumn{
				{
					Expr: &sql.Null{
						X:  &sql.NumberLit{Value: "1"},
						Op: sql.OP_ISNULL,
					},
				},
			},
		})
		AssertParseStatement(t, `SELECT 1 ISNULL`, &sql.SelectStatement{
			Columns: []*sql.ResultColumn{
				{
					Expr: &sql.Null{
						X:  &sql.NumberLit{Value: "1"},
						Op: sql.OP_ISNULL,
					},
				},
			},
		})
		AssertParseStatement(t, `SELECT 1 IS NULL AND false`, &sql.SelectStatement{
			Columns: []*sql.ResultColumn{
				{
					Expr: &sql.BinaryExpr{
						X: &sql.Null{
							X:  &sql.NumberLit{Value: "1"},
							Op: sql.OP_ISNULL,
						},
						Op: sql.OP_AND,
						Y:  &sql.BoolLit{Value: false},
					},
				},
			},
		})

		AssertParseStatement(t, `SELECT * FROM tbl`, &sql.SelectStatement{
			Columns: []*sql.ResultColumn{
				{Star: pos(7)},
			},
			Source: &sql.QualifiedName{
				Name: &sql.Ident{Name: "tbl"},
			},
		})

		AssertParseStatement(t, `SELECT * FROM main.tbl;`, &sql.SelectStatement{
			Columns: []*sql.ResultColumn{
				{Star: pos(7)},
			},
			Source: &sql.QualifiedName{
				Schema: &sql.Ident{Name: "main"},
				Name:   &sql.Ident{Name: "tbl"},
			},
		})

		AssertParseStatement(t, `SELECT DISTINCT * FROM tbl`, &sql.SelectStatement{
			Distinct: pos(7),
			Columns: []*sql.ResultColumn{
				{Star: pos(16)},
			},
			Source: &sql.QualifiedName{
				Name: &sql.Ident{Name: "tbl"},
			},
		})

		AssertParseStatement(t, `SELECT ALL * FROM tbl`, &sql.SelectStatement{
			All: pos(7),
			Columns: []*sql.ResultColumn{
				{Star: pos(11)},
			},
			Source: &sql.QualifiedName{
				Name: &sql.Ident{Name: "tbl"},
			},
		})

		AssertParseStatement(t, `SELECT foo AS FOO, bar baz, tbl.* FROM tbl`, &sql.SelectStatement{
			Columns: []*sql.ResultColumn{
				{
					Expr:  &sql.Ident{Name: "foo"},
					Alias: &sql.Ident{Name: "FOO"},
				},
				{
					Expr:  &sql.Ident{Name: "bar"},
					Alias: &sql.Ident{Name: "baz"},
				},
				{
					Expr: &sql.QualifiedRef{
						Table: &sql.QualifiedName{Name: &sql.Ident{Name: "tbl"}},
						Star:  pos(32),
					},
				},
			},
			Source: &sql.QualifiedName{
				Name: &sql.Ident{Name: "tbl"},
			},
		})
		AssertParseStatement(t, `SELECT * FROM tbl AS tbl2`, &sql.SelectStatement{
			Columns: []*sql.ResultColumn{
				{Star: pos(7)},
			},
			Source: &sql.QualifiedName{
				Name:  &sql.Ident{Name: "tbl"},
				Alias: &sql.Ident{Name: "tbl2"},
			},
		})
		AssertParseStatement(t, `SELECT * FROM tbl AS tbl2`, &sql.SelectStatement{
			Columns: []*sql.ResultColumn{
				{Star: pos(7)},
			},
			Source: &sql.QualifiedName{
				Name:  &sql.Ident{Name: "tbl"},
				Alias: &sql.Ident{Name: "tbl2"},
			},
		})
		AssertParseStatement(t, `SELECT * FROM main.tbl AS tbl2`, &sql.SelectStatement{
			Columns: []*sql.ResultColumn{
				{Star: pos(7)},
			},
			Source: &sql.QualifiedName{
				Schema: &sql.Ident{Name: "main"},
				Name:   &sql.Ident{Name: "tbl"},
				Alias:  &sql.Ident{Name: "tbl2"},
			},
		})
		AssertParseStatement(t, `SELECT * FROM tbl INDEXED BY idx`, &sql.SelectStatement{
			Columns: []*sql.ResultColumn{
				{Star: pos(7)},
			},
			Source: &sql.QualifiedName{
				Name:  &sql.Ident{Name: "tbl"},
				Index: &sql.Ident{Name: "idx"},
			},
		})
		AssertParseStatement(t, `SELECT * FROM tbl NOT INDEXED`, &sql.SelectStatement{
			Columns: []*sql.ResultColumn{
				{Star: pos(7)},
			},
			Source: &sql.QualifiedName{
				Name:       &sql.Ident{Name: "tbl"},
				NotIndexed: pos(22),
			},
		})

		AssertParseStatement(t, `SELECT * FROM (SELECT *) AS tbl`, &sql.SelectStatement{
			Columns: []*sql.ResultColumn{
				{Star: pos(7)},
			},
			Source: &sql.ParenSource{
				X: &sql.SelectStatement{
					Columns: []*sql.ResultColumn{
						{Star: pos(22)},
					},
				},
				Alias: &sql.Ident{Name: "tbl"},
			},
		})

		AssertParseStatement(t, `SELECT * FROM (VALUES (NULL))`, &sql.SelectStatement{
			Columns: []*sql.ResultColumn{
				{Star: pos(7)},
			},
			Source: &sql.ParenSource{
				X: &sql.SelectStatement{
					ValueLists: []*sql.ExprList{
						{
							Exprs: []sql.Expr{
								&sql.NullLit{},
							},
						},
					},
				},
			},
		})
		AssertParseStatement(t, `SELECT * FROM ( t ) a`, &sql.SelectStatement{
			Columns: []*sql.ResultColumn{
				{Star: pos(7)},
			},
			Source: &sql.ParenSource{
				X: &sql.QualifiedName{
					Name: &sql.Ident{Name: "t"},
				},
				Alias: &sql.Ident{Name: "a"},
			},
		})

		AssertParseStatement(t, `SELECT * FROM foo, bar`, &sql.SelectStatement{
			Columns: []*sql.ResultColumn{
				{Star: pos(7)},
			},
			Source: &sql.JoinClause{
				X: &sql.QualifiedName{
					Name: &sql.Ident{Name: "foo"},
				},
				Operator: &sql.JoinOperator{},
				Y: &sql.QualifiedName{
					Name: &sql.Ident{Name: "bar"},
				},
			},
		})
		AssertParseStatement(t, `SELECT * FROM foo JOIN bar`, &sql.SelectStatement{
			Columns: []*sql.ResultColumn{
				{Star: pos(7)},
			},
			Source: &sql.JoinClause{
				X: &sql.QualifiedName{
					Name: &sql.Ident{Name: "foo"},
				},
				Operator: &sql.JoinOperator{},
				Y: &sql.QualifiedName{
					Name: &sql.Ident{Name: "bar"},
				},
			},
		})
		AssertParseStatement(t, `SELECT * FROM foo NATURAL JOIN bar`, &sql.SelectStatement{
			Columns: []*sql.ResultColumn{
				{Star: pos(7)},
			},
			Source: &sql.JoinClause{
				X: &sql.QualifiedName{
					Name: &sql.Ident{Name: "foo"},
				},
				Operator: &sql.JoinOperator{Natural: pos(18)},
				Y: &sql.QualifiedName{
					Name: &sql.Ident{Name: "bar"},
				},
			},
		})
		AssertParseStatement(t, `SELECT * FROM foo INNER JOIN bar ON true`, &sql.SelectStatement{
			Columns: []*sql.ResultColumn{
				{Star: pos(7)},
			},
			Source: &sql.JoinClause{
				X: &sql.QualifiedName{
					Name: &sql.Ident{Name: "foo"},
				},
				Operator: &sql.JoinOperator{Inner: pos(18)},
				Y: &sql.QualifiedName{
					Name: &sql.Ident{Name: "bar"},
				},
				Constraint: &sql.OnConstraint{
					X: &sql.BoolLit{Value: true},
				},
			},
		})
		AssertParseStatement(t, `SELECT * FROM foo LEFT JOIN bar USING (x, y)`, &sql.SelectStatement{
			Columns: []*sql.ResultColumn{
				{Star: pos(7)},
			},
			Source: &sql.JoinClause{
				X: &sql.QualifiedName{
					Name: &sql.Ident{Name: "foo"},
				},
				Operator: &sql.JoinOperator{Left: pos(18)},
				Y: &sql.QualifiedName{
					Name: &sql.Ident{Name: "bar"},
				},
				Constraint: &sql.UsingConstraint{
					Columns: []*sql.Ident{
						{Name: "x"},
						{Name: "y"},
					},
				},
			},
		})
		AssertParseStatement(t, `SELECT * FROM X INNER JOIN Y ON true INNER JOIN Z ON false`, &sql.SelectStatement{
			Columns: []*sql.ResultColumn{
				{Star: pos(7)},
			},
			Source: &sql.JoinClause{
				X: &sql.QualifiedName{
					Name: &sql.Ident{Name: "X"},
				},
				Operator: &sql.JoinOperator{Inner: pos(16)},
				Y: &sql.JoinClause{
					X: &sql.QualifiedName{
						Name: &sql.Ident{Name: "Y"},
					},
					Operator: &sql.JoinOperator{Inner: pos(37)},
					Y: &sql.QualifiedName{
						Name: &sql.Ident{Name: "Z"},
					},
					Constraint: &sql.OnConstraint{
						X: &sql.BoolLit{Value: false},
					},
				},
				Constraint: &sql.OnConstraint{
					X: &sql.BoolLit{Value: true},
				},
			},
		})
		AssertParseStatement(t, `SELECT * FROM X as a JOIN Y as b ON a.id = b.id JOIN Z as c ON b.id = c.id`, &sql.SelectStatement{
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
		})
		AssertParseStatement(t, `SELECT * FROM foo LEFT OUTER JOIN bar`, &sql.SelectStatement{
			Columns: []*sql.ResultColumn{
				{Star: pos(7)},
			},
			Source: &sql.JoinClause{
				X: &sql.QualifiedName{
					Name: &sql.Ident{Name: "foo"},
				},
				Operator: &sql.JoinOperator{Left: pos(18), Outer: pos(23)},
				Y: &sql.QualifiedName{
					Name: &sql.Ident{Name: "bar"},
				},
			},
		})
		AssertParseStatement(t, `SELECT * FROM foo CROSS JOIN bar`, &sql.SelectStatement{
			Columns: []*sql.ResultColumn{
				{Star: pos(7)},
			},
			Source: &sql.JoinClause{
				X: &sql.QualifiedName{
					Name: &sql.Ident{Name: "foo"},
				},
				Operator: &sql.JoinOperator{Cross: pos(18)},
				Y: &sql.QualifiedName{
					Name: &sql.Ident{Name: "bar"},
				},
			},
		})

		AssertParseStatement(t, `WITH cte (foo, bar) AS (SELECT baz), xxx AS (SELECT yyy) SELECT bat`, &sql.SelectStatement{
			WithClause: &sql.WithClause{
				CTEs: []*sql.CTE{
					{
						TableName: &sql.Ident{Name: "cte"},
						Columns: []*sql.Ident{
							{Name: "foo"},
							{Name: "bar"},
						},
						Select: &sql.SelectStatement{
							Columns: []*sql.ResultColumn{
								{Expr: &sql.Ident{Name: "baz"}},
							},
						},
					},
					{
						TableName: &sql.Ident{Name: "xxx"},
						Select: &sql.SelectStatement{
							Columns: []*sql.ResultColumn{
								{Expr: &sql.Ident{Name: "yyy"}},
							},
						},
					},
				},
			},
			Columns: []*sql.ResultColumn{
				{Expr: &sql.Ident{Name: "bat"}},
			},
		})
		AssertParseStatement(t, `WITH RECURSIVE cte AS (SELECT foo) SELECT bar`, &sql.SelectStatement{
			WithClause: &sql.WithClause{
				Recursive: pos(5),
				CTEs: []*sql.CTE{
					{
						TableName: &sql.Ident{Name: "cte"},
						Select: &sql.SelectStatement{
							Columns: []*sql.ResultColumn{
								{Expr: &sql.Ident{Name: "foo"}},
							},
						},
					},
				},
			},
			Columns: []*sql.ResultColumn{
				{Expr: &sql.Ident{Name: "bar"}},
			},
		})

		AssertParseStatement(t, `SELECT * WHERE true`, &sql.SelectStatement{
			Columns:   []*sql.ResultColumn{{Star: pos(7)}},
			WhereExpr: &sql.BoolLit{Value: true},
		})

		AssertParseStatement(t, `SELECT 1 WHERE true AND true`, &sql.SelectStatement{
			Columns: []*sql.ResultColumn{{Expr: &sql.NumberLit{Value: "1"}}},
			WhereExpr: &sql.BinaryExpr{
				X:  &sql.BoolLit{Value: true},
				Op: sql.OP_AND,
				Y:  &sql.BoolLit{Value: true},
			},
		})

		AssertParseStatement(t, `SELECT 1 WHERE true AND (0, 1) = (SELECT 2,3)`, &sql.SelectStatement{
			Columns: []*sql.ResultColumn{{Expr: &sql.NumberLit{Value: "1"}}},
			WhereExpr: &sql.BinaryExpr{
				X:  &sql.BoolLit{Value: true},
				Op: sql.OP_AND,
				Y: &sql.BinaryExpr{
					X: &sql.ExprList{
						Exprs: []sql.Expr{
							&sql.NumberLit{Value: "0"},
							&sql.NumberLit{Value: "1"},
						},
					},
					Op: sql.OP_EQ,
					Y: &sql.ParenExpr{
						Expr: &sql.SelectStatement{
							Columns: []*sql.ResultColumn{
								{Expr: &sql.NumberLit{Value: "2"}},
								{Expr: &sql.NumberLit{Value: "3"}},
							},
						},
					},
				},
			},
		})

		AssertParseStatement(t, `SELECT * GROUP BY foo, bar`, &sql.SelectStatement{
			Columns: []*sql.ResultColumn{{Star: pos(7)}},
			GroupByExprs: []sql.Expr{
				&sql.Ident{Name: "foo"},
				&sql.Ident{Name: "bar"},
			},
		})
		AssertParseStatement(t, `SELECT * GROUP BY foo HAVING true`, &sql.SelectStatement{
			Columns: []*sql.ResultColumn{{Star: pos(7)}},
			GroupByExprs: []sql.Expr{
				&sql.Ident{Name: "foo"},
			},
			HavingExpr: &sql.BoolLit{Value: true},
		})
		AssertParseStatement(t, `SELECT * WINDOW win1 AS (), win2 AS ()`, &sql.SelectStatement{
			Columns: []*sql.ResultColumn{{Star: pos(7)}},
			Windows: []*sql.Window{
				{
					Name:       &sql.Ident{Name: "win1"},
					Definition: &sql.WindowDefinition{},
				},
				{
					Name:       &sql.Ident{Name: "win2"},
					Definition: &sql.WindowDefinition{},
				},
			},
		})

		AssertParseStatement(t, `SELECT * ORDER BY foo ASC, bar DESC`, &sql.SelectStatement{
			Columns: []*sql.ResultColumn{
				{Star: pos(7)},
			},
			OrderingTerms: []*sql.OrderingTerm{
				{X: &sql.Ident{Name: "foo"}, Asc: pos(22)},
				{X: &sql.Ident{Name: "bar"}, Desc: pos(31)},
			},
		})

		AssertParseStatement(t, `SELECT * ORDER BY random()`, &sql.SelectStatement{
			Columns: []*sql.ResultColumn{
				{Star: pos(7)},
			},
			OrderingTerms: []*sql.OrderingTerm{
				{
					X: &sql.Call{
						Name: &sql.QualifiedName{Name: &sql.Ident{Name: "random"}, FunctionCall: true},
					},
				},
			},
		})

		AssertParseStatement(t, `SELECT * ORDER BY c1 COLLATE BINARY;`, &sql.SelectStatement{
			Columns: []*sql.ResultColumn{
				{Star: pos(7)},
			},
			OrderingTerms: []*sql.OrderingTerm{
				{
					X: &sql.BinaryExpr{
						X:  &sql.Ident{Name: "c1"},
						Op: sql.OP_COLLATE,
						Y:  &sql.Ident{Name: "BINARY"},
					},
				},
			},
		})

		AssertParseStatement(t, `SELECT * ORDER BY c1 COLLATE NOCASE DESC;`, &sql.SelectStatement{
			Columns: []*sql.ResultColumn{
				{Star: pos(7)},
			},
			OrderingTerms: []*sql.OrderingTerm{
				{
					X: &sql.BinaryExpr{
						X:  &sql.Ident{Name: "c1"},
						Op: sql.OP_COLLATE,
						Y:  &sql.Ident{Name: "NOCASE"},
					},
					Desc: pos(36),
				},
			},
		})

		AssertParseStatement(t, `SELECT * LIMIT 1`, &sql.SelectStatement{
			Columns: []*sql.ResultColumn{
				{Star: pos(7)},
			},
			LimitExpr: &sql.NumberLit{Value: "1"},
		})
		AssertParseStatement(t, `SELECT * LIMIT 1 OFFSET 2`, &sql.SelectStatement{
			Columns: []*sql.ResultColumn{
				{Star: pos(7)},
			},
			LimitExpr:  &sql.NumberLit{Value: "1"},
			OffsetExpr: &sql.NumberLit{Value: "2"},
		})
		AssertParseStatement(t, `SELECT * LIMIT 1, 2`, &sql.SelectStatement{
			Columns: []*sql.ResultColumn{
				{Star: pos(7)},
			},
			LimitExpr:  &sql.NumberLit{Value: "1"},
			OffsetExpr: &sql.NumberLit{Value: "2"},
		})
		AssertParseStatement(t, `SELECT * UNION SELECT * ORDER BY foo`, &sql.SelectStatement{
			Columns: []*sql.ResultColumn{
				{Star: pos(7)},
			},
			Compound: &sql.SelectStatement{
				Columns: []*sql.ResultColumn{
					{Star: pos(22)},
				},
			},
			OrderingTerms: []*sql.OrderingTerm{
				{X: &sql.Ident{Name: "foo"}},
			},
		})
		AssertParseStatement(t, `SELECT * UNION ALL SELECT *`, &sql.SelectStatement{
			Columns: []*sql.ResultColumn{
				{Star: pos(7)},
			},
			UnionAll: pos(15),
			Compound: &sql.SelectStatement{
				Columns: []*sql.ResultColumn{
					{Star: pos(26)},
				},
			},
		})
		AssertParseStatement(t, `SELECT * INTERSECT SELECT *`, &sql.SelectStatement{
			Columns: []*sql.ResultColumn{
				{Star: pos(7)},
			},
			Intersect: pos(9),
			Compound: &sql.SelectStatement{
				Columns: []*sql.ResultColumn{
					{Star: pos(26)},
				},
			},
		})
		AssertParseStatement(t, `SELECT * EXCEPT SELECT *`, &sql.SelectStatement{
			Columns: []*sql.ResultColumn{
				{Star: pos(7)},
			},
			Except: pos(9),
			Compound: &sql.SelectStatement{
				Columns: []*sql.ResultColumn{
					{Star: pos(23)},
				},
			},
		})

		AssertParseStatement(t, `VALUES (1, 2), (3, 4)`, &sql.SelectStatement{
			ValueLists: []*sql.ExprList{
				{
					Exprs: []sql.Expr{
						&sql.NumberLit{Value: "1"},
						&sql.NumberLit{Value: "2"},
					},
				},
				{
					Exprs: []sql.Expr{
						&sql.NumberLit{Value: "3"},
						&sql.NumberLit{Value: "4"},
					},
				},
			},
		})

		AssertParseStatement(t, `SELECT * FROM foo WHERE foo.elem = 0`, &sql.SelectStatement{
			Columns: []*sql.ResultColumn{
				{
					Star: pos(7),
				},
			},
			Source: &sql.QualifiedName{
				Name: &sql.Ident{
					Name: "foo",
				},
			},
			WhereExpr: &sql.BinaryExpr{
				X: &sql.QualifiedRef{
					Table: &sql.QualifiedName{Name: &sql.Ident{
						Name: "foo",
					}},
					Column: &sql.Ident{
						Name: "elem",
					},
				},
				Op: sql.OP_EQ,
				Y: &sql.NumberLit{
					Value: "0",
				},
			},
		})

		AssertParseStatement(t, `SELECT rowid FROM foo`, &sql.SelectStatement{
			Columns: []*sql.ResultColumn{
				{
					Expr: &sql.Ident{Name: "rowid"},
				},
			},
			Source: &sql.QualifiedName{
				Name: &sql.Ident{
					Name: "foo",
				},
			},
		})

		AssertParseStatement(t, `SELECT rowid FROM foo ORDER BY rowid`, &sql.SelectStatement{
			Columns: []*sql.ResultColumn{
				{
					Expr: &sql.Ident{
						Name: "rowid",
					},
				},
			},
			Source: &sql.QualifiedName{
				Name: &sql.Ident{
					Name: "foo",
				},
			},
			OrderingTerms: []*sql.OrderingTerm{
				{
					X: &sql.Ident{
						Name: "rowid",
					},
				},
			},
		})

		AssertParseStatement(t, `SELECT CURRENT_TIMESTAMP FROM foo`, &sql.SelectStatement{
			Columns: []*sql.ResultColumn{
				{
					Expr: &sql.Ident{
						Name: "CURRENT_TIMESTAMP",
					},
				},
			},
			Source: &sql.QualifiedName{
				Name: &sql.Ident{
					Name: "foo",
				},
			},
		})

		AssertParseStatement(t, `SELECT max(rowid) FROM foo`, &sql.SelectStatement{
			Columns: []*sql.ResultColumn{
				{
					Expr: &sql.Call{
						Name: &sql.QualifiedName{
							Name:         &sql.Ident{Name: "max"},
							FunctionCall: true,
							FunctionArgs: []*sql.FunctionArg{
								{Expr: &sql.Ident{Name: "rowid"}},
							},
						},
					},
				},
			},
			Source: &sql.QualifiedName{
				Name: &sql.Ident{
					Name: "foo",
				},
			},
		})

		AssertParseStatement(t, `SELECT * FROM generate_series(1,3)`, &sql.SelectStatement{
			Columns: []*sql.ResultColumn{
				{
					Star: pos(7),
				},
			},
			Source: &sql.QualifiedName{
				Name: &sql.Ident{
					Name: "generate_series",
				},
				FunctionCall: true,
				FunctionArgs: []*sql.FunctionArg{
					{Expr: &sql.NumberLit{Value: "1"}},
					{Expr: &sql.NumberLit{Value: "3"}},
				},
			},
		})

		AssertParseStatementError(t, `WITH `, `1:6: expected table name, found 'EOF'`)
		AssertParseStatementError(t, `WITH cte`, `1:9: expected AS, found 'EOF'`)
		AssertParseStatementError(t, `WITH cte (`, `1:11: expected column name, found 'EOF'`)
		AssertParseStatementError(t, `WITH cte (foo`, `1:14: expected comma or right paren, found 'EOF'`)
		AssertParseStatementError(t, `WITH cte (foo)`, `1:15: expected AS, found 'EOF'`)
		AssertParseStatementError(t, `WITH cte AS`, `1:12: expected left paren, found 'EOF'`)
		AssertParseStatementError(t, `WITH cte AS (`, `1:14: expected SELECT or VALUES, found 'EOF'`)
		AssertParseStatementError(t, `WITH cte AS (SELECT foo`, `1:24: expected right paren, found 'EOF'`)
		AssertParseStatementError(t, `WITH cte AS (SELECT foo)`, `1:25: expected SELECT, VALUES, INSERT, REPLACE, UPDATE, or DELETE, found 'EOF'`)
		AssertParseStatementError(t, `SELECT `, `1:8: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `SELECT 1+`, `1:10: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `SELECT foo,`, `1:12: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `SELECT foo AS`, `1:14: expected column alias, found 'EOF'`)
		AssertParseStatementError(t, `SELECT foo.* AS`, `1:14: expected semicolon or EOF, found 'AS'`)
		AssertParseStatementError(t, `SELECT foo FROM`, `1:16: expected qualified name, found 'EOF'`)
		AssertParseStatementError(t, `SELECT foo FROM foo INDEXED`, `1:28: expected BY, found 'EOF'`)
		AssertParseStatementError(t, `SELECT foo FROM foo INDEXED BY`, `1:31: expected index name, found 'EOF'`)
		AssertParseStatementError(t, `SELECT foo FROM foo NOT`, `1:24: expected INDEXED, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * FROM foo INNER`, `1:24: expected JOIN, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * FROM foo CROSS`, `1:24: expected JOIN, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * FROM foo NATURAL`, `1:26: expected JOIN, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * FROM foo LEFT`, `1:23: expected JOIN, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * FROM foo LEFT OUTER`, `1:29: expected JOIN, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * FROM foo,`, `1:19: expected qualified name, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * FROM foo JOIN bar ON`, `1:30: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * FROM foo JOIN bar USING`, `1:33: expected left paren, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * FROM foo JOIN bar USING (`, `1:35: expected column name, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * FROM foo JOIN bar USING (x`, `1:36: expected comma or right paren, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * FROM foo JOIN bar USING (x,`, `1:37: expected column name, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * FROM (`, `1:16: expected qualified name, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * FROM ((`, `1:17: expected qualified name, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * FROM (SELECT`, `1:22: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * FROM (tbl`, `1:19: expected right paren, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * FROM (SELECT *) AS`, `1:28: expected table alias, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * FROM foo AS`, `1:21: expected alias name, found 'EOF'`)
		AssertParseStatementError(t, `SELECT foo WHERE`, `1:17: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * GROUP`, `1:15: expected BY, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * GROUP BY`, `1:18: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * GROUP BY foo bar`, `1:23: expected semicolon or EOF, found bar`)
		AssertParseStatementError(t, `SELECT * GROUP BY foo HAVING`, `1:29: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * WINDOW`, `1:16: expected window name, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * WINDOW win1`, `1:21: expected AS, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * WINDOW win1 AS`, `1:24: expected left paren, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * WINDOW win1 AS (`, `1:26: expected right paren, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * WINDOW win1 AS () win2`, `1:28: expected semicolon or EOF, found win2`)
		AssertParseStatementError(t, `SELECT * ORDER`, `1:15: expected BY, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * ORDER BY`, `1:18: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * ORDER BY 1,`, `1:21: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * LIMIT`, `1:15: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * LIMIT 1,`, `1:18: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * LIMIT 1 OFFSET`, `1:24: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `VALUES`, `1:7: expected left paren, found 'EOF'`)
		AssertParseStatementError(t, `VALUES (`, `1:9: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `VALUES (1`, `1:10: expected comma or right paren, found 'EOF'`)
		AssertParseStatementError(t, `VALUES (1,`, `1:11: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * UNION`, `1:15: expected SELECT or VALUES, found 'EOF'`)
	})

	t.Run("Insert", func(t *testing.T) {
		AssertParseStatement(t, `INSERT INTO tbl (x, y) VALUES (1, 2)`, &sql.InsertStatement{
			Table: &sql.QualifiedName{Name: &sql.Ident{Name: "tbl"}},
			Columns: []*sql.Ident{
				{Name: "x"},
				{Name: "y"},
			},
			ValueLists: []*sql.ExprList{{
				Exprs: []sql.Expr{
					&sql.NumberLit{Value: "1"},
					&sql.NumberLit{Value: "2"},
				},
			}},
		})
		AssertParseStatement(t, `INSERT INTO tbl (x, y) VALUES (?, ?)`, &sql.InsertStatement{
			Table: &sql.QualifiedName{Name: &sql.Ident{Name: "tbl"}},
			Columns: []*sql.Ident{
				{Name: "x"},
				{Name: "y"},
			},
			ValueLists: []*sql.ExprList{{
				Exprs: []sql.Expr{
					&sql.BindExpr{Name: "?"},
					&sql.BindExpr{Name: "?"},
				},
			}},
		})
		AssertParseStatement(t, `INSERT INTO tbl (x, y) VALUES (?1, ?2)`, &sql.InsertStatement{
			Table: &sql.QualifiedName{Name: &sql.Ident{Name: "tbl"}},
			Columns: []*sql.Ident{
				{Name: "x"},
				{Name: "y"},
			},
			ValueLists: []*sql.ExprList{{
				Exprs: []sql.Expr{
					&sql.BindExpr{Name: "?1"},
					&sql.BindExpr{Name: "?2"},
				},
			}},
		})
		AssertParseStatement(t, `INSERT INTO tbl (x, y) VALUES (:foo, :bar)`, &sql.InsertStatement{
			Table: &sql.QualifiedName{Name: &sql.Ident{Name: "tbl"}},
			Columns: []*sql.Ident{
				{Name: "x"},
				{Name: "y"},
			},
			ValueLists: []*sql.ExprList{{
				Exprs: []sql.Expr{
					&sql.BindExpr{Name: ":foo"},
					&sql.BindExpr{Name: ":bar"},
				},
			}},
		})
		AssertParseStatement(t, `INSERT INTO tbl (x, y) VALUES (@foo, @bar)`, &sql.InsertStatement{
			Table: &sql.QualifiedName{Name: &sql.Ident{Name: "tbl"}},
			Columns: []*sql.Ident{
				{Name: "x"},
				{Name: "y"},
			},
			ValueLists: []*sql.ExprList{{
				Exprs: []sql.Expr{
					&sql.BindExpr{Name: "@foo"},
					&sql.BindExpr{Name: "@bar"},
				},
			}},
		})
		AssertParseStatement(t, `INSERT INTO tbl (x, y) VALUES ($foo, $bar)`, &sql.InsertStatement{
			Table: &sql.QualifiedName{Name: &sql.Ident{Name: "tbl"}},
			Columns: []*sql.Ident{
				{Name: "x"},
				{Name: "y"},
			},
			ValueLists: []*sql.ExprList{{
				Exprs: []sql.Expr{
					&sql.BindExpr{Name: "$foo"},
					&sql.BindExpr{Name: "$bar"},
				},
			}},
		})
		AssertParseStatement(t, `INSERT INTO tbl (x, y) VALUES (1, random())`, &sql.InsertStatement{
			Table: &sql.QualifiedName{Name: &sql.Ident{Name: "tbl"}},
			Columns: []*sql.Ident{
				{Name: "x"},
				{Name: "y"},
			},
			ValueLists: []*sql.ExprList{{
				Exprs: []sql.Expr{
					&sql.NumberLit{Value: "1"},
					&sql.Call{
						Name: &sql.QualifiedName{
							Name:         &sql.Ident{Name: "random"},
							FunctionCall: true,
						},
					},
				},
			}},
		})
		AssertParseStatement(t, `INSERT INTO tbl (x, y) VALUES (1, abs(random()))`, &sql.InsertStatement{
			Table: &sql.QualifiedName{Name: &sql.Ident{Name: "tbl"}},
			Columns: []*sql.Ident{
				{Name: "x"},
				{Name: "y"},
			},
			ValueLists: []*sql.ExprList{{
				Exprs: []sql.Expr{
					&sql.NumberLit{Value: "1"},
					&sql.Call{
						Name: &sql.QualifiedName{
							Name:         &sql.Ident{Name: "abs"},
							FunctionCall: true,
							FunctionArgs: []*sql.FunctionArg{
								{Expr: &sql.Call{
									Name: &sql.QualifiedName{
										Name:         &sql.Ident{Name: "random"},
										FunctionCall: true,
									},
								}},
							},
						},
					},
				},
			}},
		})
		AssertParseStatement(t, `REPLACE INTO tbl (x, y) VALUES (1, 2), (3, 4)`, &sql.InsertStatement{
			Replace: pos(0),
			Table:   &sql.QualifiedName{Name: &sql.Ident{Name: "tbl"}},
			Columns: []*sql.Ident{
				{Name: "x"},
				{Name: "y"},
			},
			ValueLists: []*sql.ExprList{
				{
					Exprs: []sql.Expr{
						&sql.NumberLit{Value: "1"},
						&sql.NumberLit{Value: "2"},
					},
				},
				{
					Exprs: []sql.Expr{
						&sql.NumberLit{Value: "3"},
						&sql.NumberLit{Value: "4"},
					},
				},
			},
		})

		AssertParseStatement(t, `INSERT OR REPLACE INTO tbl (x) VALUES (1)`, &sql.InsertStatement{
			InsertOrReplace: pos(10),
			Table:           &sql.QualifiedName{Name: &sql.Ident{Name: "tbl"}},
			Columns: []*sql.Ident{
				{Name: "x"},
			},
			ValueLists: []*sql.ExprList{{
				Exprs: []sql.Expr{
					&sql.NumberLit{Value: "1"},
				},
			}},
		})
		AssertParseStatement(t, `INSERT OR ROLLBACK INTO tbl (x) VALUES (1)`, &sql.InsertStatement{
			InsertOrRollback: pos(10),
			Table:            &sql.QualifiedName{Name: &sql.Ident{Name: "tbl"}},
			Columns: []*sql.Ident{
				{Name: "x"},
			},
			ValueLists: []*sql.ExprList{{
				Exprs: []sql.Expr{
					&sql.NumberLit{Value: "1"},
				},
			}},
		})
		AssertParseStatement(t, `INSERT OR ABORT INTO tbl (x) VALUES (1)`, &sql.InsertStatement{
			InsertOrAbort: pos(10),
			Table:         &sql.QualifiedName{Name: &sql.Ident{Name: "tbl"}},
			Columns: []*sql.Ident{
				{Name: "x"},
			},
			ValueLists: []*sql.ExprList{{
				Exprs: []sql.Expr{
					&sql.NumberLit{Value: "1"},
				},
			}},
		})
		AssertParseStatement(t, `INSERT OR FAIL INTO tbl VALUES (1)`, &sql.InsertStatement{
			InsertOrFail: pos(10),
			Table:        &sql.QualifiedName{Name: &sql.Ident{Name: "tbl"}},
			ValueLists: []*sql.ExprList{{
				Exprs: []sql.Expr{
					&sql.NumberLit{Value: "1"},
				},
			}},
		})
		AssertParseStatement(t, `INSERT OR IGNORE INTO tbl AS tbl2 VALUES (1)`, &sql.InsertStatement{
			InsertOrIgnore: pos(10),
			Table: &sql.QualifiedName{
				Name:  &sql.Ident{Name: "tbl"},
				Alias: &sql.Ident{Name: "tbl2"},
			},
			ValueLists: []*sql.ExprList{{
				Exprs: []sql.Expr{
					&sql.NumberLit{Value: "1"},
				},
			}},
		})

		AssertParseStatement(t, `WITH cte (foo) AS (SELECT bar) INSERT INTO tbl VALUES (1)`, &sql.InsertStatement{
			WithClause: &sql.WithClause{
				CTEs: []*sql.CTE{{
					TableName: &sql.Ident{Name: "cte"},
					Columns: []*sql.Ident{
						{Name: "foo"},
					},
					Select: &sql.SelectStatement{
						Columns: []*sql.ResultColumn{
							{Expr: &sql.Ident{Name: "bar"}},
						},
					},
				}},
			},
			Table: &sql.QualifiedName{Name: &sql.Ident{Name: "tbl"}},
			ValueLists: []*sql.ExprList{{
				Exprs: []sql.Expr{
					&sql.NumberLit{Value: "1"},
				},
			}},
		})
		AssertParseStatement(t, `WITH cte (foo) AS (SELECT bar) INSERT INTO tbl VALUES (1)`, &sql.InsertStatement{
			WithClause: &sql.WithClause{
				CTEs: []*sql.CTE{{
					TableName: &sql.Ident{Name: "cte"},
					Columns: []*sql.Ident{
						{Name: "foo"},
					},
					Select: &sql.SelectStatement{
						Columns: []*sql.ResultColumn{
							{Expr: &sql.Ident{Name: "bar"}},
						},
					},
				}},
			},
			Table: &sql.QualifiedName{Name: &sql.Ident{Name: "tbl"}},
			ValueLists: []*sql.ExprList{{
				Exprs: []sql.Expr{
					&sql.NumberLit{Value: "1"},
				},
			}},
		})

		AssertParseStatement(t, `INSERT INTO tbl (x) SELECT y`, &sql.InsertStatement{
			Table: &sql.QualifiedName{Name: &sql.Ident{Name: "tbl"}},
			Columns: []*sql.Ident{
				{Name: "x"},
			},
			Select: &sql.SelectStatement{
				Columns: []*sql.ResultColumn{
					{Expr: &sql.Ident{Name: "y"}},
				},
			},
		})

		AssertParseStatement(t, `INSERT INTO tbl (x) DEFAULT VALUES`, &sql.InsertStatement{
			Table: &sql.QualifiedName{Name: &sql.Ident{Name: "tbl"}},
			Columns: []*sql.Ident{
				{Name: "x"},
			},
			DefaultValues: pos(28),
		})

		AssertParseStatement(t, `INSERT INTO tbl (x) VALUES (1) ON CONFLICT (y ASC, z DESC) DO NOTHING`, &sql.InsertStatement{
			Table: &sql.QualifiedName{Name: &sql.Ident{Name: "tbl"}},
			Columns: []*sql.Ident{
				{Name: "x"},
			},
			ValueLists: []*sql.ExprList{{
				Exprs: []sql.Expr{
					&sql.NumberLit{Value: "1"},
				},
			}},
			UpsertClause: &sql.UpsertClause{
				Columns: []*sql.IndexedColumn{
					{X: &sql.Ident{Name: "y"}, Asc: pos(46)},
					{X: &sql.Ident{Name: "z"}, Desc: pos(53)},
				},
				DoNothing: pos(62),
			},
		})
		AssertParseStatement(t, `INSERT INTO tbl (x) VALUES (1) RETURNING *`, &sql.InsertStatement{
			Table: &sql.QualifiedName{Name: &sql.Ident{Name: "tbl"}},
			Columns: []*sql.Ident{
				{Name: "x"},
			},
			ValueLists: []*sql.ExprList{{
				Exprs: []sql.Expr{
					&sql.NumberLit{Value: "1"},
				},
			}},
			ReturningColumns: []*sql.ResultColumn{{Star: pos(41)}},
		})
		AssertParseStatement(t, `INSERT INTO tbl (x) VALUES (1) RETURNING x`, &sql.InsertStatement{
			Table: &sql.QualifiedName{Name: &sql.Ident{Name: "tbl"}},
			Columns: []*sql.Ident{
				{Name: "x"},
			},
			ValueLists: []*sql.ExprList{{
				Exprs: []sql.Expr{
					&sql.NumberLit{Value: "1"},
				},
			}},
			ReturningColumns: []*sql.ResultColumn{
				{Expr: &sql.Ident{Name: "x"}},
			},
		})
		AssertParseStatement(t, `INSERT INTO tbl (x) VALUES (1) RETURNING x AS y`, &sql.InsertStatement{
			Table: &sql.QualifiedName{Name: &sql.Ident{Name: "tbl"}},
			Columns: []*sql.Ident{
				{Name: "x"},
			},
			ValueLists: []*sql.ExprList{{
				Exprs: []sql.Expr{
					&sql.NumberLit{Value: "1"},
				},
			}},
			ReturningColumns: []*sql.ResultColumn{
				{Expr: &sql.Ident{Name: "x"}, Alias: &sql.Ident{Name: "y"}},
			},
		})
		AssertParseStatement(t, `INSERT INTO tbl (x) VALUES (1) RETURNING x,y`, &sql.InsertStatement{
			Table: &sql.QualifiedName{Name: &sql.Ident{Name: "tbl"}},
			Columns: []*sql.Ident{
				{Name: "x"},
			},
			ValueLists: []*sql.ExprList{{
				Exprs: []sql.Expr{
					&sql.NumberLit{Value: "1"},
				},
			}},
			ReturningColumns: []*sql.ResultColumn{
				{Expr: &sql.Ident{Name: "x"}},
				{Expr: &sql.Ident{Name: "y"}},
			},
		})
		AssertParseStatement(t, `INSERT INTO tbl (x) VALUES (1) RETURNING x,y*2`, &sql.InsertStatement{
			Table: &sql.QualifiedName{Name: &sql.Ident{Name: "tbl"}},
			Columns: []*sql.Ident{
				{Name: "x"},
			},
			ValueLists: []*sql.ExprList{{
				Exprs: []sql.Expr{
					&sql.NumberLit{Value: "1"},
				},
			}},
			ReturningColumns: []*sql.ResultColumn{
				{Expr: &sql.Ident{Name: "x"}},
				{
					Expr: &sql.BinaryExpr{
						X:  &sql.Ident{Name: "y"},
						Op: sql.OP_MULTIPLY,
						Y:  &sql.NumberLit{Value: "2"},
					},
				},
			},
		})
		AssertParseStatement(t, `INSERT INTO tbl (x) VALUES (1) ON CONFLICT (y) WHERE true DO UPDATE SET foo = 1, (bar, baz) = 2 WHERE false`, &sql.InsertStatement{
			Table: &sql.QualifiedName{Name: &sql.Ident{Name: "tbl"}},
			Columns: []*sql.Ident{
				{Name: "x"},
			},
			ValueLists: []*sql.ExprList{{
				Exprs: []sql.Expr{
					&sql.NumberLit{Value: "1"},
				},
			}},
			UpsertClause: &sql.UpsertClause{
				Columns: []*sql.IndexedColumn{
					{X: &sql.Ident{Name: "y"}},
				},
				WhereExpr:   &sql.BoolLit{Value: true},
				DoUpdateSet: pos(68),
				Assignments: []*sql.Assignment{
					{
						Columns: []*sql.Ident{
							{Name: "foo"},
						},
						Expr: &sql.NumberLit{Value: "1"},
					},
					{
						Columns: []*sql.Ident{
							{Name: "bar"},
							{Name: "baz"},
						},
						Expr: &sql.NumberLit{Value: "2"},
					},
				},
				UpdateWhereExpr: &sql.BoolLit{Value: false},
			},
		})

		AssertParseStatementError(t, `INSERT`, `1:7: expected INTO, found 'EOF'`)
		AssertParseStatementError(t, `INSERT OR`, `1:10: expected ROLLBACK, REPLACE, ABORT, FAIL, or IGNORE, found 'EOF'`)
		AssertParseStatementError(t, `INSERT INTO`, `1:12: expected qualified name, found 'EOF'`)
		AssertParseStatementError(t, `INSERT INTO tbl AS`, `1:19: expected alias name, found 'EOF'`)
		AssertParseStatementError(t, `INSERT INTO tbl `, `1:17: expected VALUES, SELECT, or DEFAULT VALUES, found 'EOF'`)
		AssertParseStatementError(t, `INSERT INTO tbl (`, `1:18: expected column name, found 'EOF'`)
		AssertParseStatementError(t, `INSERT INTO tbl (x`, `1:19: expected comma or right paren, found 'EOF'`)
		AssertParseStatementError(t, `INSERT INTO tbl (x)`, `1:20: expected VALUES, SELECT, or DEFAULT VALUES, found 'EOF'`)
		AssertParseStatementError(t, `INSERT INTO tbl (x) VALUES`, `1:27: expected left paren, found 'EOF'`)
		AssertParseStatementError(t, `INSERT INTO tbl (x) VALUES (`, `1:29: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `INSERT INTO tbl (x) VALUES (1`, `1:30: expected comma or right paren, found 'EOF'`)
		AssertParseStatementError(t, `INSERT INTO tbl (x) SELECT`, `1:27: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `INSERT INTO tbl (x) DEFAULT`, `1:28: expected VALUES, found 'EOF'`)
		AssertParseStatementError(t, `INSERT INTO tbl (x) VALUES (1) RETURNING`, `1:41: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `INSERT INTO tbl (x) VALUES (1) ON`, `1:34: expected CONFLICT, found 'EOF'`)
		AssertParseStatementError(t, `INSERT INTO tbl (x) VALUES (1) ON CONFLICT (`, `1:45: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `INSERT INTO tbl (x) VALUES (1) ON CONFLICT (x`, `1:46: expected comma or right paren, found 'EOF'`)
		AssertParseStatementError(t, `INSERT INTO tbl (x) VALUES (1) ON CONFLICT (x) WHERE`, `1:53: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `INSERT INTO tbl (x) VALUES (1) ON CONFLICT (x)`, `1:47: expected DO, found 'EOF'`)
		AssertParseStatementError(t, `INSERT INTO tbl (x) VALUES (1) ON CONFLICT (x) DO`, `1:50: expected NOTHING or UPDATE SET, found 'EOF'`)
		AssertParseStatementError(t, `INSERT INTO tbl (x) VALUES (1) ON CONFLICT (x) DO UPDATE`, `1:57: expected SET, found 'EOF'`)
		AssertParseStatementError(t, `INSERT INTO tbl (x) VALUES (1) ON CONFLICT (x) DO UPDATE SET foo`, `1:65: expected =, found 'EOF'`)
		AssertParseStatementError(t, `INSERT INTO tbl (x) VALUES (1) ON CONFLICT (x) DO UPDATE SET foo =`, `1:67: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `INSERT INTO tbl (x) VALUES (1) ON CONFLICT (x) DO UPDATE SET foo = 1 WHERE`, `1:75: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `INSERT INTO tbl (x) VALUES (1) ON CONFLICT (x) DO UPDATE SET (`, `1:63: expected column name, found 'EOF'`)
		AssertParseStatementError(t, `INSERT INTO tbl (x) VALUES (1) ON CONFLICT (x) DO UPDATE SET (foo`, `1:66: expected comma or right paren, found 'EOF'`)
	})

	t.Run("Update", func(t *testing.T) {
		AssertParseStatement(t, `UPDATE tbl SET x = 1, y = 2`, &sql.UpdateStatement{
			Table: &sql.QualifiedName{
				Name: &sql.Ident{Name: "tbl"},
			},
			Assignments: []*sql.Assignment{
				{
					Columns: []*sql.Ident{{Name: "x"}},
					Expr:    &sql.NumberLit{Value: "1"},
				},
				{
					Columns: []*sql.Ident{{Name: "y"}},
					Expr:    &sql.NumberLit{Value: "2"},
				},
			},
		})

		AssertParseStatement(t, `UPDATE tbl SET x = 1, y = 2 ORDER BY x LIMIT 7 OFFSET 0`, &sql.UpdateStatement{
			Table: &sql.QualifiedName{
				Name: &sql.Ident{Name: "tbl"},
			},
			Assignments: []*sql.Assignment{
				{
					Columns: []*sql.Ident{{Name: "x"}},
					Expr:    &sql.NumberLit{Value: "1"},
				},
				{
					Columns: []*sql.Ident{{Name: "y"}},
					Expr:    &sql.NumberLit{Value: "2"},
				},
			},
			OrderingTerms: []*sql.OrderingTerm{
				{X: &sql.Ident{Name: "x"}},
			},
			LimitExpr:  &sql.NumberLit{Value: "7"},
			OffsetExpr: &sql.NumberLit{Value: "0"},
		})
		AssertParseStatement(t, `UPDATE tbl SET x = 1 WHERE y = 2`, &sql.UpdateStatement{
			Table: &sql.QualifiedName{
				Name: &sql.Ident{Name: "tbl"},
			},
			Assignments: []*sql.Assignment{{
				Columns: []*sql.Ident{{Name: "x"}},
				Expr:    &sql.NumberLit{Value: "1"},
			}},
			WhereExpr: &sql.BinaryExpr{
				X:  &sql.Ident{Name: "y"},
				Op: sql.OP_EQ,
				Y:  &sql.NumberLit{Value: "2"},
			},
		})
		AssertParseStatement(t, `UPDATE OR ROLLBACK tbl SET x = 1`, &sql.UpdateStatement{
			UpdateOrRollback: pos(10),
			Table: &sql.QualifiedName{
				Name: &sql.Ident{Name: "tbl"},
			},
			Assignments: []*sql.Assignment{{
				Columns: []*sql.Ident{{Name: "x"}},
				Expr:    &sql.NumberLit{Value: "1"},
			}},
		})
		AssertParseStatement(t, `UPDATE OR ABORT tbl SET x = 1`, &sql.UpdateStatement{
			UpdateOrAbort: pos(10),
			Table: &sql.QualifiedName{
				Name: &sql.Ident{Name: "tbl"},
			},
			Assignments: []*sql.Assignment{{
				Columns: []*sql.Ident{{Name: "x"}},
				Expr:    &sql.NumberLit{Value: "1"},
			}},
		})
		AssertParseStatement(t, `UPDATE OR REPLACE tbl SET x = 1`, &sql.UpdateStatement{
			UpdateOrReplace: pos(10),
			Table: &sql.QualifiedName{
				Name: &sql.Ident{Name: "tbl"},
			},
			Assignments: []*sql.Assignment{{
				Columns: []*sql.Ident{{Name: "x"}},
				Expr:    &sql.NumberLit{Value: "1"},
			}},
		})
		AssertParseStatement(t, `UPDATE OR FAIL tbl SET x = 1`, &sql.UpdateStatement{
			UpdateOrFail: pos(10),
			Table: &sql.QualifiedName{
				Name: &sql.Ident{Name: "tbl"},
			},
			Assignments: []*sql.Assignment{{
				Columns: []*sql.Ident{{Name: "x"}},
				Expr:    &sql.NumberLit{Value: "1"},
			}},
		})
		AssertParseStatement(t, `UPDATE OR IGNORE tbl SET x = 1`, &sql.UpdateStatement{
			UpdateOrIgnore: pos(10),
			Table: &sql.QualifiedName{
				Name: &sql.Ident{Name: "tbl"},
			},
			Assignments: []*sql.Assignment{{
				Columns: []*sql.Ident{{Name: "x"}},
				Expr:    &sql.NumberLit{Value: "1"},
			}},
		})
		AssertParseStatement(t, `WITH cte (x) AS (SELECT y) UPDATE tbl SET x = 1`, &sql.UpdateStatement{
			WithClause: &sql.WithClause{
				CTEs: []*sql.CTE{
					{
						TableName: &sql.Ident{Name: "cte"},
						Columns: []*sql.Ident{
							{Name: "x"},
						},
						Select: &sql.SelectStatement{
							Columns: []*sql.ResultColumn{
								{Expr: &sql.Ident{Name: "y"}},
							},
						},
					},
				},
			},
			Table: &sql.QualifiedName{
				Name: &sql.Ident{Name: "tbl"},
			},
			Assignments: []*sql.Assignment{{
				Columns: []*sql.Ident{{Name: "x"}},
				Expr:    &sql.NumberLit{Value: "1"},
			}},
		})

		AssertParseStatementError(t, `UPDATE`, `1:7: expected qualified name, found 'EOF'`)
		AssertParseStatementError(t, `UPDATE OR`, `1:10: expected ROLLBACK, REPLACE, ABORT, FAIL, or IGNORE, found 'EOF'`)
		AssertParseStatementError(t, `UPDATE tbl`, `1:11: expected SET, found 'EOF'`)
		AssertParseStatementError(t, `UPDATE tbl SET`, `1:15: expected column name or column list, found 'EOF'`)
		AssertParseStatementError(t, `UPDATE tbl SET x = `, `1:20: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `UPDATE tbl SET x = 1 WHERE`, `1:27: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `UPDATE tbl SET x = 1 WHERE y =`, `1:31: expected expression, found 'EOF'`)
	})

	t.Run("Delete", func(t *testing.T) {
		AssertParseStatement(t, `DELETE FROM tbl`, &sql.DeleteStatement{
			Table: &sql.QualifiedName{
				Name: &sql.Ident{Name: "tbl"},
			},
		})
		AssertParseStatement(t, `DELETE FROM tbl WHERE x = 1`, &sql.DeleteStatement{
			Table: &sql.QualifiedName{
				Name: &sql.Ident{Name: "tbl"},
			},
			WhereExpr: &sql.BinaryExpr{
				X:  &sql.Ident{Name: "x"},
				Op: sql.OP_EQ,
				Y:  &sql.NumberLit{Value: "1"},
			},
		})
		AssertParseStatement(t, `DELETE FROM tbl WHERE x = 1 RETURNING x`, &sql.DeleteStatement{
			Table: &sql.QualifiedName{
				Name: &sql.Ident{Name: "tbl"},
			},
			WhereExpr: &sql.BinaryExpr{
				X:  &sql.Ident{Name: "x"},
				Op: sql.OP_EQ,
				Y:  &sql.NumberLit{Value: "1"},
			},
			ReturningColumns: []*sql.ResultColumn{{Expr: &sql.Ident{Name: "x"}}},
		})
		AssertParseStatement(t, `WITH cte (x) AS (SELECT y) DELETE FROM tbl`, &sql.DeleteStatement{
			WithClause: &sql.WithClause{
				CTEs: []*sql.CTE{
					{
						TableName: &sql.Ident{Name: "cte"},
						Columns: []*sql.Ident{
							{Name: "x"},
						},
						Select: &sql.SelectStatement{
							Columns: []*sql.ResultColumn{
								{Expr: &sql.Ident{Name: "y"}},
							},
						},
					},
				},
			},
			Table: &sql.QualifiedName{
				Name: &sql.Ident{Name: "tbl"},
			},
		})
		AssertParseStatement(t, `DELETE FROM tbl ORDER BY x, y LIMIT 1 OFFSET 2`, &sql.DeleteStatement{
			Table: &sql.QualifiedName{
				Name: &sql.Ident{Name: "tbl"},
			},
			OrderingTerms: []*sql.OrderingTerm{
				{X: &sql.Ident{Name: "x"}},
				{X: &sql.Ident{Name: "y"}},
			},
			LimitExpr:  &sql.NumberLit{Value: "1"},
			OffsetExpr: &sql.NumberLit{Value: "2"},
		})
		AssertParseStatement(t, `DELETE FROM tbl LIMIT 1`, &sql.DeleteStatement{
			Table: &sql.QualifiedName{
				Name: &sql.Ident{Name: "tbl"},
			},
			LimitExpr: &sql.NumberLit{Value: "1"},
		})
		AssertParseStatement(t, `DELETE FROM tbl LIMIT 1, 2`, &sql.DeleteStatement{
			Table: &sql.QualifiedName{
				Name: &sql.Ident{Name: "tbl"},
			},
			LimitExpr:  &sql.NumberLit{Value: "1"},
			OffsetExpr: &sql.NumberLit{Value: "2"},
		})

		AssertParseStatement(t, `DELETE FROM tbl1 WHERE id IN (SELECT tbl1_id FROM tbl2 WHERE foo = 'bar')`, &sql.DeleteStatement{
			Table: &sql.QualifiedName{
				Name: &sql.Ident{Name: "tbl1"},
			},
			WhereExpr: &sql.InExpr{
				X:  &sql.Ident{Name: "id"},
				Op: sql.OP_IN,
				Select: &sql.SelectStatement{
					Columns: []*sql.ResultColumn{
						{
							Expr: &sql.Ident{Name: "tbl1_id"},
						},
					},
					Source: &sql.QualifiedName{
						Name: &sql.Ident{Name: "tbl2"},
					},
					WhereExpr: &sql.BinaryExpr{
						X:  &sql.Ident{Name: "foo"},
						Op: sql.OP_EQ,
						Y:  &sql.StringLit{Value: "bar"},
					},
				},
			},
		})

		AssertParseStatementError(t, `DELETE`, `1:7: expected FROM, found 'EOF'`)
		AssertParseStatementError(t, `DELETE FROM`, `1:12: expected qualified name, found 'EOF'`)
		AssertParseStatementError(t, `DELETE FROM tbl WHERE`, `1:22: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `DELETE FROM tbl ORDER `, `1:23: expected BY, found 'EOF'`)
		AssertParseStatementError(t, `DELETE FROM tbl ORDER BY`, `1:25: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `DELETE FROM tbl ORDER BY x`, `1:27: expected LIMIT, found 'EOF'`)
		AssertParseStatementError(t, `DELETE FROM tbl LIMIT`, `1:22: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `DELETE FROM tbl LIMIT 1,`, `1:25: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `DELETE FROM tbl LIMIT 1 OFFSET`, `1:31: expected expression, found 'EOF'`)
	})

	t.Run("AlterTable", func(t *testing.T) {
		AssertParseStatement(t, `ALTER TABLE tbl RENAME TO new_tbl`, &sql.AlterTableStatement{
			Name:    &sql.QualifiedName{Name: &sql.Ident{Name: "tbl"}},
			NewName: &sql.Ident{Name: "new_tbl"},
		})
		AssertParseStatement(t, `ALTER TABLE tbl RENAME COLUMN col TO new_col`, &sql.AlterTableStatement{
			Name:          &sql.QualifiedName{Name: &sql.Ident{Name: "tbl"}},
			ColumnName:    &sql.Ident{Name: "col"},
			NewColumnName: &sql.Ident{Name: "new_col"},
		})
		AssertParseStatement(t, `ALTER TABLE tbl RENAME col TO new_col`, &sql.AlterTableStatement{
			Name:          &sql.QualifiedName{Name: &sql.Ident{Name: "tbl"}},
			ColumnName:    &sql.Ident{Name: "col"},
			NewColumnName: &sql.Ident{Name: "new_col"},
		})
		AssertParseStatement(t, `ALTER TABLE tbl ADD COLUMN col TEXT PRIMARY KEY`, &sql.AlterTableStatement{
			Name: &sql.QualifiedName{Name: &sql.Ident{Name: "tbl"}},
			ColumnDef: &sql.ColumnDefinition{
				Name: &sql.Ident{Name: "col"},
				Type: &sql.Type{
					Name: &sql.Ident{Name: "TEXT"},
				},
				Constraints: []sql.Constraint{
					&sql.PrimaryKeyConstraint{},
				},
			},
		})
		AssertParseStatement(t, `ALTER TABLE tbl ADD col TEXT`, &sql.AlterTableStatement{
			Name: &sql.QualifiedName{Name: &sql.Ident{Name: "tbl"}},
			ColumnDef: &sql.ColumnDefinition{
				Name: &sql.Ident{Name: "col"},
				Type: &sql.Type{
					Name: &sql.Ident{Name: "TEXT"},
				},
			},
		})

		AssertParseStatementError(t, `ALTER`, `1:6: expected TABLE, found 'EOF'`)
		AssertParseStatementError(t, `ALTER TABLE`, `1:12: expected qualified name, found 'EOF'`)
		AssertParseStatementError(t, `ALTER TABLE tbl`, `1:16: expected ADD or RENAME, found 'EOF'`)
		AssertParseStatementError(t, `ALTER TABLE tbl RENAME`, `1:23: expected COLUMN keyword or column name, found 'EOF'`)
		AssertParseStatementError(t, `ALTER TABLE tbl RENAME TO`, `1:26: expected new table name, found 'EOF'`)
		AssertParseStatementError(t, `ALTER TABLE tbl RENAME COLUMN`, `1:30: expected column name, found 'EOF'`)
		AssertParseStatementError(t, `ALTER TABLE tbl RENAME COLUMN col`, `1:34: expected TO, found 'EOF'`)
		AssertParseStatementError(t, `ALTER TABLE tbl RENAME COLUMN col TO`, `1:37: expected new column name, found 'EOF'`)
		AssertParseStatementError(t, `ALTER TABLE tbl ADD`, `1:20: expected COLUMN keyword or column name, found 'EOF'`)
		AssertParseStatementError(t, `ALTER TABLE tbl ADD COLUMN`, `1:27: expected column name, found 'EOF'`)
	})

	t.Run("Analyze", func(t *testing.T) {
		AssertParseStatement(t, `ANALYZE`, &sql.AnalyzeStatement{})
		AssertParseStatement(t, `ANALYZE tbl`, &sql.AnalyzeStatement{
			Name: &sql.QualifiedName{Name: &sql.Ident{Name: "tbl"}},
		})
	})
	t.Run("Reindex", func(t *testing.T) {
		AssertParseStatement(t, `REINDEX`, &sql.ReindexStatement{})
		AssertParseStatement(t, `REINDEX tbl`, &sql.ReindexStatement{
			Name: &sql.QualifiedName{Name: &sql.Ident{Name: "tbl"}},
		})
		AssertParseStatement(t, `REINDEX schema.tbl`, &sql.ReindexStatement{
			Name: &sql.QualifiedName{
				Schema: &sql.Ident{Name: "schema"},
				Name:   &sql.Ident{Name: "tbl"},
			},
		})
	})
}

func TestParser_ParseExpr(t *testing.T) {
	t.Run("Ident", func(t *testing.T) {
		AssertParseExpr(t, `fooBAR_123'`, &sql.Ident{Name: `fooBAR_123`})
	})
	t.Run("StringLit", func(t *testing.T) {
		AssertParseExpr(t, `'foo bar'`, &sql.StringLit{Value: `foo bar`})
	})
	t.Run("BlobLit", func(t *testing.T) {
		AssertParseExpr(t, `x'0123'`, &sql.BlobLit{Value: `0123`})
	})
	t.Run("Integer", func(t *testing.T) {
		AssertParseExpr(t, `123`, &sql.NumberLit{Value: `123`})
	})
	t.Run("Float", func(t *testing.T) {
		AssertParseExpr(t, `123.456`, &sql.NumberLit{Value: `123.456`})
	})
	t.Run("Null", func(t *testing.T) {
		AssertParseExpr(t, `NULL`, &sql.NullLit{})
	})
	t.Run("Bool", func(t *testing.T) {
		AssertParseExpr(t, `true`, &sql.BoolLit{Value: true})
		AssertParseExpr(t, `false`, &sql.BoolLit{Value: false})
	})
	t.Run("Bind", func(t *testing.T) {
		AssertParseExpr(t, `$bar`, &sql.BindExpr{Name: "$bar"})
	})
	t.Run("UnaryExpr", func(t *testing.T) {
		AssertParseExpr(t, `-123`, &sql.UnaryExpr{Op: sql.OP_MINUS, X: &sql.NumberLit{Value: `123`}})
		AssertParseExpr(t, `NOT foo`, &sql.UnaryExpr{Op: sql.OP_NOT, X: &sql.Ident{Name: "foo"}})
		AssertParseExpr(t, `~1`, &sql.UnaryExpr{Op: sql.OP_BITNOT, X: &sql.NumberLit{Value: "1"}})
		AssertParseExprError(t, `-`, `1:2: expected expression, found 'EOF'`)
	})
	t.Run("QualifiedRef", func(t *testing.T) {
		AssertParseExpr(t, `tbl.col`, &sql.QualifiedRef{
			Table:  &sql.QualifiedName{Name: &sql.Ident{Name: "tbl"}},
			Column: &sql.Ident{Name: "col"},
		})
		AssertParseExpr(t, `"tbl"."col"`, &sql.QualifiedRef{
			Table:  &sql.QualifiedName{Name: &sql.Ident{Name: "tbl", Quoted: true}},
			Column: &sql.Ident{Name: "col", Quoted: true},
		})
		AssertParseExprError(t, `tbl.`, `1:5: expected column name, found 'EOF'`)
	})
	t.Run("Exists", func(t *testing.T) {
		AssertParseExpr(t, `EXISTS (SELECT *)`, &sql.Exists{
			Select: &sql.SelectStatement{
				Columns: []*sql.ResultColumn{
					{Star: pos(15)},
				},
			},
		})
		AssertParseExpr(t, `NOT EXISTS (SELECT *)`, &sql.Exists{
			Not: pos(0),
			Select: &sql.SelectStatement{
				Columns: []*sql.ResultColumn{
					{Star: pos(19)},
				},
			},
		})
		AssertParseExprError(t, `NOT`, `1:4: expected expression, found 'EOF'`)
		AssertParseExprError(t, `EXISTS`, `1:7: expected left paren, found 'EOF'`)
		AssertParseExprError(t, `EXISTS (`, `1:9: expected SELECT or VALUES, found 'EOF'`)
		AssertParseExprError(t, `EXISTS (SELECT`, `1:15: expected expression, found 'EOF'`)
		AssertParseExprError(t, `EXISTS (SELECT *`, `1:17: expected right paren, found 'EOF'`)
	})
	t.Run("BinaryExpr", func(t *testing.T) {
		AssertParseExpr(t, `1 + 2'`, &sql.BinaryExpr{
			X:  &sql.NumberLit{Value: "1"},
			Op: sql.OP_PLUS,
			Y:  &sql.NumberLit{Value: "2"},
		})
		AssertParseExpr(t, `1 - 2'`, &sql.BinaryExpr{
			X:  &sql.NumberLit{Value: "1"},
			Op: sql.OP_MINUS,
			Y:  &sql.NumberLit{Value: "2"},
		})
		AssertParseExpr(t, `1 * 2'`, &sql.BinaryExpr{
			X:  &sql.NumberLit{Value: "1"},
			Op: sql.OP_MULTIPLY,
			Y:  &sql.NumberLit{Value: "2"},
		})
		AssertParseExpr(t, `1 / 2'`, &sql.BinaryExpr{
			X:  &sql.NumberLit{Value: "1"},
			Op: sql.OP_DIVIDE,
			Y:  &sql.NumberLit{Value: "2"},
		})
		AssertParseExpr(t, `1 % 2'`, &sql.BinaryExpr{
			X:  &sql.NumberLit{Value: "1"},
			Op: sql.OP_MODULO,
			Y:  &sql.NumberLit{Value: "2"},
		})
		AssertParseExpr(t, `1 || 2'`, &sql.BinaryExpr{
			X:  &sql.NumberLit{Value: "1"},
			Op: sql.OP_CONCAT,
			Y:  &sql.NumberLit{Value: "2"},
		})
		AssertParseExpr(t, `1 << 2'`, &sql.BinaryExpr{
			X:  &sql.NumberLit{Value: "1"},
			Op: sql.OP_LSHIFT,
			Y:  &sql.NumberLit{Value: "2"},
		})
		AssertParseExpr(t, `1 >> 2'`, &sql.BinaryExpr{
			X:  &sql.NumberLit{Value: "1"},
			Op: sql.OP_RSHIFT,
			Y:  &sql.NumberLit{Value: "2"},
		})
		AssertParseExpr(t, `1 & 2'`, &sql.BinaryExpr{
			X:  &sql.NumberLit{Value: "1"},
			Op: sql.OP_BITAND,
			Y:  &sql.NumberLit{Value: "2"},
		})
		AssertParseExpr(t, `1 | 2'`, &sql.BinaryExpr{
			X:  &sql.NumberLit{Value: "1"},
			Op: sql.OP_BITOR,
			Y:  &sql.NumberLit{Value: "2"},
		})
		AssertParseExpr(t, `1 < 2'`, &sql.BinaryExpr{
			X:  &sql.NumberLit{Value: "1"},
			Op: sql.OP_LT,
			Y:  &sql.NumberLit{Value: "2"},
		})
		AssertParseExpr(t, `1 <= 2'`, &sql.BinaryExpr{
			X:  &sql.NumberLit{Value: "1"},
			Op: sql.OP_LE,
			Y:  &sql.NumberLit{Value: "2"},
		})
		AssertParseExpr(t, `1 > 2'`, &sql.BinaryExpr{
			X:  &sql.NumberLit{Value: "1"},
			Op: sql.OP_GT,
			Y:  &sql.NumberLit{Value: "2"},
		})
		AssertParseExpr(t, `1 >= 2'`, &sql.BinaryExpr{
			X:  &sql.NumberLit{Value: "1"},
			Op: sql.OP_GE,
			Y:  &sql.NumberLit{Value: "2"},
		})
		AssertParseExpr(t, `1 = 2'`, &sql.BinaryExpr{
			X:  &sql.NumberLit{Value: "1"},
			Op: sql.OP_EQ,
			Y:  &sql.NumberLit{Value: "2"},
		})
		AssertParseExpr(t, `1 != 2'`, &sql.BinaryExpr{
			X:  &sql.NumberLit{Value: "1"},
			Op: sql.OP_NE,
			Y:  &sql.NumberLit{Value: "2"},
		})
		AssertParseExpr(t, `(1 + 2)'`, &sql.ParenExpr{Expr: &sql.BinaryExpr{
			X:  &sql.NumberLit{Value: "1"},
			Op: sql.OP_PLUS,
			Y:  &sql.NumberLit{Value: "2"},
		}})
		AssertParseExprError(t, `(`, `1:2: expected expression, found 'EOF'`)
		AssertParseExpr(t, `1 IS 2'`, &sql.BinaryExpr{
			X:  &sql.NumberLit{Value: "1"},
			Op: sql.OP_IS,
			Y:  &sql.NumberLit{Value: "2"},
		})
		AssertParseExpr(t, `1 IS NOT 2'`, &sql.BinaryExpr{
			X:  &sql.NumberLit{Value: "1"},
			Op: sql.OP_IS_NOT,
			Y:  &sql.NumberLit{Value: "2"},
		})
		AssertParseExpr(t, `1 LIKE 2'`, &sql.BinaryExpr{
			X:  &sql.NumberLit{Value: "1"},
			Op: sql.OP_LIKE,
			Y:  &sql.NumberLit{Value: "2"},
		})
		AssertParseExpr(t, `1 NOT LIKE 2'`, &sql.BinaryExpr{
			X:  &sql.NumberLit{Value: "1"},
			Op: sql.OP_NOT_LIKE,
			Y:  &sql.NumberLit{Value: "2"},
		})
		AssertParseExpr(t, `1 GLOB 2'`, &sql.BinaryExpr{
			X:  &sql.NumberLit{Value: "1"},
			Op: sql.OP_GLOB,
			Y:  &sql.NumberLit{Value: "2"},
		})
		AssertParseExpr(t, `1 NOT GLOB 2'`, &sql.BinaryExpr{
			X:  &sql.NumberLit{Value: "1"},
			Op: sql.OP_NOT_GLOB,
			Y:  &sql.NumberLit{Value: "2"},
		})
		AssertParseExpr(t, `1 REGEXP 2'`, &sql.BinaryExpr{
			X:  &sql.NumberLit{Value: "1"},
			Op: sql.OP_REGEXP,
			Y:  &sql.NumberLit{Value: "2"},
		})
		AssertParseExpr(t, `1 NOT REGEXP 2'`, &sql.BinaryExpr{
			X:  &sql.NumberLit{Value: "1"},
			Op: sql.OP_NOT_REGEXP,
			Y:  &sql.NumberLit{Value: "2"},
		})
		AssertParseExpr(t, `1 MATCH 2'`, &sql.BinaryExpr{
			X:  &sql.NumberLit{Value: "1"},
			Op: sql.OP_MATCH,
			Y:  &sql.NumberLit{Value: "2"},
		})
		AssertParseExpr(t, `1 NOT MATCH 2'`, &sql.BinaryExpr{
			X:  &sql.NumberLit{Value: "1"},
			Op: sql.OP_NOT_MATCH,
			Y:  &sql.NumberLit{Value: "2"},
		})
		AssertParseExprError(t, `1 NOT TABLE`, `1:7: expected IN, LIKE, GLOB, REGEXP, MATCH, BETWEEN, IS/NOT NULL, found 'TABLE'`)
		AssertParseExpr(t, `1 IN (2, 3)`, &sql.InExpr{
			X:  &sql.NumberLit{Value: "1"},
			Op: sql.OP_IN,
			Values: &sql.ExprList{
				Exprs: []sql.Expr{
					&sql.NumberLit{Value: "2"},
					&sql.NumberLit{Value: "3"},
				},
			},
		})
		AssertParseExpr(t, `1 IN (SELECT 1)`, &sql.InExpr{
			X:  &sql.NumberLit{Value: "1"},
			Op: sql.OP_IN,
			Select: &sql.SelectStatement{
				Columns: []*sql.ResultColumn{
					{Expr: &sql.NumberLit{Value: "1"}},
				},
			},
		})
		AssertParseExpr(t, `1 IN main.tbl`, &sql.InExpr{
			X:  &sql.NumberLit{Value: "1"},
			Op: sql.OP_IN,
			TableOrFunction: &sql.QualifiedName{
				Schema: &sql.Ident{Name: "main"},
				Name:   &sql.Ident{Name: "tbl"},
			},
		})
		AssertParseExpr(t, `1 IN main.tbl()`, &sql.InExpr{
			X:  &sql.NumberLit{Value: "1"},
			Op: sql.OP_IN,
			TableOrFunction: &sql.QualifiedName{
				Schema:       &sql.Ident{Name: "main"},
				Name:         &sql.Ident{Name: "tbl"},
				FunctionCall: true,
			},
		})
		AssertParseExpr(t, `1 NOT IN (2, 3)'`, &sql.InExpr{
			X:  &sql.NumberLit{Value: "1"},
			Op: sql.OP_NOT_IN,
			Values: &sql.ExprList{
				Exprs: []sql.Expr{
					&sql.NumberLit{Value: "2"},
					&sql.NumberLit{Value: "3"},
				},
			},
		})
		AssertParseExprError(t, `1 IN 2`, `1:6: expected qualified name, found 2`)
		AssertParseExprError(t, `1 IN (`, `1:7: expected expression, found 'EOF'`)
		AssertParseExprError(t, `1 IN (2 3`, `1:9: expected comma or right paren, found 3`)
		AssertParseExpr(t, `1 BETWEEN 2 AND 3'`, &sql.BinaryExpr{
			X:  &sql.NumberLit{Value: "1"},
			Op: sql.OP_BETWEEN,
			Y: &sql.BinaryExpr{
				X:  &sql.NumberLit{Value: "2"},
				Op: sql.OP_AND,
				Y:  &sql.NumberLit{Value: "3"},
			},
		})
		AssertParseExpr(t, `1 NOT BETWEEN 2 AND 3'`, &sql.BinaryExpr{
			X:  &sql.NumberLit{Value: "1"},
			Op: sql.OP_NOT_BETWEEN,
			Y: &sql.BinaryExpr{
				X:  &sql.NumberLit{Value: "2"},
				Op: sql.OP_AND,
				Y:  &sql.NumberLit{Value: "3"},
			},
		})
		AssertParseExpr(t, `1 -> 2`, &sql.BinaryExpr{
			X:  &sql.NumberLit{Value: "1"},
			Op: sql.OP_JSON_EXTRACT_JSON,
			Y:  &sql.NumberLit{Value: "2"},
		})
		AssertParseExpr(t, `1 ->> 2`, &sql.BinaryExpr{
			X:  &sql.NumberLit{Value: "1"},
			Op: sql.OP_JSON_EXTRACT_SQL,
			Y:  &sql.NumberLit{Value: "2"},
		})
		AssertParseExprError(t, `1 BETWEEN`, `1:10: expected expression, found 'EOF'`)
		AssertParseExprError(t, `1 BETWEEN 2`, `1:12: expected AND, found 'EOF'`)
		AssertParseExprError(t, `1 BETWEEN 2 + 3`, `1:16: expected AND, found 'EOF'`)
		AssertParseExprError(t, `1 + `, `1:5: expected expression, found 'EOF'`)
	})
	t.Run("Call", func(t *testing.T) {
		AssertParseExpr(t, `sum()`, &sql.Call{
			Name: &sql.QualifiedName{Name: &sql.Ident{Name: "sum"}, FunctionCall: true},
		})
		AssertParseExpr(t, `sum(*)`, &sql.Call{
			Name: &sql.QualifiedName{
				Name:         &sql.Ident{Name: "sum"},
				FunctionCall: true,
				FunctionStar: true,
			},
		})
		AssertParseExpr(t, `sum(foo, 123)`, &sql.Call{
			Name: &sql.QualifiedName{
				Name:         &sql.Ident{Name: "sum"},
				FunctionCall: true,
				FunctionArgs: []*sql.FunctionArg{
					{Expr: &sql.Ident{Name: "foo"}},
					{Expr: &sql.NumberLit{Value: "123"}},
				},
			},
		})
		AssertParseExpr(t, `sum(distinct 'foo')`, &sql.Call{
			Name: &sql.QualifiedName{
				Name:             &sql.Ident{Name: "sum"},
				FunctionCall:     true,
				FunctionDistinct: true,
				FunctionArgs: []*sql.FunctionArg{
					{Expr: &sql.StringLit{Value: "foo"}},
				},
			},
		})
		AssertParseExpr(t, `sum(1, sum(2, 3))`, &sql.Call{
			Name: &sql.QualifiedName{
				Name:         &sql.Ident{Name: "sum"},
				FunctionCall: true,
				FunctionArgs: []*sql.FunctionArg{
					{Expr: &sql.NumberLit{Value: "1"}},
					{Expr: &sql.Call{
						Name: &sql.QualifiedName{
							Name:         &sql.Ident{Name: "sum"},
							FunctionCall: true,
							FunctionArgs: []*sql.FunctionArg{
								{Expr: &sql.NumberLit{Value: "2"}},
								{Expr: &sql.NumberLit{Value: "3"}},
							},
						},
					}},
				},
			},
		})
		AssertParseExpr(t, `sum(sum(1,2), sum(3, 4))`, &sql.Call{
			Name: &sql.QualifiedName{
				Name:         &sql.Ident{Name: "sum"},
				FunctionCall: true,
				FunctionArgs: []*sql.FunctionArg{
					{Expr: &sql.Call{
						Name: &sql.QualifiedName{
							Name:         &sql.Ident{Name: "sum"},
							FunctionCall: true,
							FunctionArgs: []*sql.FunctionArg{
								{Expr: &sql.NumberLit{Value: "1"}},
								{Expr: &sql.NumberLit{Value: "2"}},
							},
						},
					}},
					{Expr: &sql.Call{
						Name: &sql.QualifiedName{
							Name:         &sql.Ident{Name: "sum"},
							FunctionCall: true,
							FunctionArgs: []*sql.FunctionArg{
								{Expr: &sql.NumberLit{Value: "3"}},
								{Expr: &sql.NumberLit{Value: "4"}},
							},
						},
					}},
				},
			},
		})
		AssertParseExpr(t, `sum() filter (where true)`, &sql.Call{
			Name: &sql.QualifiedName{
				Name:         &sql.Ident{Name: "sum"},
				FunctionCall: true,
			},
			Filter: &sql.BoolLit{Value: true},
		})

		AssertParseExpr(t, `sum() over win1`, &sql.Call{
			Name: &sql.QualifiedName{
				Name:         &sql.Ident{Name: "sum"},
				FunctionCall: true,
			},
			OverName: &sql.Ident{Name: "win1"},
		})
		AssertParseExpr(t, `sum() over (win1 partition by foo, bar order by baz ASC NULLS FIRST, biz)`, &sql.Call{
			Name: &sql.QualifiedName{
				Name:         &sql.Ident{Name: "sum"},
				FunctionCall: true,
			},
			OverWindow: &sql.WindowDefinition{
				Base: &sql.Ident{Name: "win1"},
				Partitions: []sql.Expr{
					&sql.Ident{Name: "foo"},
					&sql.Ident{Name: "bar"},
				},
				OrderingTerms: []*sql.OrderingTerm{
					{
						X:          &sql.Ident{Name: "baz"},
						Asc:        pos(52),
						NullsFirst: pos(62),
					},
					{
						X: &sql.Ident{Name: "biz"},
					},
				},
			},
		})
		AssertParseExpr(t, `sum() over (order by baz DESC NULLS LAST)`, &sql.Call{
			Name: &sql.QualifiedName{
				Name:         &sql.Ident{Name: "sum"},
				FunctionCall: true,
			},
			OverWindow: &sql.WindowDefinition{
				OrderingTerms: []*sql.OrderingTerm{
					{
						X:         &sql.Ident{Name: "baz"},
						Desc:      pos(25),
						NullsLast: pos(36),
					},
				},
			},
		})
		AssertParseExpr(t, `sum() over (range foo preceding)`, &sql.Call{
			Name: &sql.QualifiedName{
				Name:         &sql.Ident{Name: "sum"},
				FunctionCall: true,
			},
			OverWindow: &sql.WindowDefinition{
				Frame: &sql.FrameSpec{
					Range:      pos(12),
					X:          &sql.Ident{Name: "foo"},
					PrecedingX: pos(22),
				},
			},
		})
		AssertParseExpr(t, `sum() over (rows between foo following and bar preceding)`, &sql.Call{
			Name: &sql.QualifiedName{
				Name:         &sql.Ident{Name: "sum"},
				FunctionCall: true,
			},
			OverWindow: &sql.WindowDefinition{
				Frame: &sql.FrameSpec{
					Rows:       pos(12),
					Between:    pos(17),
					X:          &sql.Ident{Name: "foo"},
					FollowingX: pos(29),
					Y:          &sql.Ident{Name: "bar"},
					PrecedingY: pos(47),
				},
			},
		})
		AssertParseExpr(t, `sum() over (rows between foo following and bar following)`, &sql.Call{
			Name: &sql.QualifiedName{
				Name:         &sql.Ident{Name: "sum"},
				FunctionCall: true,
			},
			OverWindow: &sql.WindowDefinition{
				Frame: &sql.FrameSpec{
					Rows:       pos(12),
					Between:    pos(17),
					X:          &sql.Ident{Name: "foo"},
					FollowingX: pos(29),
					Y:          &sql.Ident{Name: "bar"},
					FollowingY: pos(47),
				},
			},
		})
		AssertParseExpr(t, `sum() over (groups between unbounded preceding and unbounded following)`, &sql.Call{
			Name: &sql.QualifiedName{
				Name:         &sql.Ident{Name: "sum"},
				FunctionCall: true,
			},
			OverWindow: &sql.WindowDefinition{
				Frame: &sql.FrameSpec{
					Groups:     pos(12),
					Between:    pos(19),
					UnboundedX: pos(27),
					PrecedingX: pos(37),
					UnboundedY: pos(51),
					FollowingY: pos(61),
				},
			},
		})
		AssertParseExpr(t, `sum() over (groups between current row and current row)`, &sql.Call{
			Name: &sql.QualifiedName{
				Name:         &sql.Ident{Name: "sum"},
				FunctionCall: true,
			},
			OverWindow: &sql.WindowDefinition{
				Frame: &sql.FrameSpec{
					Groups:      pos(12),
					Between:     pos(19),
					CurrentRowX: pos(35),
					CurrentRowY: pos(51),
				},
			},
		})
		AssertParseExpr(t, `sum() over (groups current row exclude no others)`, &sql.Call{
			Name: &sql.QualifiedName{
				Name:         &sql.Ident{Name: "sum"},
				FunctionCall: true,
			},
			OverWindow: &sql.WindowDefinition{
				Frame: &sql.FrameSpec{
					Groups:          pos(12),
					CurrentRowX:     pos(27),
					ExcludeNoOthers: pos(42),
				},
			},
		})
		AssertParseExpr(t, `sum() over (groups current row exclude current row)`, &sql.Call{
			Name: &sql.QualifiedName{
				Name:         &sql.Ident{Name: "sum"},
				FunctionCall: true,
			},
			OverWindow: &sql.WindowDefinition{
				Frame: &sql.FrameSpec{
					Groups:            pos(12),
					CurrentRowX:       pos(27),
					ExcludeCurrentRow: pos(47),
				},
			},
		})
		AssertParseExpr(t, `sum() over (groups current row exclude group)`, &sql.Call{
			Name: &sql.QualifiedName{
				Name:         &sql.Ident{Name: "sum"},
				FunctionCall: true,
			},
			OverWindow: &sql.WindowDefinition{
				Frame: &sql.FrameSpec{
					Groups:       pos(12),
					CurrentRowX:  pos(27),
					ExcludeGroup: pos(39),
				},
			},
		})
		AssertParseExpr(t, `sum() over (groups current row exclude ties)`, &sql.Call{
			Name: &sql.QualifiedName{
				Name:         &sql.Ident{Name: "sum"},
				FunctionCall: true,
			},
			OverWindow: &sql.WindowDefinition{
				Frame: &sql.FrameSpec{
					Groups:      pos(12),
					CurrentRowX: pos(27),
					ExcludeTies: pos(39),
				},
			},
		})

		AssertParseExprError(t, `sum(`, `1:5: expected expression, found 'EOF'`)
		AssertParseExprError(t, `sum(*`, `1:6: expected right paren, found 'EOF'`)
		AssertParseExprError(t, `sum(foo foo`, `1:9: expected comma or right paren, found foo`)
		AssertParseExprError(t, `sum() filter`, `1:13: expected left paren, found 'EOF'`)
		AssertParseExprError(t, `sum() filter (`, `1:15: expected WHERE, found 'EOF'`)
		AssertParseExprError(t, `sum() filter (where`, `1:20: expected expression, found 'EOF'`)
		AssertParseExprError(t, `sum() filter (where true`, `1:25: expected right paren, found 'EOF'`)
		AssertParseExprError(t, `sum() over`, `1:11: expected left paren, found 'EOF'`)
		AssertParseExprError(t, `sum() over (partition`, `1:22: expected BY, found 'EOF'`)
		AssertParseExprError(t, `sum() over (partition by`, `1:25: expected expression, found 'EOF'`)
		AssertParseExprError(t, `sum() over (partition by foo foo`, `1:30: expected right paren, found foo`)
		AssertParseExprError(t, `sum() over (order`, `1:18: expected BY, found 'EOF'`)
		AssertParseExprError(t, `sum() over (order by`, `1:21: expected expression, found 'EOF'`)
		AssertParseExprError(t, `sum() over (order by foo foo`, `1:26: expected right paren, found foo`)
		AssertParseExprError(t, `sum() over (order by foo nulls foo`, `1:32: expected FIRST or LAST, found foo`)
		AssertParseExprError(t, `sum() over (range between`, `1:26: expected expression, found 'EOF'`)
		AssertParseExprError(t, `sum() over (range between unbounded`, `1:36: expected PRECEDING, found 'EOF'`)
		AssertParseExprError(t, `sum() over (range between current`, `1:34: expected ROW, found 'EOF'`)
		AssertParseExprError(t, `sum() over (range between foo`, `1:30: expected PRECEDING or FOLLOWING, found 'EOF'`)
		AssertParseExprError(t, `sum() over (range between foo following`, `1:40: expected AND, found 'EOF'`)
		AssertParseExprError(t, `sum() over (range between foo following and`, `1:44: expected expression, found 'EOF'`)
		AssertParseExprError(t, `sum() over (range between foo following and unbounded`, `1:54: expected FOLLOWING, found 'EOF'`)
		AssertParseExprError(t, `sum() over (range between foo following and current`, `1:52: expected ROW, found 'EOF'`)
		AssertParseExprError(t, `sum() over (range between foo following and foo`, `1:48: expected PRECEDING or FOLLOWING, found 'EOF'`)
		AssertParseExprError(t, `sum() over (range current row exclude`, `1:38: expected NO OTHERS, CURRENT ROW, GROUP, or TIES, found 'EOF'`)
		AssertParseExprError(t, `sum() over (range current row exclude no`, `1:41: expected OTHERS, found 'EOF'`)
		AssertParseExprError(t, `sum() over (range current row exclude current`, `1:46: expected ROW, found 'EOF'`)
		AssertParseExprError(t, `sum() over (range foo following`, `1:23: expected PRECEDING, found 'FOLLOWING'`)
	})

	t.Run("Cast", func(t *testing.T) {
		AssertParseExpr(t, `CAST (1 AS INTEGER)`, &sql.CastExpr{
			X:    &sql.NumberLit{Value: "1"},
			Type: &sql.Type{Name: &sql.Ident{Name: "INTEGER"}},
		})

		AssertParseExpr(t, `CAST (20 AS SOME TYPE)`, &sql.CastExpr{
			X:    &sql.NumberLit{Value: "20"},
			Type: &sql.Type{Name: &sql.Ident{Name: "SOME TYPE"}},
		})

		AssertParseExprError(t, `CAST`, `1:5: expected left paren, found 'EOF'`)
		AssertParseExprError(t, `CAST (`, `1:7: expected expression, found 'EOF'`)
		AssertParseExprError(t, `CAST (1`, `1:8: expected AS, found 'EOF'`)
		AssertParseExprError(t, `CAST (1 AS`, `1:11: expected type name, found 'EOF'`)
		AssertParseExprError(t, `CAST (1 AS INTEGER`, `1:19: expected right paren, found 'EOF'`)
	})

	t.Run("Case", func(t *testing.T) {
		AssertParseExpr(t, `CASE 1 WHEN 2 THEN 3 WHEN 4 THEN 5 ELSE 6 END`, &sql.CaseExpr{
			Operand: &sql.NumberLit{Value: "1"},
			Blocks: []*sql.CaseBlock{
				{
					Condition: &sql.NumberLit{Value: "2"},
					Body:      &sql.NumberLit{Value: "3"},
				},
				{
					Condition: &sql.NumberLit{Value: "4"},
					Body:      &sql.NumberLit{Value: "5"},
				},
			},
			ElseExpr: &sql.NumberLit{Value: "6"},
		})
		AssertParseExpr(t, `CASE WHEN 1 THEN 2 END`, &sql.CaseExpr{
			Blocks: []*sql.CaseBlock{
				{
					Condition: &sql.NumberLit{Value: "1"},
					Body:      &sql.NumberLit{Value: "2"},
				},
			},
		})
		AssertParseExpr(t, `CASE WHEN 1 IS NULL THEN 2 END`, &sql.CaseExpr{
			Blocks: []*sql.CaseBlock{
				{
					Condition: &sql.Null{
						X:  &sql.NumberLit{Value: "1"},
						Op: sql.OP_ISNULL,
					},
					Body: &sql.NumberLit{Value: "2"},
				},
			},
		})
		AssertParseExprError(t, `CASE`, `1:5: expected expression, found 'EOF'`)
		AssertParseExprError(t, `CASE 1`, `1:7: expected WHEN, found 'EOF'`)
		AssertParseExprError(t, `CASE WHEN`, `1:10: expected expression, found 'EOF'`)
		AssertParseExprError(t, `CASE WHEN 1`, `1:12: expected THEN, found 'EOF'`)
		AssertParseExprError(t, `CASE WHEN 1 THEN`, `1:17: expected expression, found 'EOF'`)
		AssertParseExprError(t, `CASE WHEN 1 THEN 2`, `1:19: expected WHEN, ELSE or END, found 'EOF'`)
		AssertParseExprError(t, `CASE WHEN 1 THEN 2 ELSE`, `1:24: expected expression, found 'EOF'`)
		AssertParseExprError(t, `CASE WHEN 1 THEN 2 ELSE 3`, `1:26: expected END, found 'EOF'`)
	})

	t.Run("Raise", func(t *testing.T) {
		AssertParseExpr(t, `RAISE(IGNORE)`, &sql.Raise{
			Ignore: pos(6),
		})
		AssertParseExpr(t, `RAISE(ROLLBACK, 'bad error')`, &sql.Raise{
			Rollback: pos(6),
			Error:    &sql.StringLit{Value: "bad error"},
		})
		AssertParseExpr(t, `RAISE(ABORT, 'error')`, &sql.Raise{
			Abort: pos(6),
			Error: &sql.StringLit{Value: "error"},
		})
		AssertParseExpr(t, `RAISE(FAIL, 'error')`, &sql.Raise{
			Fail:  pos(6),
			Error: &sql.StringLit{Value: "error"},
		})
		AssertParseExprError(t, `RAISE`, `1:6: expected left paren, found 'EOF'`)
		AssertParseExprError(t, `RAISE(`, `1:7: expected IGNORE, ROLLBACK, ABORT, or FAIL, found 'EOF'`)
		AssertParseExprError(t, `RAISE(IGNORE`, `1:13: expected right paren, found 'EOF'`)
		AssertParseExprError(t, `RAISE(ROLLBACK`, `1:15: expected comma, found 'EOF'`)
		AssertParseExprError(t, `RAISE(ROLLBACK,`, `1:16: expected error message, found 'EOF'`)
	})
}

func Test_ParseMultiStmtString(t *testing.T) {
	i := 0
	expected := []string{
		`SELECT 1`,
		`SELECT 2`,
		`CREATE TABLE "tbl" AS SELECT "foo"`,
	}

	if err := sql.ParseMultiStmtString(strings.Join(expected[:], ";\n"), func(stmt sql.Statement) error {
		if expected[i] != stmt.String() {
			t.Fatalf("expected %q, got %q", expected[i], stmt.String())
		}

		i++
		return nil
	}); err != nil {
		t.Fatalf("ParseMultiStmtString failed: %v", err)
	}

	if i != len(expected) {
		t.Fatalf("expected %d statements, got %d", len(expected), i)
	}
}

func TestError_Error(t *testing.T) {
	err := &sql.Error{Msg: "test"}
	if got, want := err.Error(), `-: test`; got != want {
		t.Fatalf("Error()=%s, want %s", got, want)
	}
}

// ParseStatementOrFail parses a statement from s. Fail on error.
func ParseStatementOrFail(tb testing.TB, s string) sql.Statement {
	tb.Helper()
	stmt, err := sql.ParseStmtString(s)
	if err != nil {
		tb.Fatal(err)
	}
	return stmt
}

// AssertParseStatement asserts the value of the first parse of s.
func AssertParseStatement(tb testing.TB, s string, want sql.Statement) {
	tb.Helper()
	stmt, err := sql.ParseStmtString(s)
	if err != nil {
		tb.Fatal(err)
	} else if diff := deep.Equal(stmt, want); diff != nil {
		tb.Fatalf("mismatch:\n%s", strings.Join(diff, "\n"))
	}
}

// AssertParseStatementError asserts s parses to a given error string.
func AssertParseStatementError(tb testing.TB, s string, want string) {
	tb.Helper()
	_, err := sql.ParseStmtString(s)
	if err == nil || err.Error() != want {
		tb.Fatalf("ParseStatement()=%q, want %q", err, want)
	}
}

// ParseExprOrFail parses a expression from s. Fail on error.
func ParseExprOrFail(tb testing.TB, s string) sql.Expr {
	tb.Helper()
	stmt, err := sql.ParseExprString(s)
	if err != nil {
		tb.Fatal(err)
	}
	return stmt
}

// AssertParseExpr asserts the value of the first parse of s.
func AssertParseExpr(tb testing.TB, s string, want sql.Expr) {
	tb.Helper()
	stmt, err := sql.ParseExprString(s)
	if err != nil {
		tb.Fatal(err)
	} else if diff := deep.Equal(stmt, want); diff != nil {
		tb.Fatalf("mismatch:\n%s", strings.Join(diff, "\n"))
	}
}

// AssertParseExprError asserts s parses to a given error string.
func AssertParseExprError(tb testing.TB, s string, want string) {
	tb.Helper()
	_, err := sql.ParseExprString(s)
	if err == nil || err.Error() != want {
		tb.Fatalf("ParseExpr()=%q, want %q", err, want)
	}
}

func deepEqual(a, b interface{}) string {
	return strings.Join(deep.Equal(a, b), "\n")
}
