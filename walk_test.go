package sql_test

import (
	"reflect"
	"testing"

	"github.com/TcMits/sql"
)

func Test_Walk(t *testing.T) {
	s := `WITH derived AS (
		SELECT MAX(a) AS max_a,
					 COUNT(b) AS b_num,
					 user_id
		FROM table_name
		GROUP BY user_id
)
SELECT * FROM table_name
LEFT JOIN derived USING (user_id)`
	stmt, err := sql.ParseStmtString(s)
	if err != nil {
		t.Fatal(err)
	}

	expected := [...]reflect.Type{
		reflect.TypeOf(&sql.SelectStatement{}),

		// WITH derived
		reflect.TypeOf(&sql.WithClause{}),
		reflect.TypeOf(&sql.CTE{}),
		reflect.TypeOf(&sql.Ident{}),
		reflect.TypeOf(&sql.SelectStatement{}),

		// max(a) as max_a
		reflect.TypeOf(&sql.ResultColumn{}),
		reflect.TypeOf(&sql.Call{}),
		reflect.TypeOf(&sql.QualifiedName{}),
		reflect.TypeOf(&sql.Ident{}),
		reflect.TypeOf(&sql.FunctionArg{}),
		reflect.TypeOf(&sql.Ident{}),
		reflect.TypeOf(&sql.Ident{}),

		// count(b) AS b_num
		reflect.TypeOf(&sql.ResultColumn{}),
		reflect.TypeOf(&sql.Call{}),
		reflect.TypeOf(&sql.QualifiedName{}),
		reflect.TypeOf(&sql.Ident{}),
		reflect.TypeOf(&sql.FunctionArg{}),
		reflect.TypeOf(&sql.Ident{}),
		reflect.TypeOf(&sql.Ident{}),

		// user_id
		reflect.TypeOf(&sql.ResultColumn{}),
		reflect.TypeOf(&sql.Ident{}),

		// FROM table_name
		reflect.TypeOf(&sql.QualifiedName{}),
		reflect.TypeOf(&sql.Ident{}),

		// GROUP BY user_id
		reflect.TypeOf(&sql.Ident{}),

		// SELECT *
		reflect.TypeOf(&sql.ResultColumn{}),

		reflect.TypeOf(&sql.JoinClause{}),

		// FROM table_name
		reflect.TypeOf(&sql.QualifiedName{}),
		reflect.TypeOf(&sql.Ident{}),

		// LEFT JOIN
		reflect.TypeOf(&sql.JoinOperator{}),

		// derived
		reflect.TypeOf(&sql.QualifiedName{}),
		reflect.TypeOf(&sql.Ident{}),

		// USING (user_id)
		reflect.TypeOf(&sql.UsingConstraint{}),
		reflect.TypeOf(&sql.Ident{}),
	}

	i := 0
	yield := func(n sql.Node) bool {
		v := reflect.ValueOf(n)

		if v.Type() != expected[i] {
			t.Fatalf("expected type %s, got %s %v %v at index %d", expected[i], v.Type(), v.IsZero(), v.IsNil(), i)
		}

		i++
		return true
	}

	sql.Walk(stmt, yield)

	if i != len(expected) {
		t.Errorf("expected %d nodes, got %d", len(expected), i)
	}
}

func Benchmark_Walk(b *testing.B) {
	s := `WITH derived AS (
		SELECT MAX(a) AS max_a,
					 COUNT(b) AS b_num,
					 user_id
		FROM table_name
		GROUP BY user_id
)
SELECT * FROM table_name
LEFT JOIN derived USING (user_id)`
	stmt, err := sql.ParseStmtString(s)
	if err != nil {
		b.Fatal(err)
	}

	yield := func(n sql.Node) bool { return true }
	for b.Loop() {
		sql.Walk(stmt, yield)
	}
}
