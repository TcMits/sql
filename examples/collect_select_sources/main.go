package main

import (
	"fmt"

	"github.com/TcMits/sql"
)

func tableNamesFromSelect(s *sql.SelectStatement, names []string) []string {
	return tableNamesFromSource(s.Source, names)
}

func tableNamesFromSource(source sql.Source, names []string) []string {
	switch source := source.(type) {
	case *sql.JoinClause:
		return tableNamesFromSource(source.Y, tableNamesFromSource(source.X, names))
	case *sql.ParenSource:
		return tableNamesFromSource(source.X, names)
	case *sql.QualifiedName:
		if source.FunctionCall {
			return names
		}

		name := ""
		if source.Schema != nil {
			name = source.Schema.Name + "."
		}

		return append(names, name+source.Name.Name)
	case *sql.SelectStatement:
		return tableNamesFromSelect(source, names)
	default:
		panic("unknown source type: " + source.String())
	}
}

func main() {
	s := `WITH derived AS (
		SELECT MAX(a) AS max_a,
					 COUNT(b) AS b_num,
					 user_id
		FROM table_name_1
		GROUP BY user_id
)
SELECT * FROM table_name_2
LEFT JOIN derived USING (user_id)`
	stmt, err := sql.ParseStmtString(s)
	if err != nil {
		panic(err)
	}

	tableNames := make([]string, 0)
	sql.Walk(stmt, func(node sql.Node) bool {
		switch n := node.(type) {
		case *sql.SelectStatement:
			tableNames = tableNamesFromSelect(n, tableNames)
		}

		return true
	})

	fmt.Println("Table names found in the query: ", tableNames)
}
