package main

import (
	"fmt"

	"github.com/TcMits/sql"
)

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

	switch stmt.(type) {
	case *sql.SelectStatement, *sql.ExplainStatement:
		fmt.Println("The statement is a readonly statement.")
	default:
		fmt.Println("The statement is not a readonly statement.")
	}
}

