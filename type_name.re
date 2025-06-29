package sql

// Returns "fake" terminating null if cursor has reached limit.
func peekIsTypeName(str string, cur int) byte {
	if cur >= len(str) {
		return 0 // fake null
	} else {
		return str[cur]
	}
}

//run script: re2c $INPUT --lang go -o $OUTPUT --bit-vectors --nested-ifs
func isTypeName(str string) bool {
  var cur, marker int

	/*!re2c
		re2c:YYCTYPE = byte;
		re2c:yyfill:enable = 0;
		re2c:YYPEEK = "peekIsTypeName(str, cur)";
		re2c:YYSKIP = "cur += 1";
		re2c:YYBACKUP = "marker = cur";
    re2c:YYRESTORE = "cur = marker";

    end = "\x00";
	('BIGINT' | 'BLOB' | 'BOOLEAN' | 'CHARACTER' | 'CLOB' | 'DATE' | 'DATETIME' |
		'DECIMAL' | 'DOUBLE' | 'FLOAT' | 'INT' | 'INTEGER' | 'MEDIUMINT' | 'NCHAR' |
		'NUMERIC' | 'NVARCHAR' | 'REAL' | 'SMALLINT' | 'TEXT' | 'TINYINT' | 'VARCHAR') end { return true }
    * { return false }
	*/
}
