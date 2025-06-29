package sql

// Returns "fake" terminating null if cursor has reached limit.
func peekKeywordOrIdent(str string, cur int) byte {
	if cur >= len(str) {
		return 0 // fake null
	} else {
		return str[cur]
	}
}

//run script: re2c $INPUT --lang go -o $OUTPUT --bit-vectors --nested-ifs
func keywordOrIdent(str string) Token {
  var cur, marker int

	/*!re2c
		re2c:YYCTYPE = byte;
		re2c:yyfill:enable = 0;
		re2c:YYPEEK = "peekKeywordOrIdent(str, cur)";
		re2c:YYSKIP = "cur += 1";
		re2c:YYBACKUP = "marker = cur";
    re2c:YYRESTORE = "cur = marker";

    end = "\x00";
    'NULL' end { return NULL }
    'TRUE' end { return TRUE }
    'FALSE' end { return FALSE }

    'STRICT' end { return STRICT }
    'ROWID' end { return ROWID }
    'STORED' end { return STORED }

    'ABORT' end { return ABORT }
    'ACTION' end { return ACTION }
    'ADD' end { return ADD }
    'AFTER' end { return AFTER }
    'ALL' end { return ALL }
    'ALTER' end { return ALTER }
    'ALWAYS' end { return ALWAYS }
    'ANALYZE' end { return ANALYZE }
    'AND' end { return AND }
    'AS' end { return AS }
    'ASC' end { return ASC }
    'ATTACH' end { return ATTACH }
    'AUTOINCREMENT' end { return AUTOINCREMENT }
    'BEFORE' end { return BEFORE }
    'BEGIN' end { return BEGIN }
    'BETWEEN' end { return BETWEEN }
    'BY' end { return BY }
    'CASCADE' end { return CASCADE }
    'CASE' end { return CASE }
    'CAST' end { return CAST }
    'CHECK' end { return CHECK }
    'COLLATE' end { return COLLATE }
    'COLUMN' end { return COLUMN }
    'COMMIT' end { return COMMIT }
    'CONFLICT' end { return CONFLICT }
    'CONSTRAINT' end { return CONSTRAINT }
    'CREATE' end { return CREATE }
    'CROSS' end { return CROSS }
    'CURRENT' end { return CURRENT }
    'CURRENT_DATE' end { return CURRENT_DATE }
    'CURRENT_TIME' end { return CURRENT_TIME }
    'CURRENT_TIMESTAMP' end { return CURRENT_TIMESTAMP }
    'DATABASE' end { return DATABASE }
    'DEFAULT' end { return DEFAULT }
    'DEFERRABLE' end { return DEFERRABLE }
    'DEFERRED' end { return DEFERRED }
    'DELETE' end { return DELETE }
    'DESC' end { return DESC }
    'DETACH' end { return DETACH }
    'DISTINCT' end { return DISTINCT }
    'DO' end { return DO }
    'DROP' end { return DROP }
    'EACH' end { return EACH }
    'ELSE' end { return ELSE }
    'END' end { return END }
    'ESCAPE' end { return ESCAPE }
    'EXCEPT' end { return EXCEPT }
    'EXCLUDE' end { return EXCLUDE }
    'EXCLUSIVE' end { return EXCLUSIVE }
    'EXISTS' end { return EXISTS }
    'EXPLAIN' end { return EXPLAIN }
    'FAIL' end { return FAIL }
    'FILTER' end { return FILTER }
    'FIRST' end { return FIRST }
    'FOLLOWING' end { return FOLLOWING }
    'FOR' end { return FOR }
    'FOREIGN' end { return FOREIGN }
    'FROM' end { return FROM }
    'FULL' end { return FULL }
    'GENERATED' end { return GENERATED }
    'GLOB' end { return GLOB }
    'GROUP' end { return GROUP }
    'GROUPS' end { return GROUPS }
    'HAVING' end { return HAVING }
    'IF' end { return IF }
    'IGNORE' end { return IGNORE }
    'IMMEDIATE' end { return IMMEDIATE }
    'IN' end { return IN }
    'INDEX' end { return INDEX }
    'INDEXED' end { return INDEXED }
    'INITIALLY' end { return INITIALLY }
    'INNER' end { return INNER }
    'INSERT' end { return INSERT }
    'INSTEAD' end { return INSTEAD }
    'INTERSECT' end { return INTERSECT }
    'INTO' end { return INTO }
    'IS' end { return IS }
    'ISNULL' end { return ISNULL }
    'JOIN' end { return JOIN }
    'KEY' end { return KEY }
    'LAST' end { return LAST }
    'LEFT' end { return LEFT }
    'LIKE' end { return LIKE }
    'LIMIT' end { return LIMIT }
    'MATCH' end { return MATCH }
    'MATERIALIZED' end { return MATERIALIZED }
    'NATURAL' end { return NATURAL }
    'NO' end { return NO }
    'NOT' end { return NOT }
    'NOTHING' end { return NOTHING }
    'NOTNULL' end { return NOTNULL }
    'NULLS' end { return NULLS }
    'OF' end { return OF }
    'OFFSET' end { return OFFSET }
    'ON' end { return ON }
    'OR' end { return OR }
    'ORDER' end { return ORDER }
    'OTHERS' end { return OTHERS }
    'OUTER' end { return OUTER }
    'OVER' end { return OVER }
    'PARTITION' end { return PARTITION }
    'PLAN' end { return PLAN }
    'PRAGMA' end { return PRAGMA }
    'PRECEDING' end { return PRECEDING }
    'PRIMARY' end { return PRIMARY }
    'QUERY' end { return QUERY }
    'RAISE' end { return RAISE }
    'RANGE' end { return RANGE }
    'RECURSIVE' end { return RECURSIVE }
    'REFERENCES' end { return REFERENCES }
    'REGEXP' end { return REGEXP }
    'REINDEX' end { return REINDEX }
    'RELEASE' end { return RELEASE }
    'RENAME' end { return RENAME }
    'REPLACE' end { return REPLACE }
    'RESTRICT' end { return RESTRICT }
    'RETURNING' end { return RETURNING }
    'RIGHT' end { return RIGHT }
    'ROLLBACK' end { return ROLLBACK }
    'ROW' end { return ROW }
    'ROWS' end { return ROWS }
    'SAVEPOINT' end { return SAVEPOINT }
    'SELECT' end { return SELECT }
    'SET' end { return SET }
    'TABLE' end { return TABLE }
    'TEMP' end { return TEMP }
    'TEMPORARY' end { return TEMPORARY }
    'THEN' end { return THEN }
    'TIES' end { return TIES }
    'TO' end { return TO }
    'TRANSACTION' end { return TRANSACTION }
    'TRIGGER' end { return TRIGGER }
    'UNBOUNDED' end { return UNBOUNDED }
    'UNION' end { return UNION }
    'UNIQUE' end { return UNIQUE }
    'UPDATE' end { return UPDATE }
    'USING' end { return USING }
    'VACUUM' end { return VACUUM }
    'VALUES' end { return VALUES }
    'VIEW' end { return VIEW }
    'VIRTUAL' end { return VIRTUAL }
    'WHEN' end { return WHEN }
    'WHERE' end { return WHERE }
    'WINDOW' end { return WINDOW }
    'WITH' end { return WITH }
    'WITHOUT' end { return WITHOUT }
    * { return IDENT }
	*/
}
