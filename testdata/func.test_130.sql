-- func.test
-- 
-- execsql {
--     SELECT match(a,b) FROM t1 WHERE 0;
-- }
SELECT match_func(a,b) FROM t1 WHERE 0;
