-- limit.test
-- 
-- db eval {
--     SELECT x FROM t1 LIMIT :limit OFFSET :offset;
-- }
SELECT x FROM t1 LIMIT 1 OFFSET 20000;
