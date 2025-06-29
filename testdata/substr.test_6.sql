-- substr.test
-- 
-- execsql {
--       SELECT hex(substr(x'hex', i1, i2))
-- }
SELECT hex(substr(x'0500', i1, i2))
