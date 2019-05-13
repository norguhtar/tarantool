test_run = require('test_run').new()
engine = test_run:get_cfg('engine')
box.execute('pragma sql_default_engine=\''..engine..'\'')
-- box.cfg()

-- create space
box.execute("CREATE TABLE t1(a INT,b INT,c INT,PRIMARY KEY(b,c));")

-- Debug
-- box.execute("PRAGMA vdbe_debug=ON ; INSERT INTO zoobar VALUES (111, 222, 'c3', 444)")

-- Seed entries
box.execute("WITH RECURSIVE cnt(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM cnt WHERE x<1000) INSERT INTO t1 SELECT x, x%40, x/40 FROM cnt;")

-- Ephemeral table is not belong to Tarantool, so must be cleared sql-way.
box.execute("SELECT a FROM t1 ORDER BY b, a LIMIT 10 OFFSET 20;");

-- Cleanup
box.execute("DROP TABLE t1")

-- Debug
-- require("console").start()
