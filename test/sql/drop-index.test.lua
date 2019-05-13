test_run = require('test_run').new()
engine = test_run:get_cfg('engine')
box.execute('pragma sql_default_engine=\''..engine..'\'')

-- box.cfg()

-- create space
box.execute("CREATE TABLE zzoobar (c1 FLOAT, c2 INT PRIMARY KEY, c3 TEXT, c4 FLOAT)")

box.execute("CREATE UNIQUE INDEX zoobar2 ON zzoobar(c1, c4)")
box.execute("CREATE        INDEX zoobar3 ON zzoobar(c3)")

-- Debug
-- box.execute("PRAGMA vdbe_debug=ON ; INSERT INTO zzoobar VALUES (111, 222, 'c3', 444)")

-- Dummy entry
box.execute("INSERT INTO zzoobar VALUES (111, 222, 'c3', 444)")

box.execute("DROP INDEX zoobar2 ON zzoobar")
box.execute("DROP INDEX zoobar3 On zzoobar")

-- zoobar2 is dropped - should be OK
box.execute("INSERT INTO zzoobar VALUES (111, 223, 'c3', 444)")

-- zoobar2 was dropped. Re-creation should  be OK
box.execute("CREATE INDEX zoobar2 ON zzoobar(c3)")

-- Cleanup
box.execute("DROP INDEX zoobar2 ON zzoobar")
box.execute("DROP TABLE zzoobar")

-- Debug
-- require("console").start()
