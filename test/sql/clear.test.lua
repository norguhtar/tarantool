test_run = require('test_run').new()
engine = test_run:get_cfg('engine')
box.execute('pragma sql_default_engine=\''..engine..'\'')

-- box.cfg()

-- create space
box.execute("CREATE TABLE zoobar (c1 INT, c2 INT PRIMARY KEY, c3 TEXT, c4 INT)")
box.execute("CREATE UNIQUE INDEX zoobar2 ON zoobar(c1, c4)")

-- Debug
-- box.execute("PRAGMA vdbe_debug=ON;")

-- Seed entry
for i=1, 100 do box.execute(string.format("INSERT INTO zoobar VALUES (%d, %d, 'c3', 444)", i+i, i)) end

-- Check table is not empty
box.execute("SELECT * FROM zoobar")

-- Do clean up
box.execute("DELETE FROM zoobar")

-- Make sure table is empty
box.execute("SELECT * from zoobar")

-- Cleanup
box.execute("DROP INDEX zoobar2 ON zoobar")
box.execute("DROP TABLE zoobar")

-- Debug
-- require("console").start()
