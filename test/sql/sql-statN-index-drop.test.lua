test_run = require('test_run').new()
engine = test_run:get_cfg('engine')
box.execute('pragma sql_default_engine=\''..engine..'\'')

-- Initializing some things.
box.execute("CREATE TABLE t1(id INT PRIMARY KEY, a INT);")
box.execute("CREATE TABLE t2(id INT PRIMARY KEY, a INT);")
box.execute("CREATE INDEX i1 ON t1(a);")
box.execute("CREATE INDEX i1 ON t2(a);")
box.execute("INSERT INTO t1 VALUES(1, 2);")
box.execute("INSERT INTO t2 VALUES(1, 2);")

-- Analyze.
box.execute("ANALYZE;")

-- Checking the data.
box.execute("SELECT * FROM \"_sql_stat4\";")
box.execute("SELECT * FROM \"_sql_stat1\";")

-- Dropping an index.
box.execute("DROP INDEX i1 ON t1;")

-- Checking the DROP INDEX results.
box.execute("SELECT * FROM \"_sql_stat4\";")
box.execute("SELECT * FROM \"_sql_stat1\";")

--Cleaning up.
box.execute("DROP TABLE t1;")
box.execute("DROP TABLE t2;")

-- Same test but dropping an INDEX ON t2.

box.execute("CREATE TABLE t1(id INT PRIMARY KEY, a INT);")
box.execute("CREATE TABLE t2(id INT PRIMARY KEY, a INT);")
box.execute("CREATE INDEX i1 ON t1(a);")
box.execute("CREATE INDEX i1 ON t2(a);")
box.execute("INSERT INTO t1 VALUES(1, 2);")
box.execute("INSERT INTO t2 VALUES(1, 2);")

-- Analyze.
box.execute("ANALYZE;")

-- Checking the data.
box.execute("SELECT * FROM \"_sql_stat4\";")
box.execute("SELECT * FROM \"_sql_stat1\";")

-- Dropping an index.
box.execute("DROP INDEX i1 ON t2;")

-- Checking the DROP INDEX results.
box.execute("SELECT * FROM \"_sql_stat4\";")
box.execute("SELECT * FROM \"_sql_stat1\";")

--Cleaning up.
box.execute("DROP TABLE t1;")
box.execute("DROP TABLE t2;")
