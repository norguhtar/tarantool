test_run = require('test_run').new()
engine = test_run:get_cfg('engine')
box.sql.execute('pragma sql_default_engine=\''..engine..'\'')

-- Forbid multistatement queries.
box.sql.execute('select 1;')
box.sql.execute('select 1; select 2;')
box.sql.execute('create table t1 (id INT primary key); select 100;')
box.space.t1 == nil
box.sql.execute(';')
box.sql.execute('')
box.sql.execute('     ;')
box.sql.execute('\n\n\n\t\t\t   ')

--
-- gh-3832: Some statements do not return column type

-- Check that "PRAGMA case_sensitive_like" returns 0 or 1 if
-- called without parameter.
result = box.sql.execute('PRAGMA case_sensitive_like')
-- Should be nothing.
box.sql.execute('PRAGMA case_sensitive_like = 1')
-- Should be 1.
box.sql.execute('PRAGMA case_sensitive_like')
-- Should be nothing.
box.sql.execute('PRAGMA case_sensitive_like = '.. result[1][1])
