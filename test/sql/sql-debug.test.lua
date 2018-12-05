remote = require('net.box')
test_run = require('test_run').new()
engine = test_run:get_cfg('engine')
box.sql.execute('pragma sql_default_engine=\''..engine..'\'')

--
-- gh-3832: Some statements do not return column type

-- Check that "PRAGMA parser_trace" returns 0 or 1 if called
-- without parameter.
result = box.sql.execute('PRAGMA parser_trace')
-- Should be nothing.
box.sql.execute('PRAGMA parser_trace = 1')
-- Should be 1.
box.sql.execute('PRAGMA parser_trace')
-- Should be nothing.
box.sql.execute('PRAGMA parser_trace = '.. result[1][1])

--
-- Make PRAGMA command return the result in a more appropriate
-- format.
--
result = box.sql.execute('PRAGMA')
for _,v in pairs(result) do v[2] = nil end
result