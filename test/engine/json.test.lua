test_run = require('test_run').new()
engine = test_run:get_cfg('engine')
--
-- gh-1012: Indexes for JSON-defined paths.
--
s = box.schema.space.create('withdata', {engine = engine})
s:create_index('test1', {parts = {{2, 'number'}, {3, 'str', path = 'FIO["fname"]'}, {3, 'str', path = '["FIO"].fname'}}})
s:create_index('test1', {parts = {{2, 'number'}, {3, 'str', path = 666}, {3, 'str', path = '["FIO"]["fname"]'}}})
s:create_index('test1', {parts = {{2, 'number'}, {3, 'map', path = 'FIO'}}})
s:create_index('test1', {parts = {{2, 'number'}, {3, 'array', path = '[1]'}}})
s:create_index('test1', {parts = {{2, 'number'}, {3, 'str', path = 'FIO'}, {3, 'str', path = 'FIO.fname'}}})
s:create_index('test1', {parts = {{2, 'number'}, {3, 'str', path = '[1].sname'}, {3, 'str', path = '["FIO"].fname'}}})
s:create_index('test1', {parts = {{2, 'number'}, {3, 'str', path = 'FIO....fname'}}})
idx = s:create_index('test1', {parts = {{2, 'number'}, {3, 'str', path = 'FIO.fname', is_nullable = false}, {3, 'str', path = '["FIO"]["sname"]'}}})
assert(idx ~= nil)
assert(idx.parts[2].path == 'FIO.fname')
format = {{'id', 'unsigned'}, {'meta', 'unsigned'}, {'data', 'array'}, {'age', 'unsigned'}, {'level', 'unsigned'}}
s:format(format)
format = {{'id', 'unsigned'}, {'meta', 'unsigned'}, {'data', 'map'}, {'age', 'unsigned'}, {'level', 'unsigned'}}
s:format(format)
s:create_index('test2', {parts = {{2, 'number'}, {3, 'number', path = 'FIO.fname'}, {3, 'str', path = '["FIO"]["sname"]'}}})
s:insert{7, 7, {town = 'London', FIO = 666}, 4, 5}
s:insert{7, 7, {town = 'London', FIO = {fname = 666, sname = 'Bond'}}, 4, 5}
s:insert{7, 7, {town = 'London', FIO = {fname = "James"}}, 4, 5}
s:insert{7, 7, {town = 'London', FIO = {fname = 'James', sname = 'Bond'}}, 4, 5}
s:insert{7, 7, {town = 'London', FIO = {fname = 'James', sname = 'Bond'}}, 4, 5}
s:insert{7, 7, {town = 'London', FIO = {fname = 'James', sname = 'Bond', data = "extra"}}, 4, 5}
s:insert{7, 7, {town = 'Moscow', FIO = {fname = 'Max', sname = 'Isaev', data = "extra"}}, 4, 5}
idx:select()
idx:min()
idx:max()
s:drop()

s = box.schema.create_space('withdata', {engine = engine})
parts = {}
parts[1] = {1, 'unsigned', path='[2]'}
pk = s:create_index('pk', {parts = parts})
s:insert{{1, 2}, 3}
s:upsert({{box.null, 2}}, {{'+', 2, 5}})
s:get(2)
s:drop()

-- Create index on space with data
s = box.schema.space.create('withdata', {engine = engine})
pk = s:create_index('primary', { type = 'tree' })
s:insert{1, 7, {town = 'London', FIO = 1234}, 4, 5}
s:insert{2, 7, {town = 'London', FIO = {fname = 'James', sname = 'Bond'}}, 4, 5}
s:insert{3, 7, {town = 'London', FIO = {fname = 'James', sname = 'Bond'}}, 4, 5}
s:insert{4, 7, {town = 'London', FIO = {1,2,3}}, 4, 5}
s:create_index('test1', {parts = {{2, 'number'}, {3, 'str', path = '["FIO"]["fname"]'}, {3, 'str', path = '["FIO"]["sname"]'}}})
_ = s:delete(1)
s:create_index('test1', {parts = {{2, 'number'}, {3, 'str', path = '["FIO"]["fname"]'}, {3, 'str', path = '["FIO"]["sname"]'}}})
_ = s:delete(2)
s:create_index('test1', {parts = {{2, 'number'}, {3, 'str', path = '["FIO"]["fname"]'}, {3, 'str', path = '["FIO"]["sname"]'}}})
_ = s:delete(4)
idx = s:create_index('test1', {parts = {{2, 'number'}, {3, 'str', path = '["FIO"]["fname"]', is_nullable = true}, {3, 'str', path = '["FIO"]["sname"]'}, {3, 'str', path = '["FIO"]["extra"]', is_nullable = true}}})
assert(idx ~= nil)
s:create_index('test2', {parts = {{2, 'number'}, {3, 'number', path = '["FIO"]["fname"]'}}})
idx2 = s:create_index('test2', {parts = {{2, 'number'}, {3, 'str', path = '["FIO"]["fname"]'}}})
assert(idx2 ~= nil)
t = s:insert{5, 7, {town = 'Matrix', FIO = {fname = 'Agent', sname = 'Smith'}}, 4, 5}
idx:select()
idx:min()
idx:max()
idx:drop()
s:drop()

-- Test complex JSON indexes
s = box.schema.space.create('withdata', {engine = engine})
parts = {}
parts[1] = {1, 'str', path='[3][2].a'}
parts[2] = {1, 'unsigned', path = '[3][1]'}
parts[3] = {2, 'str', path = '[2].d[1]'}
pk = s:create_index('primary', { type = 'tree', parts =  parts})
s:insert{{1, 2, {3, {3, a = 'str', b = 5}}}, {'c', {d = {'e', 'f'}, e = 'g'}}, 6, {1, 2, 3}}
s:insert{{1, 2, {3, {a = 'str', b = 1}}}, {'c', {d = {'e', 'f'}, e = 'g'}}, 6}
parts = {}
parts[1] = {4, 'unsigned', path='[1]', is_nullable = false}
parts[2] = {4, 'unsigned', path='[2]', is_nullable = true}
parts[3] = {4, 'unsigned', path='[4]', is_nullable = true}
trap_idx = s:create_index('trap', { type = 'tree', parts = parts})
s:insert{{1, 2, {3, {3, a = 'str2', b = 5}}}, {'c', {d = {'e', 'f'}, e = 'g'}}, 6, {}}
parts = {}
parts[1] = {1, 'unsigned', path='[3][2].b' }
parts[2] = {3, 'unsigned'}
crosspart_idx = s:create_index('crosspart', { parts =  parts})
s:insert{{1, 2, {3, {a = 'str2', b = 2}}}, {'c', {d = {'e', 'f'}, e = 'g'}}, 6, {9, 2, 3}}
parts = {}
parts[1] = {1, 'unsigned', path='[3][2].b'}
num_idx = s:create_index('numeric', {parts =  parts})
s:insert{{1, 2, {3, {a = 'str3', b = 9}}}, {'c', {d = {'e', 'f'}, e = 'g'}}, 6, {0}}
num_idx:get(2)
num_idx:select()
num_idx:max()
num_idx:min()
assert(crosspart_idx:max() == num_idx:max())
assert(crosspart_idx:min() == num_idx:min())
trap_idx:max()
trap_idx:min()
s:drop()

s = box.schema.space.create('withdata', {engine = engine})
pk_simplified = s:create_index('primary', { type = 'tree',  parts = {{1, 'unsigned'}}})
assert(pk_simplified.path == box.NULL)
idx = s:create_index('idx', {parts = {{2, 'integer', path = 'a'}}})
s:insert{31, {a = 1, aa = -1}}
s:insert{22, {a = 2, aa = -2}}
s:insert{13, {a = 3, aa = -3}}
idx:select()
idx:alter({parts = {{2, 'integer', path = 'aa'}}})
idx:select()
s:drop()

-- incompatible format change
s = box.schema.space.create('test')
i = s:create_index('pk', {parts = {{1, 'integer', path = '[1]'}}})
s:insert{{-1}}
i:alter{parts = {{1, 'string', path = '[1]'}}}
s:insert{{'a'}}
i:drop()
i = s:create_index('pk', {parts = {{1, 'integer', path = '[1].FIO'}}})
s:insert{{{FIO=-1}}}
i:alter{parts = {{1, 'integer', path = '[1][1]'}}}
i:alter{parts = {{1, 'integer', path = '[1].FIO[1]'}}}
s:drop()

engine = nil
test_run = nil

