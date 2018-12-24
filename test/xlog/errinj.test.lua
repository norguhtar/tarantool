--
-- we actually need to know what xlogs the server creates,
-- so start from a clean state
--
-- 
-- Check how well we handle a failed log write
-- in panic_on_wal_error=false mode
--
env = require('test_run')
test_run = env.new()
test_run:cmd('restart server default with cleanup=1')

box.error.injection.set("ERRINJ_WAL_WRITE", true)
box.space._schema:insert{"key"}
box.info.lsn
test_run:cmd('restart server default')
box.info.lsn
box.space._schema:insert{"key"}
box.info.lsn
test_run:cmd('restart server default')
box.info.lsn
box.space._schema:get{"key"}
box.space._schema:delete{"key"}
-- list all the logs
name = string.match(arg[0], "([^,]+)%.lua")
require('fio').glob(name .. "/*.xlog")
test_run:cmd('restart server default with cleanup=1')

-- gh-881 iproto request with wal IO error
errinj = box.error.injection

test = box.schema.create_space('test')
_ = test:create_index('primary')

box.schema.user.grant('guest', 'write', 'space', 'test')

for i=1, box.cfg.rows_per_wal do test:insert{i, 'test'} end
c = require('net.box').connect(box.cfg.listen)

-- try to write xlog without permission to write to disk
errinj.set('ERRINJ_WAL_WRITE', true)
c.space.test:insert({box.cfg.rows_per_wal + 1,1,2,3})
errinj.set('ERRINJ_WAL_WRITE', false)

-- Cleanup
test:drop()
errinj = nil

--
-- gh-3878: Panic when checkpoint_interval is reconfigured while
-- the checkpoint daemon is making a checkpoint.
--
fio = require('fio')
box.space._schema:delete('test') -- bump LSN
box.error.injection.set('ERRINJ_SNAP_WRITE_ROW_TIMEOUT', 0.1)
default_checkpoint_interval = box.cfg.checkpoint_interval
box.cfg{checkpoint_interval = 0.01}
-- Wait for the checkpoint daemon to start making a checkpoint.
test_run:cmd("setopt delimiter ';'")
test_run:wait_cond(function()
    local filename = string.format('%020d.snap.inprogress', box.info.signature)
    return fio.path.exists(fio.pathjoin(box.cfg.memtx_dir, filename))
end, 10);
test_run:cmd("setopt delimiter ''");
box.cfg{checkpoint_interval = 0.1}
box.cfg{checkpoint_interval = 1}
box.cfg{checkpoint_interval = 10}
box.cfg{checkpoint_interval = default_checkpoint_interval}
box.error.injection.set('ERRINJ_SNAP_WRITE_ROW_TIMEOUT', 0)
-- Wait for the checkpoint daemon to finish making a checkpoint.
test_run:cmd("setopt delimiter ';'")
test_run:wait_cond(function()
    local filename = string.format('%020d.snap', box.info.signature)
    return fio.path.exists(fio.pathjoin(box.cfg.memtx_dir, filename))
end, 10);
test_run:cmd("setopt delimiter ''");
