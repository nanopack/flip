-- -*- mode: lua; tab-width: 2; indent-tabs-mode: 1; st-rulers: [70] -*-
-- vim: ts=4 sw=4 ft=lua noet
---------------------------------------------------------------------
-- @author Daniel Barney <daniel@pagodabox.com>
-- @copyright 2014, Pagoda Box, Inc.
-- @doc
--
-- @end
-- Created :   4 Feb 2015 by Daniel Barney <daniel@pagodabox.com>
---------------------------------------------------------------------
local logger = require('../logger')
local JSON = require('json')
local fs = require('fs')
local hrtime = require('uv').hrtime
local lmmdb = require('../lmmdb')
Env = lmmdb.Env
Txn = lmmdb.Txn
Cursor = lmmdb.Cursor
DB = lmmdb.DB

return function(Store)

	function Store:load_from_disk(cb)
		fs.stat(self.db_path,function(_,exists)
			local env,err = Env.create()
			p(Env.set_maxdbs(env,4))
			p(Env.set_mapsize(env,1024*1024*1024))
			p(Env.reader_check(env))
			if err then 
				logger:fatal('unable to create store',err)
				process:exit(1)
			end
			err = Env.open(env,self.db_path,Env.MDB_NOSUBDIR,0755)
			if err == 'Device busy' then
				fs.unlink(env,self.db_path .. '-lock')
				err = Env.open(env,self.db_path,Env.MDB_NOSUBDIR,0755)
			end
			if err then 
				logger:fatal('unable to open store',err)
				process:exit(1)
			end
			self.env = env
			local txn = Env.txn_begin(env,nil,0)
			DB.open(txn,"objects",DB.MDB_CREATE)
			DB.open(txn,"replication",DB.MDB_CREATE)
			local logs,err1 = DB.open(txn,"logs",DB.MDB_CREATE + DB.MDB_INTEGERKEY)
			local cursor = Cursor.open(txn,logs)
			local key,_op = Cursor.get(cursor,nil,Cursor.MDB_LAST,"unsigned long*")
			if key then
				logger:info("last operation commited",key[0])
				self.version = key[0]
			else
				logger:info("new database was opened")
				self.version = hrtime() * 100000
			end
			DB.open(txn,"buckets",DB.MDB_DUPSORT + DB.MDB_CREATE)
			Txn.commit(txn)
			cb(not exists,err)
		end)
	end

	function Store:open(cb)
		self:load_from_disk(function(need_bootstrap,err)
			if need_bootstrap then
				logger:info("bootstrapping store")

				
				-- we use the dev load command to load all the default
				-- systems into flip
				local load = require('../system-dev/cli/load')
				env = getfenv(load)

				-- we don't need any logs. the load command will always work
				env.logger = 
					{info = function() end
					,warning = function() end
					,error = function() end
					,debug = function() end}
				env.store = self
				env.require = require
				setfenv(load,env)
				self.loading = true
				load('lib/system-topology','topology')
				load('lib/system-store','store')
				load('lib/system-dev','dev')
				self.loading = false
				logger:info('loaded bootstrapped store')
			elseif err then
				logger:fatal("unable to open disk store",err)
				process:exit(1)
			else
				logger:info('store was loaded from disk')
			end
			self:start_replication_connection()
			cb()
		end)
	end
end