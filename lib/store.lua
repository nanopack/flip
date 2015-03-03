-- -*- mode: lua; tab-width: 2; indent-tabs-mode: 1; st-rulers: [70] -*-
-- vim: ts=4 sw=4 ft=lua noet
---------------------------------------------------------------------
-- @author Daniel Barney <daniel@pagodabox.com>
-- @copyright 2014, Pagoda Box, Inc.
-- @doc
--
-- @end
-- Created :   27 Jan 2015 by Daniel Barney <daniel@pagodabox.com>
---------------------------------------------------------------------

local Emitter = require('core').Emitter
local logger = require('./logger')
local JSON = require('json')
local timer = require('timer')
local table = require('table')
local hrtime = require('uv').hrtime
local lmmdb = require("./lmmdb")
Env = lmmdb.Env
DB = lmmdb.DB
Txn = lmmdb.Txn
Cursor = lmmdb.Cursor

local Store = Emitter:extend()

function Store:configure(path,id,flip,ip,port,api)
	require('./store/failover')(Store)
	require('./store/storage')(Store)
	self.api = api
	self.flip = flip
	self.id = id
	self.scripts = {}
	self.ip = ip
	self.port = port
	self.is_master = true
	self.connections = {}
	self.push_connections = {}
	self.master = {}
	self.master_replication = {}
	self.db_path = path
end

function Store:index(b_id,idx)
	local txn,err = Env.txn_begin(self.env,nil,Txn.MDB_RDONLY)
	if err then
		return nil,err
	end
	local buckets,err = DB.open(txn,"buckets",DB.MDB_DUPSORT)
	if err then
		logger:info("unable to open 'buckets' DB",err)
		return nil,err
	end
	local cursor,err = Cursor.open(txn,buckets)
	if err then
		logger:info("unable to create cursor",err)
		return nil,err
	end

	local key,check = Cursor.get(cursor,b_id,Cursor.MDB_SET_KEY)
	local ret
	local count = 1
	if idx then
		while key == b_id do
			if count == idx then
				ret = check
				break
			end
			count = count + 1
			key,check,err = Cursor.get(cursor,key,Cursor.MDB_NEXT_DUP)
		end
	else
		ret = {}
		while key == b_id do
			ret[#ret + 1] = check
			key,check,err = Cursor.get(cursor,key,Cursor.MDB_NEXT_DUP)
		end
	end

	Cursor.close(cursor)
	Txn.abort(txn)
	return ret
end

function Store:get_index(b_id,id)
	local txn,err = Env.txn_begin(self.env,nil,Txn.MDB_RDONLY)
	if err then
		return nil,err
	end
	local buckets,err = DB.open(txn,"buckets",DB.MDB_DUPSORT)
	if err then
		logger:info("unable to open 'buckets' DB",err)
		return nil,err
	end
	local cursor,err = Cursor.open(txn,buckets)
	if err then
		logger:info("unable to create cursor",err)
		return nil,err
	end

	local key,check = Cursor.get(cursor,b_id,Cursor.MDB_SET_KEY)
	local ret
	local count = 1
	while key == b_id do
		if check == id then
			ret = count
			break
		end
		count = count + 1
		key,check,err = Cursor.get(cursor,key,Cursor.MDB_NEXT_DUP)
	end

	Cursor.close(cursor)
	Txn.abort(txn)
	return ret
end

function Store:fetch(b_id,id,cb)
	-- this should be a read only transaction
	local txn,err = Env.txn_begin(self.env,nil,Txn.MDB_RDONLY)
	if err then
		return nil,err
	end

	local objects,err = DB.open(txn,"objects",0)
	if err then
		logger:info("unable to open 'objects' DB",err)
		Txn.abort(txn)
		return nil,err
	end


	if id then
		local json,err = Txn.get(txn,objects, b_id .. ":" .. id)
		Txn.abort(txn)
		if err then
			return nil,err
		else
			json = JSON.parse(json)
			json.script = self.scripts[b_id .. ":" .. id]
			if json["$script"] and not json.script then
				json.script = self:compile(json,b_id,id)
				self.scripts[b_id .. ":" .. id] = json.script
			end
			return json
		end
	else
		local buckets,err = DB.open(txn,"buckets",DB.MDB_DUPSORT)
		if err then
			logger:info("unable to open 'buckets' DB",err)
			return nil,err
		end
		local cursor,err = Cursor.open(txn,buckets)
		if err then
			logger:info("unable to create cursor",err)
			return nil,err
		end

		local key,id = Cursor.get(cursor,b_id,Cursor.MDB_SET_KEY)
		local acc
		if cb then
			while key == b_id do
				json,err = Txn.get(txn,objects, b_id .. ":" .. id)
				json = JSON.parse(json)
				json.script = self.scripts[b_id .. ":" .. id]
				if json["$script"] and not json.script then
					json.script = self:compile(json,b_id,id)
					self.scripts[b_id .. ":" .. id] = json.script
				end
				cb(key,json)
				key,id,err = Cursor.get(cursor,key,Cursor.MDB_NEXT_DUP)
			end
		else
			acc = {}
			while key == b_id do
				local json,err = Txn.get(txn,objects, b_id .. ":" .. id)
				json = JSON.parse(json)
				json.script = self.scripts[b_id .. ":" .. id]
				if json["$script"] and not json.script then
					json.script = self:compile(json,b_id,id)
					self.scripts[b_id .. ":" .. id] = json.script
				end
				acc[#acc + 1] = json
				key,id,err = Cursor.get(cursor,key,Cursor.MDB_NEXT_DUP)
			end
		end

		Cursor.close(cursor)
		Txn.abort(txn)
		return acc
	end

end

function Store:store(b_id,id,data,cb)
	if b_id == nil or id == nil or data == nil then
		return nil,"missing required args"
	elseif self.is_master then
		return self:_store(b_id,id,data,false,true,nil,cb)
	else
		return {master = {ip = self.master.ip, port = self.master.port}},"read only slave"
	end
end

function Store:delete(b_id,id,cb)
	if b_id == nil or id == nil then
		return nil,"missing required args"
	elseif self.is_master then
		return self:_delete(b_id,id,false,true,nil,cb)
	else
		return {master = {ip = self.master.ip, port = self.master.port}},"read only slave"
	end
end

function Store:start(parent)
	local txn,err = Env.txn_begin(self.env,parent,0)
	if err then
		return nil,nil,nil,nil,"txn: "..err
	end

	local objects,err = DB.open(txn,"objects",0)
	if err then
		Txn.abort(txn)
		return nil,nil,nil,nil,"objects: "..err
	end

	local buckets,err = DB.open(txn,"buckets",DB.MDB_DUPSORT)
	if err then
		Txn.abort(txn)
		return nil,nil,nil,nil,"buckets: "..err
	end

	local logs,err = DB.open(txn,"logs",DB.MDB_INTEGERKEY)
	if err then
		Txn.abort(txn)
		return nil,nil,nil,nil,"logs: "..err
	end

	return txn,objects,buckets,logs
end

function Store:_store(b_id,id,data,sync,broadcast,parent,cb)
	local key = b_id .. ":" .. id

	local txn,objects,buckets,logs,err = self:start(parent)
	if err then
		logger:warning("unable to store data",err)
		return nil,err
	end

	if sync then
		local obj,err = Txn.get(txn,objects,key)
		if obj then
			obj = JSON.parse(obj)
			if obj.last_updated >= data.last_updated then
				logger:debug("we have upto date data",b_id,id)
				Txn.abort(txn)
				return obj
			end
		else
			Txn.put(txn,buckets,b_id,id,Txn.MDB_NODUPDATA)
		end
	else
		local json,err = Txn.get(txn,objects,key)
		if err then
			err = Txn.put(txn,buckets,b_id,id,Txn.MDB_NODUPDATA)
			if err then
				logger:error("unable to add id to 'buckets' DB",err)
				return nil,err
			end
			if self.loading then
				data.created_at = 0
				data.last_updated = 0
			else
				data.created_at = hrtime() * 100000
				data.last_updated = data.created_at
			end
		else
			-- there has got to be a better way to do this.
			local obj = JSON.parse(json)
			-- we carry over the created_at
			data.created_at = obj.created_at
			data.last_updated = hrtime() * 100000
		end
		data.bucket = b_id
		data.id = id
	end


	local encoded = JSON.stringify(data)
	local err = Txn.put(txn,objects,key,encoded,0)
	if err then
		logger:error("unable to add value to 'objects' DB",key,err)
		Txn.abort(txn)
		return nil,err
	end

	local op
	if not sync then
		op = JSON.stringify({action = "store",data = data})
		local err = Txn.put(txn,logs,data.last_updated,op,0)

		if err then
			logger:error("unable to add to commit log",key,err)
			Txn.abort(txn)
			return nil,err
		end
	end

	-- commit all changes
	err = Txn.commit(txn)
	
	if err then
		logger:error("unable to commit transaction",err)
		return nil,err
	end

	-- compile any scripts and store them off.
	fn = self:compile(data,b_id,id)
	self.scripts[key] = fn

	-- send any updates off
	local updated = true
	if broadcast and updated then
		logger:debug("broadcasting",b_id .. ":",b_id .. ":" .. id,data)
		self:emit(b_id .. ":","store",id,data)
		self:emit(b_id .. ":" .. id,"store",id,data)
	end
	
	if not(sync) and not(self.loading) then
		self:replicate(op,data.last_updated,cb,#self.push_connections)
	end
	return data
end

function Store:_delete(b_id,id,sync,broadcast,parent,cb)
	local txn,objects,buckets,logs,err = self:start(parent)
	local key = b_id .. ":" .. id

	if err then
		logger:warning("unable to delete data",err)
		return nil,err
	end

	local json,err = Txn.get(txn,objects,key)
	-- there has got to be a better way to do this.
	logger:info(json,err)
	if not json then
		return 
	end
	local obj = JSON.parse(json)

	local err = Txn.del(txn,objects,key)
	if err then
		logger:error("unable to delete object",key,err)
		Txn.abort(txn)
		return nil,err
	end

	local err = Txn.del(txn,buckets,b_id,id)
	if err then
		logger:error("unable to delete object key",key,err)
		Txn.abort(txn)
		return nil,err
	end

	local op
	local op_timestamp
	if not sync then
		op = JSON.stringify({action = "delete",data = {bucket = b_id,id = id}})
		 op_timestamp = hrtime() * 100000
		local err = Txn.put(txn,logs,op_timestamp,op,0)

		if err then
			logger:error("unable to add to commit log",key,err)
			Txn.abort(txn)
			return nil,err
		end
	else
		if obj.last_updated > hrtime() * 100000 then
			logger:info("got an update since the delete was queued")
			Txn.abort(txn)
			return
		end
	end

	-- commit all changes
	err = Txn.commit(txn)
	
	if err then
		return nil,err
	end

	self.scripts[key] = nil

	if broadcast then
		self:emit(b_id .. ":","delete",id)
		self:emit(b_id .. ":" .. id,"delete",id)
	end
	
	if not sync then
		self:replicate(op,op_timestamp,cb)
	end
end

function  Store:replicate(operation,op_timestamp,cb)
	local total = 1
	logger:debug("starting to clean logs",op_timestamp)
	local current = 0
	local complete = function()
		logger:debug('remote reported that log was committed')
		current = current + 1
		if cb then
			cb(current,total,nil)
		end
		if current == total then
			logger:debug("all remotes have reported, now cleaning logs")
			local txn,err = Env.txn_begin(self.env,nil,0)
			if err then
				logger:error("unable to begin txn to clear log")
				return
			end
			local logs,err = DB.open(txn,"logs",DB.MDB_INTEGERKEY)
			if err then
				Txn.abort(txn)
				logger:error("unable to open logs DB for cleaning")
				return
			end
			Txn.del(txn,logs,op_timestamp)
			err = Txn.commit(txn)
			if err then
				logger:error("unable to open clean logs DB")
			end
		end
	end
	
	for id,connection in pairs(self.push_connections) do
		total = total + 1
		logger:debug("writing",tostring(op_timestamp),connection.id)
		connection.write(operation)
		connection.events:once(tostring(op_timestamp),complete)
	end

	timer.setTimeout(0,complete)
end

function Store:compile(data,bucket,id)
	local script = data["$script"]
	local env =
			{__filename = id
			,__dirname = bucket
			,pairs = pairs
			,pcall = pcall
			,xpcall = xpcall
			,table = table
			,store = self
			,system = self.flip.system
			,logger = logger
			,JSON = JSON
			,string = string
			,tonumber = tonumber
			,tostring = tostring
			,error_code = self.api.error_code
			,require = function() end} -- this needs to be fixed.
	local fn,err = self:build(data,script,env,bucket,id)
	if err then
		logger:error("script failed to compile",err)
		return nil,err
	elseif fn then
		return fn()
	end
end

function Store:build(data,script,env,bucket,id)
	if script then
		local fn,err = loadstring(script, '@store/bucket:' .. bucket .. '/script:' .. id)
		if err then
			return nil,err
		end
		setfenv(fn,env)
		return fn
	end
end

return Store:new()