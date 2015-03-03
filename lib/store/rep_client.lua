-- -*- mode: lua; tab-width: 2; indent-tabs-mode: 1; st-rulers: [70] -*-
-- vim: ts=4 sw=4 ft=lua noet
---------------------------------------------------------------------
-- @author Daniel Barney <daniel@pagodabox.com>
-- @copyright 2014, Pagoda Box, Inc.
-- @doc
--
-- @end
-- Created :   18 Feb 2015 by Daniel Barney <daniel@pagodabox.com>
---------------------------------------------------------------------

local Emitter = require('core').Emitter
local hrtime = require('uv').hrtime
local logger = require('../logger')
local Packet = require('../packet')
local JSON = require('json')
local net = require('net')
local lmmdb = require("../lmmdb")
local store = require("../store")
Env = lmmdb.Env
DB = lmmdb.DB
Txn = lmmdb.Txn
Cursor = lmmdb.Cursor

function write(client)
	local buffer = {}
	local sync = false
	return function(data,check)
		if not sync and check == true then
			buffer[#buffer + 1 ] = data
			if check then
				sync = true
				-- we need to go through everything and send it off
				for _idx,data in pairs(buffer) do
					local a,b,c,d = Packet:pack(data:len())
					client:write(a .. b .. c .. d .. data)
				end
			end
		else

			local a,b,c,d = Packet:pack(data:len())
			client:write(a .. b .. c .. d .. data)
		end
	end
end

function wrap(client)
	return {connection = connection
	,write = write(client)}
end

function parser(buffer)
	
	local operations = {}
	while #buffer > 4 do
		local length = Packet:integerify(buffer:sub(1,4))
		if length + 4 <= buffer:len() then
			local operation = buffer:sub(5,4 + length)
			buffer = buffer:sub(5 + length)
			length = Packet:integerify(buffer:sub(1,4))
			operations[#operations + 1] = operation
		else
			break
		end
	end
	return buffer,operations
end


function push_flush_logs(operation,client)
	logger:debug("client has commited",operation)
	client.events:emit(operation)
	return push_flush_logs
end

function push_sync(operation,client)
	local version = operation
	local txn,err = Env.txn_begin(client.env,nil,Txn.MDB_RDONLY)
	if err then
		logger:warning("unable to begin txn to clear log")
		return
	end

	logger:debug("sync transaction begun",version)

	local logs,err = DB.open(txn,"logs",DB.MDB_DUPSORT)
	if err then
		logger:warning("unable to open 'logs' DB",err)
		return
	end
	local cursor,err = Cursor.open(txn,logs)
	if err then
		logger:warning("unable to create cursor",err)
		return
	end

	logger:debug("log cursor open")

	local key,op = Cursor.get(cursor,version,Cursor.MDB_SET_KEY,"unsigned long*")
	
	-- logger:info("comparing last known logs",client.version,key,key[0],version)
	if not key or key[0] == 0 then
		logger:info("performing full sync")
		local objects,err = DB.open(txn,"objects",0)
		if err then
			logger:warning("unable to open 'objects' DB",err)
			return
		end

		local obj_cursor,err = Cursor.open(txn,objects)
		if err then
			logger:warning("unable to create cursor",err)
			return
		end
		local id,json,err = Cursor.get(obj_cursor,version,Cursor.MDB_FIRST)
		while json do

			client.write(json)
			id,json,err = Cursor.get(obj_cursor,id,Cursor.MDB_NEXT)
		end	
		Cursor.close(obj_cursor)
		client.write("")
	else
		logger:info("performing partial sync")
		client.write("")
		while key do
			key,op,err = Cursor.get(cursor,nil,Cursor.MDB_NEXT)
			if op then
				logger:debug("syncing",op)
				client.write(op)
			end
		end
	end
	logger:info('sync is complete')
	Cursor.close(cursor)
	Txn.abort(txn)
end

function push_find_common(operation,client)
	local version = tonumber(operation)
	logger:debug("trying to find a common point",version)
	push_sync(version,client)
	-- something like this...
	client.write("",true)
	return push_flush_logs
end

function push_port(operation,client)
	logger:debug("starting a connection back",client.remote_ip,tonumber(operation))
	store:begin_sync(client.remote_ip,tonumber(operation))
	return push_find_common
end

function push_ip(operation,client)
	client.remote_ip = operation
	return push_port
end

function push_identify(operation,client,connections)
	logger:info("push identify",operation)
	if connections[operation] then
		logger:warning("client reconnected",operation)
	end
	connections[operation] = client
	client.id = operation
	return push_ip
end

function push_init(connections,client,id,env)
	logger:debug("push connected")
	client = wrap(client)
	state = push_identify
	local buffer = ""
	local chunk
	client.local_id = id
	client.env = env
	client.events = Emitter:new()

	client.write(id)
	chunk = coroutine.yield()
	while chunk do
		buffer = buffer .. chunk
		buffer,operations = parser(buffer)
		for _,operation in pairs(operations) do
			state = state(operation,client,connections)
		end
		chunk = coroutine.yield()
	end
end




function pull_replicate(operation,client)
	logger:debug("pull got a replicate",operation)
	-- so there is a bug that causes an empty operation to come across
	-- the first time that the connection is in this state.
	if operation:len() == 0 then
		return pull_replicate
	end
	operation = JSON.parse(operation)
	local txn = Env.txn_begin(client.env,nil,0)
	local replication,err = DB.open(txn,"replication",0)
	if err then
		Txn.abort(txn)
		logger:warning("unable to store replicated data",err)
		return replicate
	end
	local event = operation.data

	err = Txn.put(txn,replication,client.remote_id,event.last_updated,0)
	logger:debug("pulled",client.remote_id,event.id,event.last_updated)
	if operation.action == "store" then
		store:_store(event.bucket,event.id,event,true,false,txn)
	elseif operation.action == "delete" then
		store:_delete(event.bucket,event.id,true,false,txn)
	end
	local err = Txn.commit(txn)
	if err then
		logger:warning("unable to store replicated data",err)
		return
	end

	-- replicate the change out.
	store:emit(event.b_id,operation.action,event.id,event)
	-- respond to the push that the change was commited
	client.write(tostring(event.last_updated))

	return pull_replicate
end

function pull_sync(operation,client)
	if operation == "" then
		logger:debug("now all data needs to be refreshed on this node",client.remote_id,client.last_updated)

		err = Txn.put(client.txn,client.replication,client.remote_id,client.last_updated,0)
		if err then
			logger:warning("unable to sync up with remote",err)
			return
		end
		logger:info("last sync point",client.last_updated,client.remote_id)
		err = Txn.commit(client.txn)
		logger:info("database update finished in ",(hrtime() - client.sync_start)/1000000000)
		client.txn = nil
		if err then
			logger:warning("unable to sync up with remote",err)
			return
		end
		if client.cb then
			client.cb()
		end
		store:emit("refresh")
		return pull_replicate
	else
		if not client.sync_start then
			client.sync_start = hrtime()
		end
		local event = JSON.parse(operation)
		if event.last_updated > client.last_updated then
			client.last_updated = event.last_updated
		end
		local ret,err = store:_store(event.bucket,event.id,event,true,false,client.txn)
		logger:debug(event.bucket,event.id,err)
		return pull_sync
	end
end

function pull_identify(operation,client)
	logger:debug("storing remote id",operation)
	client.remote_id = operation
	local replication,err = DB.open(client.txn,"replication",0)
	if err then
		Txn.abort(client.txn)
		logger:warning("unable to open replication DB",err)
		return
	end
	client.replication = replication
	local last_updated,err = Txn.get(client.txn,replication,operation,"unsigned long*")
	if last_updated then
		logger:info("found last sync point",tonumber(last_updated[0]),operation)
		client.write(tostring(tonumber(last_updated[0])))
	else
		logger:info("first sync",operation)
		client.write("0")
	end
	return pull_sync
end

function pull_init(env,id,ip,port,client,cb)
	logger:debug("pull is connected!")
	client = wrap(client)
	client.env = env
	local txn,err = Env.txn_begin(env,nil,0)
	client.txn = txn
	client.cb = cb
	client.last_updated = 0
	logger:debug("going to send",id,ip,port)
	client.write(id)
	client.write(ip)
	client.write(tostring(port))
	local state = pull_identify
	local buffer = ""

	chunk = coroutine.yield()
	while chunk do
		buffer = buffer .. chunk
		buffer,operations = parser(buffer)
		for _,operation in pairs(operations) do
			state = state(operation,client)
			if not state then
				return
			end
		end
		chunk = coroutine.yield()
	end
end

return {pull = pull_init,push = push_init}