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
local Packet = require('../packet')
local JSON = require('json')
local http = require('http')
local net = require('net')
local timer = require('timer')
local table = require('table')
local lmmdb = require("../lmmdb")


Env = lmmdb.Env
DB = lmmdb.DB
Txn = lmmdb.Txn
Cursor = lmmdb.Cursor


return function(Store)
	local Init = require("./rep_client")
	function Store:start_replication_connection()
		net.createServer(function (client)
			logger:info("client connected")
			local state_machine = coroutine.create(Init.push)
			client:on('data',function(data)
				local worked,err = coroutine.resume(state_machine,data)
					if coroutine.status(state_machine) == "dead" then
						if not worked then
							logger:error(err)
						end
					client:destroy()
				end
			end)
			client:on('end',function()
				coroutine.resume(state_machine,false)
			end)
			coroutine.resume(state_machine,self.push_connections,client,self.id,self.env)
		end):listen(self.port,self.ip)

		logger:info("tcp replication socket is open")
	end



	function Store:cancel_sync(ip,port)
		local sync = self.connections[ip .. ":" .. port]
		if sync then
			if sync.timer then
				timer.clearTimer(sync.timer)
			end
			if sync.connection then
				-- this isn't quite right
				sync.connection:close()
			end
			self.connections[ip .. ":" .. port] = nil
		end
	end

	function Store:begin_sync(ip,port,cb)
		local key = ip .. ":" .. port

		if self.connections[key] and self.connections[key].connection then
			logger:info("already syncing with remote",ip,port)
			if cb then
				cb()
			end
		else
			if self.connections[key] and  self.connections[key].timer then
				timer.clearTimer(self.connections[key].timer)
			end
			

			-- create a connection
			local client
			client = net.createConnection(port, ip, function (err)
				if err then
					self.connections[key].timer = timer.setTimeout(5000,function() self:begin_sync(ip,port,cb) end)
					self.connections[key].connection = nil
					return
				end
				logger:info("connected to remote",ip,port)
				local state_machine = coroutine.create(Init.pull)
				client:on('data',function(data)
					local worked,err = coroutine.resume(state_machine,data)
					if coroutine.status(state_machine) == "dead" then
						if not worked then
							logger:error(err)
						end
						client:destroy()
					end
				end)
				client:once('end',function()
					coroutine.resume(state_machine,false)
					self.connections[key].timer = timer.setTimeout(5000,function() self:begin_sync(ip,port,cb) end)
					self.connections[key].connection = nil
				end)
				client:once('error',function(err)
					coroutine.resume(state_machine,false)
				end)
				coroutine.resume(state_machine,self.env,self.id,self.ip,self.port,client,cb)
			end)

			client:once('error',function(err)
				logger:info("we errored out",err)
				self.connections[key].timer = timer.setTimeout(5000,function() self:begin_sync(ip,port,cb) end)
				self.connections[key].connection = nil
			end)

			self.connections[key] = {connection = client}
		end
	end
end