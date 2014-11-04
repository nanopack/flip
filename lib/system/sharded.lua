-- -*- mode: lua; tab-width: 2; indent-tabs-mode: 1; st-rulers: [70] -*-
-- vim: ts=4 sw=4 ft=lua noet
---------------------------------------------------------------------
-- @author Daniel Barney <daniel@pagodabox.com>
-- @copyright 2014, Pagoda Box, Inc.
-- @doc
--
-- @end
-- Created :   4 Nov 2014 by Daniel Barney <daniel@pagodabox.com>
---------------------------------------------------------------------

local Emitter = require('core').Emitter
local JSON = require('json')
local spawn = require('childprocess').spawn
local logger = require('../logger')

local Replicate = Emitter:extend()

function Replicate:initialize(system)
	self.system = system
	self.failed = {}
	if not system.members then
		system.members = {}
	end
	system.members[#system.members + 1] = self
	self.id = #system.members
end

function Replicate:data_points_for(data,down_id)
	local chunk = {}
	local incr = #self.system.members
	local i = self.id
	if down_id then
		incr = incr - 1
		if down_id < i then
			i = i - 1
		end
	end
	logger:debug("splitting up",i,data,incr)
	for i = i, #data, incr do
		chunk[#chunk + 1] = data[i]
	end
	return chunk
end

function Replicate:enable(member)
	self.data = self:data_points_for(self.system.data)
	local me = self
	if member.id == member.config.id then
		self.system.master = self
		member:once('state_change',function(...) me:handle_this_shard(...) end)
		for idx,other_member in pairs(self.system.members) do
			self.failed[other_member.id] = false
			other_member:on('failover',function(...) me:handle_failover(...) end)
		end
	else
		member:on('state_change',function(...) me:handle_change(...) end)
	end
	logger:info("node is responsible for",member.id,self.data)
end

function Replicate:handle_change(member,new_state)
	if new_state == 'down' or new_state == 'alive' then
		local cb = function()
			self:emit('failover',self,new_state)
		end

		-- this is wrong
		local master = self.system.master

		if new_state == 'down' then
			self:run('alive',master:data_points_for(self.data,self.id),cb)
		else
			self:run('down',master:data_points_for(self.data,self.id),cb)
		end
	end
end

function Replicate:handle_failover(member,new_state)
	if new_state == 'alive' and self.failed[member.id] then
		self.failed[member.id] = false
	elseif new_state == 'down' and not self.failed[member.id] then
		self.failed[member.id] = true
	else
		logger:warning("inconsistant state",self.id,new_state,self.failed[member.id])
		return
	end
	
	local master = self.system.master

	for id,down in pairs(self.failed) do
		if down and not (member.id == id) then
			local data = master:data_points_for(member.data,id)
			self:run('down',data,function() 
				logger:info("failover complete")
			end)
		end
	end
end

function Replicate:handle_this_shard(member,new_state)
	if new_state == 'alive' then
		self:run('alive',self.data,function() logger:info("this node is fully online") end)
	end
end


function Replicate:run(state,data,cb)
	local sys = self.system
	local count = #data
	for idx,value in pairs(data) do
		local child = spawn(sys[state],{value,JSON.stringify(sys.config)})
		
		child.stdout:on('data', function(chunk)
			logger:debug("got",chunk)
		end)

		child:on('exit', function(code,other)
			count = count - 1
			if not (code == 0 ) then
				logger:error("script failed",sys[state],{value,JSON.stringify(sys.config)})
			else
				logger:info("script worked",sys[state],{value,JSON.stringify(sys.config)})
			end
			if count == 0 and cb then
				cb()
			end
		end)

	end
	if count == 0 then
		process.nextTick(cb)
	end
end

return Replicate