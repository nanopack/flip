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
local Plan = require('./plan')

local Sharded = Emitter:extend()

function Sharded:initialize(system)
	self.system = system
	self.failed = {}
	if not system.members then
		system.members = {}
	end
	system.members[#system.members + 1] = self
	self.id = #system.members
end

function Sharded:enable(member)
	local me = self
	if member.id == member.config.id then
		self.system.master = self

		member:once('state_change',function(a_member,state,...)
			me:handle_change(me,state) 
		end)

		self:on('state_change',function(a_member,state,sys_mem)
			me:handle_change(sys_mem,state,sys_mem)
		end)

		-- we want the main node to track other nodes
		for idx,other_member in pairs(self.system.members) do
			self.failed[other_member.id] = false
		end
		self.failed[me.id] = false

		self.plan = Plan:new(self.system,self.id)
	else
		member:on('state_change',function(a_member,state) 
			me.system.master:emit('state_change',a_member,state,me)
		end)
	end
end

function Sharded:handle_change(member,new_state)
	if new_state == 'alive' then
		self.failed[member.id] = false
	elseif new_state == 'down' and not self.failed[member.id] then
		self.failed[member.id] = true
	else
		if not(new_state == 'probably_down') then
			logger:warning("inconsistant state",member.id,new_state,self.failed[member.id])
		end
		return
	end
	logger:debug("got state",self,member,new_state)
	self.plan:set_new(self.failed)
	self.plan:activate(500)
end

return Sharded