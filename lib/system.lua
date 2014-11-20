-- -*- mode: lua; tab-width: 2; indent-tabs-mode: 1; st-rulers: [70] -*-
-- vim: ts=4 sw=4 ft=lua noet
---------------------------------------------------------------------
-- @author Daniel Barney <daniel@pagodabox.com>
-- @copyright 2014, Pagoda Box, Inc.
-- @doc
--
-- @end
-- Created :   18 Nov 2014 by Daniel Barney <daniel@pagodabox.com>
---------------------------------------------------------------------

local Plan = require('./plan/plan')


local Emitter = require('core').Emitter
local logger = require('./logger')

local System = Emitter:extend()
local id = 0
function System:initialize(config,node_id)
	self.config = config
	self.members = {}
	self.indexed_members = {}
	self.plans = {}
	self.id = node_id
	self._id = id
	id = id + 1
	self.enabled = false

	-- set the default remove_on_failure behavior
	if config.remove_on_failure == nil then
		config.remove_on_failure = true
	end
end

function System:add_member(member)
	self.members[#self.members + 1] = member.id
	self.indexed_members[member.id] = member
end

function System:disable(cb)
	if self.enabled then
		self.enabled = false
		count = 0
		for _idx,plan in pairs(self.plans) do
			count = count + 1
			plan:disable(function()
				count = count - 1
				if count == 0 then
					if cb then
						cb()
					end
				end
			end)
		end
		if count == 0 then
			process.exit(0)
		end
	else
		logger:warning('requested to disable system, but already disabled')
	end
end

function System:enable()
	if not self.enabled then
		self.enabled = true
		-- we create all plans that need to exist on this local node
		local this_node = self.indexed_members[self.id]
		for _idx,sys_id in pairs(this_node.systems) do
			local sys_config = self.config.system[sys_id]
			if sys_config then
				self.plans[sys_id] = Plan:new(sys_config,self.id)
			else
				logger:fatal('system does not exist in the cluster',sys_id)
				process.exit(1)
			end
		end

		-- all members need to be added into the plans that exists on
		-- this node
		for _idx,id in pairs(self.members) do
			local member = self.indexed_members[id]
			for _idx,sys_id in pairs(member.systems) do
				local plan = self.plans[sys_id]
				if plan then
					plan:add_member(member)
				end
			end
		end

		-- when this process shutsdown, we want to remove all data
		-- that it is responsible for, but only if requested
		if self.config.remove_on_failure then
			local me = self
			local stop = function() me:disable(function() process.exit(0) end) end
			process:on('SIGINT',stop)
			process:on('SIGQUIT',stop)
			process:on('SIGTERM',stop)
		end

		-- we enable all the plans to start the ball rolling
		for _idx,plan in pairs(self.plans) do
			plan:enable()
		end
	else
		logger:warning('requested to enable system, but already enabled')
	end
end

return System