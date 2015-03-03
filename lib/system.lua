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

local Plan = require('./plan')


local Emitter = require('core').Emitter
local logger = require('./logger')
local JSON = require('json')

local System = Emitter:extend()
function System:initialize(store,flip)
	self.flip = flip
	self.store = store
	self.plans = {}
	self.enabled = false
end

function System:disable(cb)
	if self.enabled then
		self.enabled = false
		count = 0
		for _idx,plan in pairs(self.plans) do
			count = count + 1
			self:stop_system(plan.system)
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
			if cb then
				cb()
			end
		end
	else
		logger:warning('requested to disable system, but already disabled')
	end
end

function System:check_system(kind,id,system_config) 
	if kind == "store" then
		local plan = self.plans[id]
		if plan then
			plan:update(system_config)
			logger:info("updated system:",id)
		else
			self:init_system(system_config)
			self.plans[id] = Plan:new(system_config,id,self.flip,self.store)
			plan:on('change',function(id,...) 
				self:emit('change:',id,...) 
				self:emit('change:' .. id,id,...) 
			end)
			logger:info("created system:",id)
		end
	elseif kind == "delete" then
		local plan = self.plans[id]
		if plan then
			self:stop_system(system_config)
			self.plans[id] = nil
			plan:disable(function() 
				logger:info("removed system:",id)
			end)
		end
	end
end

function System:regen(systems)
	if systems then
		logger:info("begining regeneration of",systems)
		for _idx,system in pairs(systems) do
			local plan = self.plans[system]
			if plan then
				plan:next_plan()
			end
		end
	end
end

function System:status(id)
	if id then
		local plan = self.plans[id]
		if plan then
			return plan:status()
		end
	else
		local status = {}
		for id,plan in pairs(self.plans) do
			status[id] = plan:status()
		end
		return status
	end
end

function System:enable()
	if not self.enabled then
		self.enabled = true
		local systems,err = self.store:fetch("systems")
		if err then
			logger:info("no systems present in cluster",err)
			systems = {}
		end

		for _idx,system_config in pairs(systems) do
			sys_id = system_config.id
			self:init_system(system_config)

			local plan = Plan:new(system_config,sys_id,self.flip,self.store)
			plan:on('change',function(id,...) 
				self:emit('change:',id,...) 
				self:emit('change:' .. id,id,...) 
			end)
			self.plans[sys_id] = plan

			-- this should only be enabled if I am a member of the system
			plan:enable()
		end

		self.store:on("systems",function(kind,id,system_config) self:check_system(kind,id,system_config) end)
		self.store:on("refresh",function() logger:info("we need to refresh all systems") end)

		-- when this process shutsdown, we want to remove all data
		-- that it is responsible for, but only if requested
		if true then
			local me = self
			local stop = function() me:disable(function() process:exit(0) end) end
			process:on('sigint',stop)
			process:on('sigquit',stop)
			process:on('sigterm',stop)
		end

		-- we don't do this, this happens when this server gets added into the store
		-- -- we enable all the plans to start the ball rolling
		-- for _idx,plan in pairs(self.plans) do
		-- 	plan:enable()
		-- end
	else
		logger:warning('requested to enable system, but already enabled')
	end
end

function System:init_system(system)
	if system.init then
		local script,err = self.store:fetch(system.id .. '-scripts',system.init)
		if script and script.script then
			logger:info("running",script)
			-- not sure what the init scripts are for yet
			script.script()
		end
		local build = function(key)
			return function(req,res)
				local script,err = self.store:fetch(system.id .. '-scripts',key)
				if err then
					local code = self.store.api:error_code(err)
					res:writeHead(code,{})
					res:finish(JSON.stringify({error = err}))
				elseif script and script.script then
					script.script(req,res)
				else
					res:writeHead(404,{})
					res:finish(JSON.stringify({error = "not code"}))
				end
			end
		end
		-- and we need to add in routes that are needed
		if system.endpoints then
			for method,routes in pairs(system.endpoints) do
				for route,key in pairs(routes) do
					-- this isn't safe to do but is ok for now.
					logger:info("adding route",method,route,key)
					self.flip.api.lever[method](self.flip.api.lever,route,build(key))
				end
			end
		end
	end
end

function System:stop_system(system)
	-- this should run the shutdown script, and remove all routes
	logger:info("stopping system",system.id)
end

return System