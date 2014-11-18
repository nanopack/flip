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
local timer = require('timer')
local logger = require('../logger')

local Plan = Emitter:extend()

function Plan:initialize(system,id,type)
	self.system = system
	self.id = id
	self.plan = {}
	self.mature = false
	local me = self
	process:on('SIGINT',function() me:shutdown() end)
	process:on('SIGQUIT',function() me:shutdown() end)
	process:on('SIGTERM',function() me:shutdown() end)
end

function Plan:set_new(is_dead)
	local add = {}
	local idx = 0
	local count = #is_dead
	local alive_count = count
	local id = self.id
	
	for _idx,is_dead in pairs(is_dead) do
		if is_dead then
			if _idx < self.id then
				id = id - 1
			end
			alive_count = alive_count - 1
		end
	end
	logger:debug("check what is alive",alive_count,self.system.data)

	for i=1,#self.system.data do
		logger:debug("mod",(i % count) + 1,is_dead[(i % count) + 1],is_dead,self.id)
		if (i % count) + 1 == self.id then
			logger:debug("its me!",self.system.data[i])
			add[#add + 1] = self.system.data[i]
		elseif is_dead[(i % count) + 1] then
			logger:debug("not alive",self.system.data[i],idx,alive_count,self.id,id)
			-- if the other node is down, and we are responsible for it
			-- add it in
			if (idx % alive_count) + 1 == id then
				logger:debug("failing for",self.system.data[i])
				add[#add + 1] = self.system.data[i]
			end
			idx = idx + 1
		end
	end
	self.next_plan = add
	self.last_group = #is_dead
end


function Plan:activate(Timeout)
	if self.plan_activation_timer then
		logger:debug("clearing timer")
		timer.clearTimer(self.plan_activation_timer)
	end

	if self.queue then
		self.queue = {add = self.next_plan,remove = {}}
	else

		logger:debug("setting plan",self.next_plan)
		self.plan_activation_timer = timer.setTimeout(Timeout,function(new_plan)
			-- we order the array to make the comparison easier
			table.sort(new_plan)

			local add = {}
			local remove = {}
			local index = 1
			local lidx = 1
			logger:debug("start",idx,new_plan)

			for idx=1, #new_plan do
				
				--- value     1,3,4
				--- self.plan 1,2,3
				logger:debug("compare",self.plan[index],new_plan[idx])
				if (new_plan[idx] and not self.plan[index]) or (self.plan[index] > new_plan[idx]) then
					logger:debug("adding",new_plan[idx])
					add[#add +1] = new_plan[idx]
				elseif (self.plan[idx] and not new_plan[index]) or (self.plan[index] < new_plan[idx]) then
					logger:debug("removing",new_plan[idx])
					remove[#remove +1] = self.plan[index]
					index = index + 1
					idx = idx - 1
				else
					logger:debug("skipping",new_plan[idx])
					idx = idx + 1
					index = index + 1
				end
				lidx = idx
			end
			lidx = lidx + 1

			-- everything else gets removed
			for index=index,#self.plan do
				logger:debug("batch removing",self.plan[index])
				remove[#remove +1] = self.plan[index]
			end

			-- everything else gets added
			for idx=lidx,#new_plan do
				logger:debug("batch adding",new_plan[idx])
				add[#add +1] = new_plan[idx]
			end

			-- this is not really working yet, on a restart I need to remove
			-- any data points that I am responsibe for

			if not self.mature then
				logger:info("not mature yet",#self.system.data,self.last_group)
				for i=1,#self.system.data do
					if not ((i % self.last_group) + 1 == self.id) then
						remove[#remove +1] = self.system.data[i]
					end
				end
			end

			-- if there were changes
			if (#add > 0) or (#remove > 0) then
				self.queue = {add = add,remove = remove}
				self:run()
			else
				logger:info("no change in the plan",self.plan)
			end
		end,self.next_plan)
	end
end

function Plan:shutdown()
	self:_run('down',self.plan,function()
		process.exit(1)
	end)
end

function Plan:run()

	local newest_plan = {}

	for _idx,current in pairs(self.plan) do
		local skip = false
		for _idx,value in pairs(self.queue.remove) do
			if value == current then
				skip = true
				break
			end
		end
		if not skip then
			newest_plan[#newest_plan + 1] = current
		end
	end


	
	for _idx,value in pairs(self.queue.add) do
		newest_plan[#newest_plan + 1] = value
	end

	self.plan = newest_plan
	
	local queue = self.queue
	self.queue = true

	self:_run("alive",queue.add,function()
		self:_run("down",queue.remove,function()
			self.mature = true
			-- if there is another thing queued up to run,
			-- lets run it
			if self.queue == true then
				-- if not, lets end
				self.queue = nil
				logger:info("new plan",self.plan)
			else
				logger:info("running next set of jobs",self.queue)
				self:run()
			end
		end)
	end)

end

function Plan:_run(state,data,cb)
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

return Plan