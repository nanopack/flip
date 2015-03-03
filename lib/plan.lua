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
local logger = require('./logger')

local Plan = Emitter:extend()

local table_sort = function(a,b)
	if type(a) == 'table' and type(b) == 'table' then
		return a.id > b.id
	else
		return a > b
	end
end

function Plan:initialize(system,sys_name,flip,store)
	self.store = store
	self.sys_name = sys_name
	self.flip = flip
	self.enabled = false
	self.plan = {}
	self.members = {}
	self.member_map = {}
	self.alive = {}
	
	-- we expose the ability to remove all data on start
	-- it defaults to removing the data
	self.mature = system.remove_on_start
	if self.mature == nil then
		self.mature = false
	end
	self:update(system)
end

function Plan:status()
	logger:info("getting status for plan",self.sys_name)
	return
		{data = self.plan
		,stable = (self.queue == nil) and (self.plan_activation_timer == nil)}
end

function Plan:disable(cb)
	self.enabled = false
	self:_run('down',self.plan,function()
		cb()
	end)
end

function Plan:enable()
	logger:info("enabling plan for",self.sys_name)
	self.enabled = true
	self:next_plan()
end


function Plan:update(system)
	local topology,err = self.store:fetch("topology-scripts",system.type)
	if err then
		logger:fatal("unknown data distribution type","topology-scripts",system.type,err)
		process:exit(1)
	end

	-- In future topologies, I might need to prepare the data before I 
	-- have the topology divide it between then nodes. right now I do
	-- nothing
	self.system = system
	
	self.new_plan_delay = system.delay
	if not self.new_plan_delay then
		self.new_plan_delay = 500
	end

	self:next_plan()
end

function Plan:fetch_members()
	local servers,err = self.store:fetch("servers")
	if err and not (err == "old data") then
		return nil,nil,err
	end
	local members = {}
	for _idx,server in pairs(servers) do
		members[#members + 1] = false
		local systems = server.systems
		if systems then
			for _idx,sys_name in pairs(systems) do
				if sys_name == self.sys_name then
					local member = self.flip:find_member(server.id)
					if member then
						members[#members] = not(member.state == 'down')
						break
					end
				end
			end
		end
	end
	local object,err = self.flip.store:fetch("servers",self.flip.config.id)
	logger:debug("found members of system",members,object,err)
	if err and not (err == "old data") then
		return nil,nil,err
	else
		return members,object.idx
	end
end

function Plan:next_plan()
	if self.enabled then
		local topology,err = self.store:fetch("topology-scripts",self.system.type)
		if err and not (err == "old data") then
			logger:warning("topology was not found",self.system.type)
		else
			if self.system.data then
				local data = self.system.data
				if type(data) == "string" then
					-- we grab the data from a bucket in the store
					data = self.store:fetch(data,nil)
					logger:debug("fetched data for plan",data)
				end

				local alive,idx,err = self:fetch_members()
				if err and not (err == "old data") then
					logger:warning("unable to start plan",err)
					return 
				end
			
				-- we ask the topology what data points are needed
				local add,remove = topology.script(data,idx,alive)
				logger:debug("toplogy returned",self.system.type,add,remove)
				local new_plan = {add = add,remove = remove}

				-- a plan was made and is still waiting to be run, we have a new one
				-- so lets clear it out
				if self.plan_activation_timer then
					logger:debug("clearing timer")

					timer.clearTimer(self.plan_activation_timer)
				end

				if self.queue then
					logger:debug("queueing change")
					-- we are currently running scripts, lets wait for them to be done
					self.queue = new_plan
				else

					-- we delay how long it takes for a new plan to be put in place
					-- so that if any members change at the exact smae time, the 
					-- changes get pulled into a single plan update
					logger:debug("setting plan",new_plan)
					local me = self
					self.plan_activation_timer = timer.setTimeout(self.new_plan_delay,function(a_plan)
						me.plan_activation_timer = nil
						me:compute(a_plan)
					end,new_plan)
				end
			else
				logger:info("no data attached to system")
			end
		end
	end
end

function Plan:compute(new_plan)
	
	
	-- we order the array to make the comparison easier
	local new_add = new_plan.add
	table.sort(new_add,table_sort)

	local add = {}
	local remove = {}
	local index = 1
	local lidx = 1
	logger:debug("start",idx,new_add,self.plan)

	while lidx <= #new_add do
		
		logger:debug("compare",lidx,self.plan[index],new_add[lidx])
		-- new_add = { "192.168.0.1", "192.168.0.2", "192.168.0.3", "192.168.0.4" }	
		-- self.plan = { "192.168.0.2", "192.168.0.4" }
		
		-- add .1
		-- skip .2
		-- add .3
		-- skip .4

		local is_object = type(self.system.data)

		if (not new_add[lidx]) or (not self.plan[index]) then
			-- if we run out of data, then we don't need to compare anymore
			break
		elseif (is_object and (self.plan[index].id > new_add[lidx].id)) or (not(is_object) and (self.plan[index] > new_add[lidx])) then
			-- we need to add data points that are members of new_add and 
			-- not members of self.plan
			logger:debug("adding",new_add[lidx])
			add[#add +1] = new_add[lidx]
			lidx = lidx + 1
		elseif (is_object and (self.plan[index].id < new_add[lidx].id)) or (not(is_object) and (self.plan[index] < new_add[lidx])) then
			-- we need to remove data points that are members of self.plan and 
			-- not members of new_add
			logger:debug("removing",new_add[lidx])
			remove[#remove +1] = self.plan[index]
			index = index + 1
		else
			-- everything else gets ignored.
			logger:debug("skipping",new_add[lidx])
			lidx = lidx + 1
			index = index + 1
		end
	end

	-- everything leftover else gets removed
	for index=index,#self.plan do
		logger:debug("batch removing",self.plan[index])
		remove[#remove +1] = self.plan[index]
	end

	-- everything leftover else gets added
	for idx=lidx,#new_add do
		logger:debug("batch adding",new_add[idx])
		add[#add +1] = new_add[idx]
	end

	-- on a startup, there could possibly be data that needs to be 
	-- removed
	if not self.mature then
		logger:info("not mature yet, removing",new_plan.remove)
		for _idx,value in pairs(new_plan.remove) do
			remove[#remove +1] = value
		end
	end

	-- if there were changes
	if (#add > 0) or (#remove > 0) then
		logger:info("changes in the plan",self.plan)
		self.queue = {add = add,remove = remove}
		self:run()
	else
		logger:info("no change in the plan",self.plan)
	end
end

function Plan:run()

	local newest_plan = {}

	-- we want to add in all data points that haven't been flagged
	-- to be removed
	logger:debug("examining",self.plan,self.queue)
	for _idx,current in pairs(self.plan) do
		local skip = false
		for _idx,value in pairs(self.queue.remove) do
			if value == current then
				skip = true
				break
			end
		end
		if not skip then
			logger:debug("not skipping",current)
			newest_plan[#newest_plan + 1] = current
		else
			logger:debug("skipping",current)
		end
	end


	-- add in all new data points
	for _idx,value in pairs(self.queue.add) do
		logger:debug("adding",value)
		newest_plan[#newest_plan + 1] = value
	end

	self.plan = newest_plan
	table.sort(self.plan,table_sort)
	
	local queue = self.queue
	self.queue = true

	-- run the alive scripts
	self:_run("alive",queue.add,function()
		-- run the down scripts
		self:_run("down",queue.remove,function()
			self.mature = true
			self:emit('change',self.sys_name,queue.add,queue.remove)
			
			if self.queue == true then
				-- if not, lets end
				self.queue = nil
				logger:info("finalized plan for '" .. self.sys_name .. "' is",self.plan)
			elseif not(self.queue == nil) then
				-- if there is another thing queued up to run,
				-- lets run it
				logger:debug("running next set of jobs",self.queue.add)
				local queue = self.queue
				self.queue = nil
				self:compute(queue)
			end
		end)
	end)

end

function Plan:_run(state,data,cb)
	local sys = self.system
	local count = #data

	if count == 0 then
		process.nextTick(cb)
		return
	end

	local cmd = sys[state]
	if cmd then
		if type(cmd) == "function" then
			cmd(value,sys.config,cb)

		elseif cmd:sub(1,1) == '$' then
			for idx,value in pairs(data) do
				
	 			local child = spawn(cmd:sub(1,cmd:len()),{value,JSON.stringify(sys.config)})
				
				child.stdout:on('data', function(chunk)
					logger:debug("got",chunk)
				end)

				child:on('exit', function(code,other)
					count = count - 1
					if not (code == 0 ) then
						logger:error("script failed",cmd,{value,JSON.stringify(sys.config)})
					else
						logger:info("script worked",cmd,{value,JSON.stringify(sys.config)})
					end
					if count == 0 and cb then
						cb()
					end
				end)
			end
		else
			local script,err = self.store:fetch(self.sys_name .. '-scripts',cmd)
			if err and not (err == "old data") then
				logger:warning("unable to find script in store",self.sys_name .. '-scripts',cmd)
				cb()
			else
				logger:info("running ",self.sys_name,cmd)
				script.script(data,cb)
			end
		end
		else
			cb()
		end
end

return Plan