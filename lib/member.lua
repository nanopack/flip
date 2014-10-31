-- -*- mode: lua; tab-width: 2; indent-tabs-mode: 1; st-rulers: [70] -*-
-- vim: ts=4 sw=4 ft=lua noet
---------------------------------------------------------------------
-- @author Daniel Barney <daniel@pagodabox.com>
-- @copyright 2014, Pagoda Box, Inc.
-- @doc
--
-- @end
-- Created :   23 Oct 2014 by Daniel Barney <daniel@pagodabox.com>
---------------------------------------------------------------------

local Emitter = require('core').Emitter
local hrtime = require('uv').Process.hrtime
local spawn = require('childprocess').spawn
local JSON = require('json')
local os = require('os')
local timer = require('timer')
local table = require('table')
local logger = require('./logger')

local Member = Emitter:extend()

function Member:initialize(config,global)
	self.config = global
	self.state = 'new'
	self.last_check = hrtime()
	self.last_send = hrtime()
	self.seq = -1
	self.packet_seq = 0
	self.ip = config.ip
	self.port = config.port
	self.id = config.id
	self.arbiter = config.arbiter
	logger:debug('created member',config)
	self.probed = {}
	self.systems = {}
	
	if not config.systems then
		config.systems = {}
	end

	local node_keys = {}
	for _idx,key in pairs(global.servers[self.id].systems) do
		node_keys[key] = true
	end

	for _ids,key in pairs(config.systems) do
		if node_keys[key] then
			local group = self.config.cluster.system[key]
			if not group.member_count then
				group.member_count = 0
			end
			group.member_count = group.member_count + 1
			self.systems[key] = {id = group.member_count}
		end
	end
	self:on('state_change',self.handle_change)
end

function Member:enable()
	for key,opts in pairs(self.systems) do
		local group = self.config.cluster.system[key]
		local chunk = {}
		local data = self.systems[key]
		for i = data.id, #group.data, group.member_count do
			chunk[#chunk +1] = group.data[i]
		end
		if self.id == self.config.id then
			data.data = chunk
			data.alive = group.alive
			data.down = group.down
			data.config = group.config
		else
			data.data = chunk
			data.alive = group.down
			data.down = group.alive
			data.config = group.config
		end
		logger:info("chunk",self.id,key,chunk)
	end
	self:emit('state_change',self,'new')
end

function Member:probe(who)
	if self.state == 'probably_down' then
		self.probed[who] = true
		local count = 0
		for _,_ in pairs(self.probed) do
			count = count + 1
		end
		logger:debug('checking quorum',count,self.config.quorum)
		if count >= self.config.quorum then
			self:clear_alive_check()
			self:update_state('down')
		end
	end
end

function Member:needs_ping()
	return not (self.id == self.config.id) and (
			(hrtime() - self.last_check > (1.5 * self.config.gossip_interval)) or 
			(hrtime() - self.last_send > self.config.gossip_interval) or 
			(self.state == 'probably_down'))
end

function Member:needs_probe()
	return (hrtime() - self.last_check > 750)
end

function Member:alive(seq)
	-- we need to drop duplicates
	if not (self.seq == seq) then
		logger:debug('updating seq',self.id)
		self.seq = seq
		self.last_check = hrtime()
		self.probed = {}
		self:clear_alive_check()
		self:update_state('alive')
	end
end

function Member:ping()
	self.last_send = hrtime()
	self.packet_seq = self.packet_seq + 1
	return self.config.key ..' ping:' .. self.packet_seq.. ',' .. self.config.id .. ';'
end

function Member:update_state(new_state)
	if not (self.state == new_state) and not ((new_state == 'probably_down') and (self.state == 'down'))then
		logger:debug('member is transitioning',self.id,self.state,new_state)
		
		self:clear_alive_check()
		
		self:emit('state_change',self,new_state)
		self.state = new_state
		self:probe(self.config.id)
	end
end

function Member:start_alive_check()
	if (self.state == 'alive') or (self.state == 'new')then
		local timeout = self.config.ping_timeout
		if self.state == 'new' then
			-- if we are starting up, we give everything a bit of time to
			-- catch up
			timeout = timeout * 10
		end
		if not self.timeout then
			logger:debug('starting alive check',self.state)
			self.timeout = timer.setTimeout(timeout,self.update_state,self,'probably_down')
		end

	end
end

function Member:clear_alive_check()
	if self.timeout then
		timer.clearTimer(self.timeout)
	end
end

function Member:handle_change(new_state)
	if new_state == 'alive' and not (self.state == 'probably_down') then
		self.config.members_alive = self.config.members_alive + 1
	elseif new_state == 'down' then
		self.config.members_alive = self.config.members_alive - 1
	else
		return
	end

	logger:info('member has transitioned',self.id,self.state,new_state,self.systems)
	for key,opts in pairs(self.systems) do
		for _idx,value in pairs(opts.data) do

			local child = spawn(opts[new_state],{value,JSON.stringify(opts.config)})
			child.stdout:on('data', function(chunk)
				logger:debug("got",chunk)
			end)
			child:on('exit', function(code,other)
				if not (code == 0 ) then
					logger:error("script failed",key,opts[new_state],{value,JSON.stringify(opts.config)})
				else
					logger:info("script worked",key,opts[new_state],{value,JSON.stringify(opts.config)})
				end
			end)
		end
	end
end


return Member