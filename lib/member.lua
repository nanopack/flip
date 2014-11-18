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
local JSON = require('json')
local os = require('os')
local timer = require('timer')
local table = require('table')
local logger = require('./logger')
local system = require('./system')

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
	logger:debug('created member',config)
	self.probed = {}
	self.systems = {}
	
	if not config.systems then
		config.systems = {}
	end

	local node_keys = {}

	-- we only want this node to failover for systems that it has
	-- assigned to it
	if global.servers[global.id].systems then
		for _idx,key in pairs(global.servers[global.id].systems) do
			node_keys[key] = true
		end
	end

	-- create a system for this member and its data
	for _ids,key in pairs(config.systems) do
		if node_keys[key] then
			local group = self.config.cluster.system[key]
			if not system[group.type] then
				logger:fatal('unknown system type',group.type)
				process.exit(1)
			end
			self.systems[key] = (system[group.type]):new(group)
		end
	end
	-- self:on('state_change',self.handle_change)
end

function Member:enable()
	self:emit('state_change',self,'new')
	-- set up the cluster for this member
	for key,system in pairs(self.systems) do
		system:enable(self)
	end
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
	self:clear_alive_check()
	if not (self.state == new_state) and not ((new_state == 'probably_down') and (self.state == 'down'))then
		
		self:emit('state_change',self,new_state)

		logger:info('member has transitioned',self.id,self.state,new_state)
		
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
			timeout = timeout * 2
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
		self.timeout = nil
	end
end

return Member