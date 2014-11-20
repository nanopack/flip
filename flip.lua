-- -*- mode: lua; tab-width: 2; indent-tabs-mode: 1; st-rulers: [70] -*-
-- vim: ts=4 sw=4 ft=lua noet
---------------------------------------------------------------------
-- @author Daniel Barney <daniel@pagodabox.com>
-- @copyright 2014, Pagoda Box, Inc.
-- @doc
--
-- @end
-- Created :   4 Sept 2014 by Daniel Barney <daniel@pagodabox.com>
---------------------------------------------------------------------

local Emitter = require('core').Emitter
local dgram = require('dgram')
local timer = require('timer')
local table = require('table')
local string = require('string')
local json = require('json')
local math = require('math')
local logger = require('./lib/logger')
local Member = require('./lib/member')
local System = require('./lib/system')
local Packet = require('./lib/packet')

local Flip = Emitter:extend()

function Flip:initialize(config)
	self.config = config
	self.servers = {}
	self.iservers = {}
	self.alive = {}
	self.packet = Packet:new()

	self.system = System:new(config.cluster,config.id)
	
	for idx,opts in pairs(config.sorted_servers) do
		
		-- everything starts out alive
		self.alive[idx] = true

		member = Member:new(opts,config)
		self.system:add_member(member)
		self:add_member(member)
		member:on('state_change',function(...) self:track(...,idx) end)
		if member.id == config.id then
			self.id = idx
		end
	end

	self.commands = 
		{ping = self.ping
		,probe = self.probe
		,down = self.probe}

end

function Flip:start()
	self.system:enable()

	local member = self:find_member(self.config.id)
	member:update_state('alive')
	
	local socket = dgram.createSocket('udp4')
	socket:bind(member.port,member.ip)
	socket:on('message',function(...) self:handle_message(...) end)
	self.dgram = socket

	self.gossip_timer = timer.setTimeout(self.config.gossip_interval, self.gossip_time, self)
end

function Flip:find_member(key)
	if type(key) == "number" then
		return self.iservers[key]
	else
		return self.servers[key]
	end
end

function Flip:add_member(member)
	local key = member.id
	self.servers[key] = member
	self.iservers[#self.iservers + 1] = member
end

function Flip:get_gossip_members()
	local members = {}
	for _idx,member in pairs(self.servers) do
		if member:needs_ping() then
			-- this should be randomized, and important ones at the head of
			-- the list
			members[#members + 1] = member
		end
	end
	table.sort(members,Flip.sort_members)
	return members
end

function Flip:sort_members(member,member2)
	-- i need to check this
	logger:debug("checking",member,member2)
	return (math.random() == 1)
end

function Flip:handle_message(msg, rinfo)
	logger:debug('message received',msg,rinfo)
	local key,id,seq,nodes = self.packet:parse(msg)
	if key == self.config.key then
		local down = {}
		self:ping(seq,id,nodes)
	else
		logger:warning('wrong key in packet',rinfo,msg)
	end
end



function Flip:gossip_time()
	collectgarbage()
	local members = self:get_gossip_members()
	self:ping_members(members)
end

function Flip:ping_members(members)
	if not members then
		logger:debug('no more members')
		timer.setTimeout(self.config.gossip_interval,self.gossip_time,self)
		return
	end
	local member = table.remove(members,1)
	local count = 0
	while member do
		if member:needs_ping() then
			local packet = self.packet:build(self.config.key,self.id,member:next_seq(),self.alive)
			logger:debug('sending ping',member.id)
			self:send_packet(packet,member)
			member:start_alive_check()
			count = count + 1 
		end
		if count < self.config.ping_per_interval then
			member = table.remove(members,1)
		else
			break
		end
	end

	logger:debug("done with round")
	-- if we still have some members left over, we need to ping them
	-- on the next timeout. Otherwise we start gossiping all over again
	if not (#members == 0) then
		timer.setTimeout(self.config.gossip_interval,self.ping_members,self,members)
	else
		timer.setTimeout(self.config.gossip_interval,self.gossip_time,self)
	end

end

function Flip:send_packet(packet,member)
	logger:debug('sending',packet)
	self.dgram:send(packet, member.port, member.ip, function(err)
		if err then
			logger:error('udp send errored',err)
		end
	end)
end

function Flip:ping(seq,id,nodes)
	local member = self:find_member(id)

	if member then
		member:alive(seq)
		if member:needs_ping() then
			local packet = self.packet:build(self.config.key,self.id,member:next_seq(),self.alive)
			logger:debug('sending ping (ack)',id)
			self:send_packet(packet,member)
			for node,alive in pairs(nodes) do
				if not alive then
					self:probe(id,node)
				end
			end
		end
	else
		logger:warning('unknown member',id)
	end
end

function Flip:track(member,new_state,id)
	if new_state == 'alive' then
		self.alive[id] = true
	elseif (new_state == 'down') or (new_state == 'probably_down') then
		self.alive[id] = false
	end
end

function Flip:probe(from,...)
	if not (self.config.id == who) then

		local down_member = self:find_member(who)

		if down_member then
			down_member:probe(from)
		end
	end
end

return Flip