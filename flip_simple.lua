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
local math = require('math')
local logger = require('lib/logger')
local Member = require('lib/member')

local Flip = Emitter:extend()

function Flip:initialize(config)
	self.config = config
	self.servers = {}
	self.dgram = dgram.createSocket('udp4')
	self.dgram:bind(self.config.gosip_port,self.config.gosip_ip)

	for opts in config.servers do
		logger.debug("creating member",opts)
		member = Member:new(opts)
		member:on('state_change',self:handle_change)
		self:add_member(member)
	end
end

function Flip:start()
	self.dgram:on('message',self.handle_message)
	self.gossip_timer = timer.setTimeout(self.config.gossip_interval, self:gossip_time)
end

function Flip:find_member(key)
	return self.servers[key]
end

function Flip:add_member(member)
	local key = member.id
	self.servers[key] = member
end

function Flip:get_gossip_members()
	local members = []
	for member in self.servers do
		if member:needs_ping() then
			-- this should be randomized, and important ones at the head of
			-- the list
			members[#members] = member
	end
	return table.sort(members,Flip:sort_members)
end

function Flip:sort_members(member,member2)
	-- i need to check this
	p(member,member2)
	return (member.need_ack() == member2.need_ack() or math.random() == 1)
end

function Flip:handle_message(msg, rinfo)
	logger.debug('message received',msg,rinfo)
	local key,seq,cmds  = self:validate_message(msg,rinfo)
	if key == self.config.key then
		local member = self:find_member(id)

		if member then
			member:alive(seq)
			local packet = member:ping()
			logger.debug('sending ping (ack)',id)
			self:send_packet(packet)
		else
			logger.warning('unknown member',rinfo)
		else
			
		end
	else
		logger.warning('wrong key in packet',rinfo)
	end
end

function Flip:validate_message (msg)
	-- 'key seq cmd:arg,arg2;cmd2:arg;'
	local key,seq,rest = msg:match("([^ ]+) ([^ ]+) (.+)")
	local cmds = []
	while true do
		local cmd,args = rest:match("([^:]+):([^;]+);")
		cmds[#cmds] = 
			{cmd = cmd
			,args = args:split(',')}
	end
	
	return key,seq,cmds
end

function Flip:handle_change(member,old_state,new_state)
	logger.error('member has transitioned',member,old_state,new_state)
end


function Flip:gossip_time()
	local members = self:get_gossip_members()
	self:ping_members(members)
end

function Flip:ping_members(members)
	local member = nil
	local count = 0
	while member = members.pop() and count < self.config.max_pings do
		local packet = member:ping()

		logger.debug('sending ping',member.id)
		self:send_packet(packet)
		member:start_alive_check()
	end

	-- if we still have some members left over, we need to ping them
	-- on the next timeout. Otherwise we start gossiping all over again
	if not #members == 0 then
		timer.setTimeout(self.config.gossip_interval,self:ping_members,members)
	else
		timer.setTimeout(self.config.gossip_interval,self:gossip_time)
	end

end

function Flip:send_packet(packet)

end