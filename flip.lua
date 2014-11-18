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
local math = require('math')
local logger = require('./lib/logger')
local Member = require('./lib/member')
local systems = require('./lib/system')

local Flip = Emitter:extend()

function Flip:initialize(config)
	local main_server = config.servers[config.id]
	if not main_server then
		logger:fatal("this server is not in the server config block",config.id,config.servers)
		process.exit(1)
	end
	config.members_alive = 0
	self.config = config
	self.servers = {}
	self.note = {}
	if not config.systems then
		config.systems = {}
	end
	self.dgram = dgram.createSocket('udp4')
	self.dgram:bind(main_server.port,main_server.ip)

	-- default quorum needed
	if not self.config.quorum then
		local server_count = 0
		for _id,_value in pairs(self.config.servers) do
			server_count = server_count + 1
		end
		self.config.quorum = math.floor(server_count/2) + 1
	end

	-- we sort these so that we can ensure that they are the same across
	-- all nodes
	table.sort(config.servers,function(mem1,mem2) 
		return mem1.id < mem2.id 
	end)

	-- we need to merge all the configs together
	for key,value in pairs(config.cluster.system) do
		local merged = {}
		if config.cluster.config then
			for k,v in pairs(config.cluster.config) do
				merged[k] = v
			end
		end

		if value.config then
			for k,v in pairs(value.config) do
				merged[k] = v
			end
		end

		value.config = merged
		if value.type and systems[value.type] and systems[value.type].prepare then
			value.data = systems[value.type].prepare(value.data)
		else
			-- this will only work with strings....
			table.sort(value.data)
		end
	end

	for id,opts in pairs(config.servers) do
		opts.id = id
		member = Member:new(opts,config)
		self:add_member(member)
		member:on('state_change',function(...) self:check(...) end)
	end

	self.commands = 
		{ping = self.ping
		,probe = self.probe
		,down = self.probe}

end

function Flip:start()
	for id,member in pairs(self.servers) do
		member:enable()
	end

	local member = self:find_member(self.config.id)
	member:update_state('alive')
	self.dgram:on('message',function(...) self:handle_message(...) end)
	self.gossip_timer = timer.setTimeout(self.config.gossip_interval, self.gossip_time, self)
end

function Flip:find_member(key)
	return self.servers[key]
end

function Flip:add_member(member)
	local key = member.id
	self.servers[key] = member
end

function Flip:get_gossip_members()
	local members = {}
	logger:debug('servers',self.servers)
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
	local key,cmds  = self:validate_message(msg,rinfo)
	if key == self.config.key then
		for _indx,command in pairs(cmds) do
			local cmd = command.cmd
			local args = command.args
			if self.commands[command.cmd] then
				self.commands[command.cmd](self,unpack(args))
			end
		end
	else
		logger:warning('wrong key in packet',rinfo)
	end
end


function Flip:validate_message (msg)
	-- 'key cmd:arg,arg2;cmd2:arg;'	
	
	-- this really needs to be more efficient.
	local key,rest = msg:match("([^ ]+) (.+)")
	logger:debug("packet",key,rest)
	local cmds = {}
	while rest and rest:len() > 0 do
		local cmd,args
		cmd,args,rest = rest:match("([^:]+):([^;]+);(.*)")
		split = {}
		if args then
			for arg in string.gmatch(args, "([^,]+),?") do
				split[#split + 1] = arg
			end
		end
		logger:debug("cmd",cmd,split,rest)
		cmds[#cmds + 1] = 
			{cmd = cmd
			,args = split}
	end
	
	return key,cmds
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
			local packet = member:ping()
			logger:debug('sending ping',member.id)
			local notes = self:notes()
			self:send_packet(packet .. notes,member)
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

function Flip:notes()
	local notes = ''
	local down = ''
	local pdown = ''
	for id,flags in pairs(self.note) do
		if flags.pdown then
			if pdown == '' then
				pdown = pdown .. id
			else
				pdown = pdown .. ',' ..id
			end
		end
		if flags.down then
			if down == '' then
				down = down .. id
			else
				down = down .. ',' ..id
			end
		end
	end
	if not (down == '') then
		notes = 'down:' .. self.config.id .. ',' .. down .. ';'
	end
	if not (pdown == '') then
		notes = 'probe:' .. self.config.id .. ',' .. pdown .. ';'
	end
	return notes
end

function Flip:ping(seq,id)
	local member = self:find_member(id)

	if member then
		member:alive(seq)
		if member:needs_ping() then
			local packet = member:ping()
			logger:debug('sending ping (ack)',id)
			local notes = self:notes()
			self:send_packet(packet .. notes,member)
		end
	else
		logger:warning('unknown member',id)
	end
end

function Flip:check(member,new_state)
	local notes = self.note[member.id]
	if not notes then
		notes = {}
		self.note[member.id] = notes
	end
	notes.pdown = (new_state == 'probably_down')
	notes.down = (new_state == 'down')
end

function Flip:probe(from,...)
	local member = self:find_member(from)
	if member then
		logger:debug("probing",...)
		for _idx,who in pairs({...}) do
			if not (self.config.id == who) then

				local down_member = self:find_member(who)

				if down_member then
					down_member:probe(from)
				else
					logger:warning('unknown member',who)
				end
			end
		end
	else
		logger:warning('unknown member',from)
	end
end

return Flip