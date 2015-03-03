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
local hrtime = require('uv').hrtime
local logger = require('./logger')
local Member = require('./member')
local System = require('./system')
local Packet = require('./packet')
local Api = require('./api')
local Store = require('./store')

local Flip = Emitter:extend()

function Flip:initialize(config)
	self.config = config
	self.config.quorum = 0
	self.members = {}
	self.member_count = 0
	self.alive = {}
	self.map = {}
	self.inverse_map = {}
	self.packet = Packet:new()
	self.api = Api:new(self,config.api.port,config.api.ip)

	self.store = Store
	Store:configure(config.db,config.id,self,config.replication.ip,config.replication.port,self.api)
end

function Flip:start()
	-- we create a system so that it is setup by the time that servers
	-- are added in, it starts working and creating plans
	self.system = System:new(self.store,self)

	self.store:open(function(err)
		if not err then

			-- if I'm not a member of the cluster, lets set that up.
			
			local member,err = self.store:fetch("servers",self.config.id)
			logger:debug("found member",member,err)
			if err == "MDB_NOTFOUND: No matching key/data pair found" then
				logger:info("bootstrapping node into single cluster")
				me = 
					{ip = self.config.gossip.ip
					,port = self.config.gossip.port
					,http_ip = self.config.api.ip
					,http_port = self.config.api.port
					,replication_ip = self.config.replication.ip
					,replication_port = self.config.replication.port
					,systems = {'store'}}
				local object,err = self.store:store("servers",self.config.id,me)
				if err then
					logger:error("unable to create cluster: ",err)
					process:exit(1)	
				end
			elseif err then
				logger:error("unable to access store: ",err)
				process:exit(1)
			end

			local members = self.store:fetch("servers")
			-- load all members into the monitor
			logger:debug("bootstrapping members",members)
			for _idx,member in pairs(members) do
				self:process_server_update("store",member.id,member)
			end

			-- we subscribe to any changes in the servers bucket
			self.store:on("servers",function(kind,id,data) self:process_server_update(kind,id,data) end)

			-- double check that the default config has been added in
			local config,err = self.store:fetch("config","cluster")
			logger:debug("got config",config,err,self.id)
			if err == "MDB_NOTFOUND: No matching key/data pair found" then
				key = "secret"
				config = 
					{["gossip_interval"] = 1000
					,["ping_per_interval"] = 1
					,["ping_timeout"] = 1500
					,["key"] = key:sub(0,32) .. string.rep("0",32 - math.min(32,key:len()))}
				self.store:store("config","cluster",config)
			elseif err then
				logger:error("unable check config")
			end

			local id = self.config.id
			for key,value in pairs(config) do
				self.config[key] = value
			end
			self.config.id = id
			-- now that we have been added in, lets start up the system
			self.system:enable()

			-- we set ourself to be alive. This probably should be a quorum
			-- decision TODO
			local member = self:find_member(self.config.id)
			if member == nil then
					logger:info("unable to find this server",self.config.id,err,member)
					process:exit(1)
			else
				member:update_state('alive')
			
				-- we start responding to udp queries
				local socket = dgram.createSocket('udp4')
				logger:debug("udp socket",member.port,member.ip)
				socket:bind(member.port,member.ip)
				socket:on('message',function(...) self:handle_message(...) end)
				self.dgram = socket

				-- we start probing other members
				self.gossip_timer = timer.setTimeout(self.config.gossip_interval, self.gossip_time, self)
			end
		else
			logger:error("unable to start the store: ",err)
			process:exit(1)
		end
	end)
end

-- This function handles updates from the store for members
-- as member data changes, systems added/removed etc, this function
-- will be passed in the changes.
function Flip:process_server_update(kind,id,data)
	logger:debug("server update",kind,id,data)
	if kind == "store" then
		local member = self.members[id]
		if member then
			member:update(data)
		else
			self.map[id] = #self.map + 1
			self.inverse_map[self.map[id]] = id
			self.member_count = self.member_count +1
			member = Member:new(id,data,self.config)
			self.members[id] = member
			member:on('state_change',function(...) self:track(id,...) end)
			member:enable()
		end
		self.system:regen(data.systems)
	elseif kind == "delete" then
		local member = self.members[id]
		self.members[id] = nil
		if member then
			self.member_count = self.member_count -1
			member:destroy()
			self.inverse_map[self.map[id]] = nil
			self.map[id] = nil
		end
		self.system:regen(data.systems)
	end
	if self.timer then
		timer.clearTimer(self.timer)
		self:ping_members()
	end
	self.config.quorum = math.floor(self.member_count/2) +1
	logger:info("updating quorum to",self.config.quorum)
end

function Flip:find_member(key)
	if type(key) == "number" then
		local id = self.store:index("servers",key)
		if id then
			return self.members[id]
		end
	else
		-- server =  self.store:fetch("servers",key)
		return self.members[key]
	end
end

function Flip:get_idx()
	return self.store:get_index("servers",self.config.id)
end

function Flip:get_gossip_members()
	local members = {}
	for _idx,member in pairs(self.members) do
		if member:needs_ping() then
			members[#members + 1] = member
		end
	end
	-- important ones should be at the front of the list
	table.sort(members,Flip.sort_members)
	return members
end

function Flip:sort_members(member,member2)
	-- i need to check this
	logger:debug("checking",member,member2)
	return (math.random() == 1)
end

function Flip:handle_message(msg, rinfo)
	logger:debug('message received',msg:len(),rinfo)
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
		self.timer = timer.setTimeout(self.config.gossip_interval,self.gossip_time,self)
		return
	end
	local member = table.remove(members,1)
	local count = 0
	local idx = self:get_idx()
	while member do
		if member:needs_ping() then
			local packet = self.packet:build(self.config.key,idx,member:next_seq(),self.alive)
			logger:debug('sending ping',member.id,packet:len())
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
		self.timer = timer.setTimeout(self.config.gossip_interval,self.ping_members,self,members)
	else
		self.timer = timer.setTimeout(self.config.gossip_interval,self.gossip_time,self)
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
	logger:debug('got ping',id,member.id)
	if member then
		member:alive(seq)
		if member:needs_ping() then
			local idx = self:get_idx()
			local packet = self.packet:build(self.config.key,idx,member:next_seq(),self.alive)
			logger:debug('sending ping (ack)',id)
			self:send_packet(packet,member)
		end
		for node,alive in pairs(nodes) do
			if not alive then
				self:probe(id,node)
			end
		end
	else
		logger:warning('unknown member',id)
	end
end

function Flip:track(id,member,new_state)
	if self.api then
		self.api.status:push(
			{id = member.id
			,state = new_state
			,opts = member.opts
			,systems = member.systems
			,time = hrtime()})
	end

	local server,err = self.store:fetch("servers",id)
	if err and not (err == "old data") then
		logger:info("member doesn't exist anymore",server,err,id)
		process:exit(1)
	end
	
	-- we need to regenerate all the systems that are on this member
	-- but async, so that the server state is correct
	if (new_state == 'alive') or (new_state == 'down') then
		timer.setTimeout(0,function() self.system:regen(server.systems) end)
	end

	-- this is used to build the packets
	if new_state == 'alive' then
		self.alive[self.map[id]] = true
	elseif (new_state == 'down') or (new_state == 'probably_down') then
		self.alive[self.map[id]] = false
	end
end

function Flip:probe(from,who)
	if not (self.config.id == who) then
		local down_member = self:find_member(who)

		if down_member then
			down_member:probe(from)
		end
	end
end

return Flip