-- -*- mode: lua; tab-width: 2; indent-tabs-mode: 1; st-rulers: [70] -*-
-- vim: ts=4 sw=4 ft=lua noet
---------------------------------------------------------------------
-- @author Daniel Barney <daniel@pagodabox.com>
-- @copyright 2014, Pagoda Box, Inc.
-- @doc
--
-- @end
-- Created :   19 Nov 2014 by Daniel Barney <daniel@pagodabox.com>
---------------------------------------------------------------------


-- this probably could be optimized

local math = require('math')
local bit = require('bit')
local logger = require('./logger')
local Emitter = require('core').Emitter


local Packet = Emitter:extend()
function Packet:initialize()

end

function Packet:pack(value)
	local a,b,c,d
	d = value 
	c = math.floor(d / 256)
	b = math.floor(c / 256)
	a = math.floor(b / 256)
	return string.char(d % 256),string.char(c % 256),string.char(b % 256),string.char(a % 256)
end

function Packet:integerify(val)
	local value = 0
	for i=val:len(),1,-1 do
		value = (value * 256) + val:byte(i)
	end
	return value
end

-- packets are in the format:
-- [key]:32 [id]:4 [seq]:4 [ignore_bits]:1 [nodes]:??
-- key is the key used to identify the cluster
-- id is the id of the local server that is sending the packet
-- seq is the sequence id of the packet that we are sending
-- ignore_bits is how manny bits to ignore at the end of the node
--   array. as we may not have exactly (#nodes % 8 == 0) nodes
-- nodes is an array of bits where every bit represents this nodes
--   idea of the state of the cluster, each bit is a state of an
--   individual node.
--     1 == alive
--     0 == down
function Packet:build(secret,id,seq,alive_servers)
	if not (#alive_servers < 512) then
		logger:fatal("too many servers")
		process:exit(1)
	end
	local a,b,c,d = self:pack(id)
	local a1,b1,c1,d1 = self:pack(seq)
	local server_count = string.char(8 - (#alive_servers % 8))

	local chunks = 
		{secret:sub(0,32),string.rep("0",32 - math.min(32,secret:len()))
		,a,b,c,d
		,a1,b1,c1
		,d1
		,server_count}

	local idx
	local byte = 0
	for idx,alive in pairs(alive_servers) do
		local i = (idx-1) % 8
		if alive then
			byte = bit.bor(byte,bit.lshift(1,i))
		end
		if i == 7 then
			chunks[#chunks + 1] = string.char(byte)
			byte = 0
		end
	end
	if not (byte == 0) then
		chunks[#chunks + 1] = string.char(byte)
	end
	return table.concat(chunks)
end

function Packet:parse(packet)
	local size = packet:len()
	if(size < 32+8) then
		return
	end

	local nodes = {}
	local header_size = 32 + 9
	local id = -1

	local empty_servers = packet:byte(41)
	for idx=header_size+1,size do
		local byte = packet:byte(idx)
		local max = 7
		if idx == size then
			max = max - empty_servers
		end
		for i=0,max do
			nodes[#nodes + 1] = not (bit.band(byte,bit.lshift(1,i)) == 0)
			
		end
	end
	return packet:sub(1,32),self:integerify(packet:sub(33,36)),self:integerify(packet:sub(37,40)),nodes
end

return Packet