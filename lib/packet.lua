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

local pack = function(value)
	local a,b,c,d
	d = value 
	c = math.floor(d / 256)
	b = math.floor(c / 256)
	a = math.floor(b / 256)
	return string.char(d % 256),string.char(c % 256),string.char(b % 256),string.char(a % 256)
end

local integerify = function(val)
	local value = 0
	for i=val:len(),1,-1 do
		value = (value * 256) + val:byte(i)
	end
	return value
end

function Packet:build(secret,id,seq,alive_servers)
	if not (#alive_servers < 512) then
		logger:fatal("too many servers")
		process.exit(1)
	end
	local a,b,c,d = pack(id)
	local a1,b1,c1,d1 = pack(seq)
	local chunks = {secret:sub(0,32),string.rep("0",32 - math.min(32,secret:len())),a,b,c,d,a1,b1,c1,d1}
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
	local header_size = 32 + 8
	local id = -1

	for idx=header_size+1,size do
		local byte = packet:byte(idx)
		for i=0,7 do
			nodes[#nodes + 1] = not (bit.band(byte,bit.lshift(1,i)) == 0)
		end
	end
	return packet:sub(1,32),integerify(packet:sub(33,36)),integerify(packet:sub(37,40)),nodes
end

return Packet