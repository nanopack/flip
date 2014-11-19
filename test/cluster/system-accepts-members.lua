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


local Member = require('../../lib/member')
local System = require('../../lib/system')

for i=1,10 do
	data[#data + 1] = 'testing_' .. i
end

local sys_config = 
	{ip = 
		{alive = 'up'
		,down = 'down'
		,type = 'sharded'
		,config = {}
		,data = data}}



local main = System:new(sys_config,'testing_1')
local global_config = 
	{id = 'testing_1'}

local members = {}
for i=1,10 do
	local mem_config = 
		{id = 'id_'.. i
		,systems = ['ip']}

	local member = Member:new(mem_config)
	assert(#member.id == 'id_' .. i)
	assert(member.systems[1] == 'ip')
	assert(#member.systems == 1)
	members[#members + 1] = member
	
	local count = main.members
	main:add_member(member)
	assert((main.members - count) == 1)
end

assert(#main.members == 10)
main:enable()

