-- -*- mode: lua; tab-width: 2; indent-tabs-mode: 1; st-rulers: [70] -*-
-- vim: ts=4 sw=4 ft=lua noet
---------------------------------------------------------------------
-- @author Daniel Barney <daniel@pagodabox.com>
-- @copyright 2014, Pagoda Box, Inc.
-- @doc
--
-- @end
-- Created :   20 Nov 2014 by Daniel Barney <daniel@pagodabox.com>
---------------------------------------------------------------------

local Packet = require('../lib/packet')

local test = require('../modules/tape')("Packet encode/decode test")

local packet = Packet:new()

test("packets encode and decode to the same data", nil, function(t)
	local key = "secret key"
	local id = 1234
	local seq = 6824
	local nodes = {false,true,false,true,false,true}
	local data = packet:build(key,id,seq,nodes)
	t:is_string(data,"building a packet should return a packet as a string")

	local key1,id1,seq1,nodes1 = packet:parse(data)
	t:is_number(id1,"decoded id should be a number")
	t:is_number(seq1,"decoded seq should be a number")
	t:is_array(nodes1,"decoded list of nodes should be an arry")
	t:equal(#nodes,#nodes1,"should have the same number of members")
	for idx,value in pairs(nodes1) do
		t:is_boolean(value,"node should only be a boolean")
		t:equal(value,nodes[idx],"all nodes in the srray should have the same state")
	end

  t:finish()
end)

