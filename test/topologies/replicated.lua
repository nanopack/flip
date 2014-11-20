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

local Replicated = require('../../lib/plan/topologies/replicated')

local test = require('../../modules/tape')("Replicate Topology")

test("replicated returns all of the data points in an array", nil, function(t)
	local data = {"1","2","3"}
	local replicate = Replicated:new()
	local add,remove = replicate:divide(data)
	t:is_table(add,"should have returned a table")
	t:is_table(remove,"should have returned a table")
	t:equal(#data,#add,"everything should be added")
	t:equal(0,#remove,"nothing should ahve been removed")

	for idx,value in pairs(add) do
		t:is_string(value,"value should only be a string")
		t:equal(value,data[idx],"all data points should be the same")
	end

  t:finish()
end)

