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

local Sharded = require('../../lib/plan/topologies/sharded')

local test = require('../../modules/tape')("Sharded Topology")

local generate = function(ncount,dcount)
	local nodes = {}
	local data = {}
	for i=1,ncount do
		nodes[#nodes + 1] = true
	end

	for i=1,dcount do
		data[#data + 1] = "192.168.0." .. string.char(i)
	end

	return nodes,data
end

local shard = Sharded:new()

test("sharded evenly divides up all data points over available nodes", nil, function(t)
	for count=1,20 do
		local nodes,data = generate(count,count)
		for i=1,count do
			local add,remove = shard:divide(data,i,nodes)
			t:equal(add[1],data[i],"Node ".. i .." didn't get the right data point")
			t:equal(#remove,count - 1,"other nodes should be responsible for the other data points")
		end
	end

  t:finish()
end)


local all_permutations = nil

all_permutations = function(data,cb,index)
	local count = #data
	if not index then 
		index = 1
	elseif index > count then
		return
	end
	for i=index,count do
		-- we want to test with this element being down
		data[i] = false
		cb(data)
		all_permutations(data,cb,i + 1)

		-- we want to test with this element being up
		data[i] = true
		cb(data)
		all_permutations(data,cb,i + 1)
	end
end


test("data points are never missing in node failover situations", nil, function(t)
	-- ok, so this needs to be not a very large number
	-- all permutations of large arrays is a lot of computations
	-- 5 -> 0m0.048s
	-- 6 -> 0m0.075s
	-- 7 -> 0m0.207s
	-- 8 -> 0m0.709s
	-- 9 -> 0m2.562s
	-- 10 -> 0m9.669s
	-- 11 -> 0m37.025s
	-- 12 -> 2m16.293s
	-- 13 -> maybe 9 minutes? (didn't run)
	-- 14 -> maybe 37 minutes? (didn't run)
	-- 15 -> maybe a few hours? (didn't run)
	for count=1,9 do
		local base_nodes = generate(count,count)
		
		all_permutations(base_nodes,function(nodes)
			for dcount=1,count do
				local all_up = {}
				local _nodes,data = generate(0,dcount)

				for i=1,count do
					if nodes[i] then
						local add = shard:divide(data,i,nodes)
						for _idx,value in pairs(add) do
							all_up[#all_up + 1] = value
						end
					end
				end
				if #all_up == 0 then
					for idx,value in pairs(nodes) do
						t:equal(false,value,"all servers are down")
					end
				else

					table.sort(all_up)
					t:equal(#all_up,#data,"we shouldn't have any duplicate or missing data points")
					for idx,value in pairs(all_up) do
						t:equal(data[idx],value,"all data points should be present on the up nodes")
					end
				end
			end
		end)

	end

  t:finish()
end)

