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


local Emitter = require('core').Emitter
local Sharded = Emitter:extend()

-- sharded evenly divides the data over all nodes in the cluster. when
-- one node is down, the data is divided over the remaining nodes
function Sharded:divide(data,id,is_alive)
	local add = {}
	local remove = {}
	local idx = 0
	local count = #is_alive
	local alive_count = count
	
	-- count how many servers are alive, also shift this id down if any
	-- servers before this one have been turned off
	local failed_count = 0
	for idx,is_alive in pairs(is_alive) do
		if not is_alive then
			if idx < id then
				failed_count = failed_count + 1
			end
			alive_count = alive_count - 1
		end
	end

	-- the pattern ((i - 1) % count) + 1 is because lua arrays are not
	-- 0 based. so I shift it to 0 based, then I shift it back to 1 based
	for i=1,#data do
		if ((i - 1) % count) + 1 == id then
			-- the data point is assigned to this node
			add[#add + 1] = data[i]

		elseif not is_alive[((i - 1) % count) + 1] then
			-- if the other node is down, and we are responsible for it
			-- add it in
			if ((idx - 1) % alive_count) + 1 == (id - failed_count) then
				add[#add + 1] = data[i]

			else
				-- another node is responsible for the failover
				remove[#remove + 1] = data[i]
			end
			idx = idx + 1

		else
			-- the other node is alive
			remove[#remove + 1] = data[i]
		end
	end

	return add,remove
end

return Sharded