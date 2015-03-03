-- -*- mode: lua; tab-width: 2; indent-tabs-mode: 1; st-rulers: [70] -*-
-- vim: ts=4 sw=4 ft=lua noet
---------------------------------------------------------------------
-- @author Daniel Barney <daniel@pagodabox.com>
-- @copyright 2014, Pagoda Box, Inc.
-- @doc
--
-- @end
-- Created :   30 Jan 2015 by Daniel Barney <daniel@pagodabox.com>
---------------------------------------------------------------------
return function(data,id,is_alive)
	for idx,is_alive in pairs(is_alive) do
		if is_alive then
			return {data[idx % #data +1 ]},{}
		end
	end

	-- it should never get here. how could no servers ever be alive?
	return {},{}
end