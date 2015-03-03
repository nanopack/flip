-- -*- mode: lua; tab-width: 2; indent-tabs-mode: 1; st-rulers: [70] -*-
-- vim: ts=4 sw=4 ft=lua noet
---------------------------------------------------------------------
-- @author Daniel Barney <daniel@pagodabox.com>
-- @copyright 2014, Pagoda Box, Inc.
-- @doc
--
-- @end
-- Created :   2 Feb 2015 by Daniel Barney <daniel@pagodabox.com>
---------------------------------------------------------------------

-- when a new master is chosen this script is called.
return function(members,cb)
	for idx,member in pairs(members) do

		if not (store.id == member.id) then
			store:begin_sync(member.replication_ip,member.replication_port)
		end
	end
	cb()
end