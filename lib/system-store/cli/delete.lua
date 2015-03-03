-- -*- mode: lua; tab-width: 2; indent-tabs-mode: 1; st-rulers: [70] -*-
-- vim: ts=4 sw=4 ft=lua noet
---------------------------------------------------------------------
-- @author Daniel Barney <daniel@pagodabox.com>
-- @copyright 2014, Pagoda Box, Inc.
-- @doc
--
-- @end
-- Created :   4 Feb 2015 by Daniel Barney <daniel@pagodabox.com>
---------------------------------------------------------------------

-- this will eventually be run by the cli when trying to delete
-- something from the store

return function(bucket,id,last_known)
	logger:info("I am trying to delete something from the cluster",bucket,id)
	local object,err = store:delete(bucket,id,last_known)
	logger:info("got results",object,err)
end