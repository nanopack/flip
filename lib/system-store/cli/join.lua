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

-- this will cause a flip server to join another flip cluster

return function(ip,port)
	logger:info("I am trying to join the cluster",ip,port)
	local err,body = store:request('post','cluster/join',ip,port,nil,nil)
	logger:info(err,body)
end