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
return
	{shard = require('./sharded')
	,replicate = require('./replicated')}