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
local Replicated = Emitter:extend()

-- replicated is basically a noop. everything gets added in everywhere
-- so there is no reason to divide anything up
function Replicated:divide(data,id,is_alive)
	return data,{}
end

return Replicated