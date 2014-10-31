-- -*- mode: lua; tab-width: 2; indent-tabs-mode: 1; st-rulers: [70] -*-
-- vim: ts=4 sw=4 ft=lua noet
---------------------------------------------------------------------
-- @author Daniel Barney <daniel@pagodabox.com>
-- @copyright 2014, Pagoda Box, Inc.
-- @doc
--
-- @end
-- Created :   24 Oct 2014 by Daniel Barney <daniel@pagodabox.com>
---------------------------------------------------------------------

local Emitter = require('core').Emitter
local logger = require('./logger')

local Element = Emitter:extend()

function Element:initialize(value,opts)
	self.opts = opts
	self.value = value
end

function Element:set(member)
	if not (self.member == member) then
		logger:info('updating member')
		if member:is_this() then
			self.opts[]
		end
	else
		logger:info('updated to same member')
	end
end