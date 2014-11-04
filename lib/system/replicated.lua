-- -*- mode: lua; tab-width: 2; indent-tabs-mode: 1; st-rulers: [70] -*-
-- vim: ts=4 sw=4 ft=lua noet
---------------------------------------------------------------------
-- @author Daniel Barney <daniel@pagodabox.com>
-- @copyright 2014, Pagoda Box, Inc.
-- @doc
--
-- @end
-- Created :   4 Nov 2014 by Daniel Barney <daniel@pagodabox.com>
---------------------------------------------------------------------

local Emitter = require('core').Emitter
local JSON = require('json')
local spawn = require('childprocess').spawn
local logger = require('../logger')

local Replicate = Emitter:extend()

function Replicate:initialize(system)
	self.system = system
end

function Replicate:enable(member)
	if member.id == member.config.id then
		logger:info("replicating")
		local me = self
		member:once('state_change',function(...) me:handle_change(...) end)
	end
end

function Replicate:handle_change(member,new_state)
	if new_state == 'alive' then
		local sys = self.system
		for idx,value in pairs(sys.data) do
			local child = spawn(sys.alive,{value,JSON.stringify(sys.config)})
			child.stdout:on('data', function(chunk)
				logger:debug("got",chunk)
			end)
			child:on('exit', function(code,other)
				if not (code == 0 ) then
					logger:error("script failed",sys.alive,{value,JSON.stringify(sys.config)})
				else
					logger:info("script worked",sys.alive,{value,JSON.stringify(sys.config)})
				end
			end)
		end
	end
end

return Replicate