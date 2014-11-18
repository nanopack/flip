-- -*- mode: lua; tab-width: 2; indent-tabs-mode: 1; st-rulers: [70] -*-
-- vim: ts=4 sw=4 ft=lua noet
---------------------------------------------------------------------
-- @author Daniel Barney <daniel@pagodabox.com>
-- @copyright 2014, Pagoda Box, Inc.
-- @doc
--
-- @end
-- Created :   23 Oct 2014 by Daniel Barney <daniel@pagodabox.com>
---------------------------------------------------------------------

local Emitter = require('core').Emitter

local Logger = Emitter:extend()

local levels = {'debug','info','warning','error','fatal'}



do
	-- we need to preserve the level inside of the function call
	local gen_level = function(level)
		return function (self, ...)
			-- eventually we want to do this
			for i = 1, #levels do
				self:emit('.' .. levels[i], ...)
				if levels[i] == level then
					-- we don't want anything higher then this
					-- level
					break
				end
			end
		end
	end

	for _idx,level in ipairs(levels) do
		Logger[level] = gen_level(level)
	end
end

function Logger:initialize ()
	self.loggers = {}
end

function Logger:add_logger(level,id,fun)
	if not self.loggers[id] then
		if not fun then
			fun = p
		end

		self.loggers[id] = fun

		self:on('.' ..level,fun)
	else
		return 'already exists'
	end
end

function Logger:remove_logger(id)
	local logger = self.loggers[id]
	if not logger then
		return 'not found'
	else
		self:removeListener('.' .. level,logger)
	end
end

return Logger:new()