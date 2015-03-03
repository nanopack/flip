-- -*- mode: lua; tab-width: 2; indent-tabs-mode: 1; st-rulers: [70] -*-
-- vim: ts=4 sw=4 ft=lua noet
---------------------------------------------------------------------
-- @author Daniel Barney <daniel@pagodabox.com>
-- @copyright 2014, Pagoda Box, Inc.
-- @doc
--
-- @end
-- Created :   4 Sept 2014 by Daniel Barney <daniel@pagodabox.com>
---------------------------------------------------------------------

local fs = require('fs')
local os = require('os')
local JSON = require('json')
local math = require('math')
local hrtime = require('uv').hrtime
local Flip = require('./flip')
local logger = require('./logger')


-- init the random seed for later when it is used
math.randomseed(hrtime())


local data = nil

-- it can be specified with the first parameter to the command
if process.argv[1] == "-config-file" then
	logger:info("reading file",process.argv[2])
	data = fs.readFileSync(process.argv[2])
elseif process.argv[1] == "-config-json" then
	data = process.argv[2]
end

if not data then
	logger:fatal("no config options were able to be read")
	process:exit(1)
end

local validate_config = function(config)
	-- ensure that all requires values are in the config file
	local ensure = function(value,name,...)
		if not value then
			if not name then
				logger:fatal(...)
			else
				logger:fatal('config is missing required field \''.. name ..'\'')
			end
			process:exit(1)	
		end
	end

	ensure(config.id,'id')
	ensure(config.gossip,'gossip')
	ensure(config.gossip.ip,'gossip.ip')
	ensure(config.gossip.port,'gossip.port')
	ensure(config.api,'api')
	ensure(config.api.ip,'api.ip')
	ensure(config.api.port,'api.port')
	ensure(config.db,'db')
end

logger:debug("parsing",data)
local config = JSON.parse(data)

-- this will replace the old logger that was already setup as console
if config.log_level and logger:valid_level(config.log_level) then
	logger:add_logger(config.log_level,'console',function(...) p(os.date("%x %X"),...) end)
end
validate_config(config)
local flip = Flip:new(config)

logger:info('starting up flip',config)

flip:start()