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
	-- add_default adds default values
	local add_default = function(obj,name,value)
		if obj[name] == nil then
			obj[name] = value
		end
	end

	add_default(config,'id','flip')
	add_default(config,'gossip',{})
	add_default(config.gossip,'ip','127.0.0.1')
	add_default(config.gossip,'port',2000)
	add_default(config,'replication',{})
	add_default(config.replication,'ip','127.0.0.1')
	add_default(config.replication,'port',2001)
	add_default(config,'api',{})
	add_default(config.api,'ip','127.0.0.1')
	add_default(config.api,'port',2345)
	add_default(config,'db','./db')
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