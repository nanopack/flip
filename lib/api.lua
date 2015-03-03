-- -*- mode: lua; tab-width: 2; indent-tabs-mode: 1; st-rulers: [70] -*-
-- vim: ts=4 sw=4 ft=lua noet
---------------------------------------------------------------------
-- @author Daniel Barney <daniel@pagodabox.com>
-- @copyright 2014, Pagoda Box, Inc.
-- @doc
--
-- @end
-- Created :   20 Nov 2014 by Daniel Barney <daniel@pagodabox.com>
---------------------------------------------------------------------

local Emitter = require('core').Emitter
local hrtime = require('uv').hrtime
local logger = require('./logger')
local JSON = require('json')
local Lever = require('lever/main')
local utils = require('utils')
local table = require('table')

local Readable = Lever.Stream.Readable
local Start = Readable:extend()

function Start:initialize()
  Readable.initialize(self,{objectMode = true})
end

function Start:_read() end

local Api = Emitter:extend()

function Api:initialize(flip,port,ip)
	self.flip = flip
	self.lever = Lever:new(port,ip)
	self.status = Start:new()

	-- express routes
	self.lever:get('/cluster'
		,function(req,res) self:node_status(req,res) end)

	-- piped routes

	-- subscribe routes
	self.status
		:pipe(self.lever.json())
		:pipe(self.lever:get('/cluster/stream'))

end

function Api:node_status(req,res)
	local data = {}
	for id,node in pairs(self.flip.servers) do
		
		data[#data + 1] = 
			{id = node.id
			,state = node.state
			,systems = node.systems
			,opts = node.opts}
	end
	res:writeHead(200,{})
	local time = hrtime()
	self.status:push({time = time})
  res:finish(JSON.stringify({time = time,data = data}))
end


function Api:system_status(req,res)
	local data = {}
	for id,node in pairs(self.flip.systems) do
		
		data[#data + 1] = 
			{id = node.id
			,state = node.state
			,opts = node.opts}
	end
	res:writeHead(200,{})
  res:finish(JSON.stringify(data))
end



function Api:error_code(err)
	if (err ~= "MDB_NOTFOUND") then
		return 404
	else
		return 500
	end
end

return Api