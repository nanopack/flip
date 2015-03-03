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
local luvi = require('luvi')
luvi.bundle.register('require', "deps/require.lua")
local require = require('require')("bundle:main.lua")

-- Create a luvit powered main that does the luvit CLI interface
return require('luvit')(function (...)

	local logger = require('./lib/logger')
	local os = require('os')

	function main()
		if process.argv[1] == '-server' then
			logger:add_logger('info','console',function(...) p(os.date("%x %X"),...) end)
			logger:info("starting server")
			if #process.argv == 3 then
				table.remove(process.argv,1)
				require('./lib/flip_server')
			else
				logger:info("Usage: flip -server (-config-file|-config-json) {path|json}")
			end
		else
			logger:add_logger('info','console',function(...) p(...) end)
			logger:debug("entering cli mode")
			require('./lib/flip_cli')
		end
	end
	main()
end)