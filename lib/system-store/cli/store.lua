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

-- this will eventually be run by the cli when trying to add
-- something to the store
JSON = require('json')
fs = require('fs')

return function(bucket,id,data,file)
	logger:info("I am trying to add something to the cluster",bucket,id,data)

	local object,err = nil,nil

	logger:info("check",data:sub(1,1))
	if data:sub(1,1) == '-' and file then
		local contents = fs.readFileSync(file)
		if data == '-file' then
			object,err = store:store(bucket,id,JSON.parse(contents))
		elseif data == '-script' then
			local fn,failed = loadstring(contents,'@store/bucket:' .. bucket .. '/script:' .. id)
			if failed then
				err = failed
			else
				object,err = store:store(bucket,id,{["$script"] = contents})
			end
		end
	elseif data then
		object,err = store:store(bucket,id,JSON.parse(data))
	else
		logger:fatal("unable to store nothing")
	end
	logger:info("got results",object,err)
end