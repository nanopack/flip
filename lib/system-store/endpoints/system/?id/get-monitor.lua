-- -*- mode: lua; tab-width: 2; indent-tabs-mode: 1; st-rulers: [70] -*-
-- vim: ts=4 sw=4 ft=lua noet
---------------------------------------------------------------------
-- @author Daniel Barney <daniel@pagodabox.com>
-- @copyright 2014, Pagoda Box, Inc.
-- @doc
--
-- @end
-- Created :   25 Feb 2015 by Daniel Barney <daniel@pagodabox.com>
---------------------------------------------------------------------

return function(req,res)
	logger:info("listening to system changes for",req.env.id)
	local status = system:status(req.env.id)
	if status then
		res:writeHead(200,{})
		local bounce = function(id,add,remove)
			logger:info("bouncing",id,add,remove)
			local data = 
				{id = id
				,add = add
				,remove = remove}
			res:write(JSON.stringify(data))
		end
		if req.env.id then
			system:on('change:' .. req.env.id,bounce)
		else
			system:on('change:',bounce)
		end
		logger:info("sending",status)
		res:write(JSON.stringify(status))
		req:once('done',function() 
			if req.env.id then
				system:removeListener('change:',bounce)
			else
				system:removeListener('change:' .. req.env.id,bounce)
			end 
			
		end)
	else
		res:writeHead(404,{})
		res:finish(JSON.stringify({error = "not found"}))
	end
end