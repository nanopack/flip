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

return function(req,res)
	-- this needs to be a better check
	if string.find(req.url,"[?]stream") then
		logger:info("streaming",req.env.bucket,req.env.id)
		local object,err = store:fetch(req.env.bucket,req.env.id)

		local bounce = function(type,id,data) 
			logger:info("bouncing",type,id,data)
			res:write(JSON.stringify({type = type, id = id, data = data}))
		end
		if req.env.id then
			local key = req.env.bucket .. ':' .. req.env.id
			logger:info("subscribing to",key)
			store:on(key,bounce)
			req:on('end',function() 
				store:removeListener(bounce)
				res:finish()
			end)
		else
			local key = req.env.bucket .. ':'
			logger:info("subscribing to",key)
			store:on(key,bounce)
			req:on('end',function() 
				store:removeListener(bounce)
				res:finish()
			end)
		end
		res:writeHead(200,{})

		if req.env.id and object then
			logger:info("sending back",object)
			res:write(JSON.stringify({type = "store", id = object.id, data = object}))
		end
	else
		logger:info("fetch",req.env.bucket,req.env.id)
		local object,err = store:fetch(req.env.bucket,req.env.id)
		if err then
			local code = error_code(err)
			res:writeHead(code,{})
			res:finish(JSON.stringify({error = err}))
		else
			res:writeHead(200,{})
			if req.env.id then
				object.script = nil
			else
				for _,obj in pairs(object) do 
					obj.script = nil
				end
			end
			res:finish(JSON.stringify(object))
		end
	end
end