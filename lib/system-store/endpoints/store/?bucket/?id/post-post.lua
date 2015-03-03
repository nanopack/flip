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
	logger:info("post",req.env.bucket,req.env.id)
	local chunks = {}
	
	req:on('data',function(chunk)
		chunks[#chunks + 1] = chunk
	end)
	
	req:on('end',function()

		local success,data = xpcall(function() return JSON.parse(table.concat(chunks)) end,function(err)
			res:writeHead(400,{})
			res:finish('{"error":"bad json"}')
		end)
		

		if success then
			local object,err
			local nodes = req.headers['x-commit-nodes']
			if nodes == 'all' then
				nodes = 0
			elseif nodes then
				nodes = tonumber(nodes)
			else
				nodes = 1
			end

			local done = false

			local cb = function(current,total)
				if done then 
					return
				elseif ((nodes < 1 and nodes > 0) and (current/total >= nodes)) then
					done = true
					res:writeHead(201,{})
					res:finish(JSON.stringify(object))
				elseif (nodes >= 1) and (current >= nodes) then
					done = true
					res:writeHead(201,{})
					res:finish(JSON.stringify(object))
				elseif current == total then
					done = true
					res:writeHead(201,{})
					res:finish(JSON.stringify(object))
				end
			end

			object,err = store:store(req.env.bucket,req.env.id,data,cb)
			
			if err then
				local code = error_code(err)
				res:writeHead(code,{})
				res:finish(JSON.stringify({error = err}))
			end
		end
	end)
end