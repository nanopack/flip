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

-- this script will take a folder of scripts and load them into flip
-- as a new system, or update an existing system.

local fs = require('fs')
local string = require('string')

-- not my favorite work around
local luvi = require('luvi')
local bundle = nil
if luvi then
 bundle = require('luvi').bundle
end

function read_dir(directory)
	logger:info("loading bundle",store.loading)
	if store.loading then
		logger:info(bundle.readdir(directory))
		return true,bundle.readdir(directory)
	else
		return pcall(function() return fs.readdirSync(directory) end)
	end
end

function read_file(path)
	if store.loading then
			return true,bundle.readfile(path)
	else
		return pcall(function() return fs.readFileSync(path) end)
	end
end


function load_routes(directory,route,routes,scripts)

	local is_dir,files = read_dir(directory)
	if is_dir then
		for _idx,file in pairs(files) do
			local path = directory .. "/" .. file
			if path:sub(-4) == ".lua" then
				local success,res = read_file(path)
				if success then
					logger:info("loading file",path)
					local fn, err = loadstring(res,path)
					if err then
						logger:error(err)
						process:exit(1)
					end
					local match = {}
					for elem in string.gmatch(file, "[^-]+") do
					  match[#match + 1] = elem
					end
					local method = match[1]
					if not routes[method] then
						routes[method] = {}
					end

					local key = file:sub(1,-5)
					if scripts[key] then
						logger:warning("route conflict, using previously loaded script",scripts[key])
					else
						scripts[key] = {["$script"] = res}	
					end
					routes[method][route] = key
				end
			else
				logger:info("loading folder?",path)
				load_routes(path,route .. "/" .. file,routes,scripts)
			end
		end
	end
	return routes,scripts
end

function load_dir(directory,scripts,is_root)
	local opts = {}

	local is_dir,files = read_dir(directory)
	if is_dir then
		for _idx,file in pairs(files) do
			local path = directory .. "/" .. file
			
			
			logger:debug("reading file",path)
			if path:sub(-4) == ".txt" then
				local success,res = read_file(path)
				if success then
					local name = file:sub(1,-5)
					opts[name] = res
				end
			elseif path:sub(-4) == ".lua" then
				local name = file:sub(1,-5)
				local success,res = read_file(path)
				if success then
					logger:debug("loading file",path)
					local fn, err = loadstring(res,path)
					if err then
						logger:error(err)
						process:exit(1)
					end
					
					if scripts[name] then
						logger:info("name conflict",opts[name])
					else
						scripts[name] = {["$script"] = res}	
					end

					opts[name] = name
				end
			elseif is_root and file == "endpoints" then
				logger:info("loading endpoints")
				opts.endpoints = load_routes(path,"",{},scripts)
			elseif path then
				logger:info("loading folder?",path)
				opts[file] = load_dir(path,scripts,false)
			end

		end

		return opts,scripts
	end
end

return function(directory,system_name)
	logger:info("starting to read from",directory,system_name,store.loading)
	local system,scripts = load_dir(directory,{},true)
	logger:info("was able to build system")
	if system then

		-- local success,description = pcall(function() return fs.readFileSync(directory .. "/description.txt") end)
		-- local success,help = pcall(function() return fs.readFileSync(directory .. "/help.txt") end)
		-- system.description = description
		-- system.help = help
		

		local object,err = store:fetch("systems",system_name)
		if err and not (err == "MDB_NOTFOUND: No matching key/data pair found") then
			logger:error("unable to load system into flip",err)
			process:exit(1)
		elseif err then
			system.bucket = "systems"
			system.id = system_name
		else
			if object then
				for key,value in pairs(object) do
					if system[key] == nil then
						system[key] = value
					end
				end
			end
		end
		logger:info("going to load scripts into system",system.id)
		for id,script in pairs(scripts) do
			local obj,err = store:fetch(system.id .. '-scripts',id)
			if obj then
				script.last_updated = obj.last_updated
			end
			local obj,err = store:store(system.id .. '-scripts',id,script)
			if err then
				logger:info(err)
			end
		end
		logger:info("going to load new system in",system.id)
		local object,err = store:store(system.bucket,system.id,system)
		if err then
			logger:info(err)
		end
	else
		logger:error("unable to load system")
	end
end