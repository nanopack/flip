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
-- Lua implementation of Jump Consistant Hash. 
-- http://arxiv.org/ftp/arxiv/papers/1406/1406.2294.pdf

local bit = require('bit')
local math = require('math')

local jch = function(key,num_buckets)
	local bucket = 0
	local i = 0
	while  i < num_buckets do
		bucket = i
		key = (key * 2862933555777941757) + 1
		i = math.floor((bucket + 1) * (2147483648 / (bit.rshift(bit.rshift(key,32),1) + 1)))
	end
	return bucket;
end

return jch;