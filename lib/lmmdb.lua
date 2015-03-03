-- -*- mode: lua; tab-width: 2; indent-tabs-mode: 1; st-rulers: [70] -*-
-- vim: ts=4 sw=4 ft=lua noet
---------------------------------------------------------------------
-- @author Daniel Barney <daniel@pagodabox.com>
-- @copyright 2014, Pagoda Box, Inc.
-- @doc
--
-- @end
-- Created :   6 Feb 2015 by Daniel Barney <daniel@pagodabox.com>
---------------------------------------------------------------------

local ffi = require("ffi")

local fs = require("fs")
local raw = require('luvi').bundle.readfile('extention/mdb/libraries/liblmdb/liblmdb.so')

fs.writeFileSync('/tmp/mdb.so',raw)
local lmdb = ffi.load('/tmp/mdb.so')

ffi.cdef[[
char* 	mdb_version (int* major, int* minor, int* patch);
char* 	mdb_strerror (int err);
]]


local MDB = {}

function MDB.version()
	local major = ffi.new("int[1]", 0)
	local minor = ffi.new("int[1]", 0)
	local patch = ffi.new("int[1]", 0)
	local version = lmdb.mdb_version(major,minor,patch)
	return ffi.string(version),major[0],minor[0],patch[0]
end

function MDB.error(err)
	if err == 0 then return end
	local num = ffi.new("int", err)
	local res = lmdb.mdb_strerror(num)
	return ffi.string(res)
end

local Env = 
	{MDB_FIXEDMAP   = 0x01
	,MDB_NOSUBDIR   = 0x4000
	,MDB_NOSYNC     = 0x10000
	,MDB_RDONLY     = 0x20000
	,MDB_NOMETASYNC = 0x40000
	,MDB_WRITEMAP   = 0x80000
	,MDB_MAPASYNC   = 0x100000
	,MDB_NOTLS      = 0x200000
	,MDB_NOLOCK     = 0x400000
	,MDB_NORDAHEAD  = 0x800000
	,MDB_NOMEMINIT  = 0x1000000}

ffi.cdef[[
typedef void* MDB_env;
typedef int mdb_mode_t;
typedef int mdb_filehandle_t;
typedef unsigned int 	MDB_dbi;
typedef void* MDB_txn;
typedef void* MDB_cursor;

typedef struct {
	unsigned int 	ms_psize;
	unsigned int 	ms_depth;
	size_t 	ms_branch_pages;
	size_t 	ms_leaf_pages;
	size_t 	ms_overflow_pages;
	size_t 	ms_entries;
} MDB_stat;

typedef struct {
	void * 	me_mapaddr;
	size_t 	me_mapsize;
	size_t 	me_last_pgno;
	size_t 	me_last_txnid;
	unsigned int 	me_maxreaders;
	unsigned int 	me_numreaders;
} MDB_envinfo;

typedef struct {
	size_t 	mv_size;
	void * 	mv_data;
} MDB_val;

typedef int MDB_cursor_op;
]]

local EnvFunctions = 
[[int mdb_env_create (MDB_env *env);
int mdb_env_open (MDB_env env, const char *path, unsigned int flags, mdb_mode_t mode);
int mdb_env_copy (MDB_env env, const char *path);
int mdb_env_stat (MDB_env env, MDB_stat *stat);
int mdb_env_info (MDB_env env, MDB_envinfo *stat);
int mdb_env_sync (MDB_env env, int force);
void mdb_env_close (MDB_env env);
int mdb_env_set_flags (MDB_env env, unsigned int flags, int onoff);
int mdb_env_get_flags (MDB_env env, unsigned int *flags);
int mdb_env_get_path (MDB_env env, const char **path);
int mdb_env_get_fd (MDB_env env, mdb_filehandle_t *fd);
int mdb_env_set_mapsize (MDB_env env, size_t size);
int mdb_env_set_maxreaders (MDB_env env, unsigned int readers);
int mdb_env_get_maxreaders (MDB_env env, unsigned int *readers);
int mdb_env_set_maxdbs (MDB_env env, MDB_dbi dbs);
int mdb_env_get_maxkeysize (MDB_env env);
int mdb_env_set_userctx (MDB_env env, void *ctx);
int mdb_txn_begin (MDB_env env, MDB_txn *parent, unsigned int flags, MDB_txn *txn);
int mdb_reader_check (MDB_env env, int *dead);]]


local TxnFunctions = 
[[MDB_env* mdb_txn_env (MDB_txn txn);
int mdb_txn_commit (MDB_txn txn);
void mdb_txn_abort (MDB_txn txn);
void mdb_txn_reset (MDB_txn txn);
int mdb_txn_renew (MDB_txn txn);
int mdb_stat (MDB_txn txn, MDB_dbi dbi, MDB_stat *stat);
int mdb_dbi_flags (MDB_txn txn, MDB_dbi dbi, unsigned int *flags);
int mdb_drop (MDB_txn txn, MDB_dbi dbi, int del);
int mdb_set_relctx (MDB_txn txn, MDB_dbi dbi, void *ctx);

int mdb_put (MDB_txn txn, MDB_dbi dbi, MDB_val *key, MDB_val *data, unsigned int flags);
int mdb_del (MDB_txn txn, MDB_dbi dbi, MDB_val *key, MDB_val *data);
int mdb_cursor_open (MDB_txn txn, MDB_dbi dbi, MDB_cursor *cursor);
int mdb_cursor_renew (MDB_txn txn, MDB_cursor cursor);]]

local CursorFunctions = 
[[void mdb_cursor_close (MDB_cursor cursor);
MDB_txn* mdb_cursor_txn (MDB_cursor cursor);
MDB_dbi mdb_cursor_dbi (MDB_cursor cursor);
int mdb_cursor_put (MDB_cursor cursor, MDB_val *key, MDB_val *data, unsigned int flags);
int mdb_cursor_del (MDB_cursor cursor, unsigned int flags);
int mdb_cursor_count (MDB_cursor cursor, size_t *countp);]]

local odd_functions = 
[[int mdb_cursor_get (MDB_cursor cursor, MDB_val *key, MDB_val *data, MDB_cursor_op op);
int mdb_get (MDB_txn txn, MDB_dbi dbi, MDB_val *key, MDB_val *data);
void mdb_dbi_close (MDB_env env, MDB_dbi dbi);
int mdb_dbi_open (MDB_txn txn, const char *name, unsigned int flags, MDB_dbi *dbi);]]

local unimplemented = 
[[int mdb_set_compare (MDB_txn txn, MDB_dbi dbi, MDB_cmp_func *cmp);
int mdb_set_dupsort (MDB_txn txn, MDB_dbi dbi, MDB_cmp_func *cmp);
int mdb_set_relfunc (MDB_txn txn, MDB_dbi dbi, MDB_rel_func *rel);
int mdb_reader_list (MDB_env env, MDB_msg_func *func, void *ctx);
int mdb_cmp (MDB_txn txn, MDB_dbi dbi, const MDB_val *a, const MDB_val *b);
int mdb_dcmp (MDB_txn txn, MDB_dbi dbi, const MDB_val *a, const MDB_val *b);]]

ffi.cdef(EnvFunctions)
ffi.cdef(TxnFunctions)
ffi.cdef(CursorFunctions)
ffi.cdef(odd_functions)

function Env.create()
	local pointer = ffi.new("MDB_env[1]",ffi.new("MDB_env",nil))
	local err = lmdb.mdb_env_create(pointer)
	return pointer[0],MDB.error(err)
end

function Env.open(env,path,flags,mode)
	local err = lmdb.mdb_env_open(env,path,flags,mode)
	return MDB.error(err)
end

function Env.copy(env,path)
	local err = lmdb.mdb_env_copy(env,path)
	return MDB.error(err)
end

local MDB_stat
local _MDB_stat = {}
MDB_stat = ffi.metatype("MDB_stat", _MDB_stat)

function Env.stat(env)
	local stat = ffi.new("MDB_stat[1]",MDB_stat())
	local err = lmdb.mdb_env_stat(env,stat)
	return stat[0],MDB.error(err)
end

local MDB_envinfo
local _MDB_envinfo = {}
MDB_envinfo = ffi.metatype("MDB_envinfo", _MDB_envinfo)

function Env.info(env)
	local stat = ffi.new("MDB_envinfo[1]",MDB_envinfo())
	local err = lmdb.mdb_env_info(env,stat)
	return stat[0],MDB.error(err)
end

function Env.sync(env,force)
	if force then
		force = 1
	else
		force = 0
	end
	local err = lmdb.mdb_env_sync(env,force)
	return MDB.error(err)
end

function Env.close(env)
	lmdb.mdb_env_close(env)
end

function Env.set_flags(env,flags,onoff)
	if onoff then
		onoff = 1
	else
		onoff = 0
	end
	local err = lmdb.mdb_env_set_flags(env,flags,onoff)
	return MDB.error(err)
end

function Env.get_flags(env)
	local flags = ffi.new("unsigned int[1]",0)
	local err = lmdb.mdb_env_get_flags(env,flags)
	return flags[0],MDB.error(err)
end

function Env.get_path(env)
	local path = ffi.new("char*[1]",ffi.new("char*"))
	local err = lmdb.mdb_env_get_path(env,ffi.cast("const char**",path))
	return ffi.string(path[0]),MDB.error(err)
end

function Env.get_fd(env)
	local fd = ffi.new("mdb_filehandle_t[1]")
	local err = lmdb.mdb_env_get_fd(env,fd)
	return fd[0],MDB.error(err)
end

function Env.set_mapsize(env,size)
	local err = lmdb.mdb_env_set_mapsize(env,size)
	return MDB.error(err)
end

function Env.set_maxreaders(env,readers)
	local err = lmdb.mdb_env_set_maxreaders(env,readers)
	return MDB.error(err)
end

function Env.get_maxreaders(env)
	local readers = ffi.new("unsigned int[1]")
	local err = lmdb.mdb_env_get_maxreaders(env,readers)
	return readers[0],MDB.error(err)
end

function Env.set_maxdbs(env,max_dbs)
	local err = lmdb.mdb_env_set_maxdbs(env,max_dbs)
	return MDB.error(err)
end

function Env.get_maxkeysize(env)
	local err = lmdb.mdb_env_get_maxkeysize(env)
	return MDB.error(err)
end

-- function Env.get_context(env)
-- 	return lmdb.mdb_env_get_userctx(env)
-- end

-- function Env.set_context(env,context)
-- 	local err = lmdb.mdb_env_set_userctx(env,context)
-- 	return MDB.error(err)
-- end

function Env.reader_check(env)
	local dead = ffi.new("unsigned int[1]")
	local err = lmdb.mdb_reader_check(env,dead)
	return dead[0],MDB.error(err)
end

function Env.txn_begin(env,parent,flags)
	local txn = ffi.new("MDB_txn[1]",ffi.new("MDB_txn"))
	local err = lmdb.mdb_txn_begin(env,parent,flags,txn)
	return txn[0],MDB.error(err)
end

local Txn = 
	{MDB_RDONLY      = 0x20000
	,MDB_NOOVERWRITE = 0x10
	,MDB_NODUPDATA   = 0x20
	,MDB_CURRENT     = 0x40
	,MDB_RESERVE     = 0x10000
	,MDB_APPEND      = 0x20000
	,MDB_APPENDDUP   = 0x40000
	,MDB_MULTIPLE    = 0x80000}

function Txn.env(txn)
	return lmdb.mdb_txn_env(txn)
end

function Txn.commit(txn)
	return MDB.error(lmdb.mdb_txn_commit(txn))
end

function Txn.abort(txn)
	lmdb.mdb_txn_abort(txn)
end

function Txn.reset(txn)
	lmdb.mdb_txn_reset(txn)
end

function Txn.renew(txn)
	return MDB.error(lmdb.mdb_txn_renew(txn))
end

function Txn.put(txn,dbi,key,data,flags)
	local index = build_MDB_val(key)
	local value = build_MDB_val(data)
	local err = lmdb.mdb_put(txn,dbi,index,value,flags)
	return MDB.error(err)
end

function Txn.get(txn,dbi,key,cast)
	local value = ffi.new("MDB_val[1]")

	local lookup = build_MDB_val(key)
	local err = lmdb.mdb_get(txn,dbi,lookup,value)
	local string
	if err == 0 then
		string = build_return(value,cast)
	end
	return string,MDB.error(err)
end

function Txn.del(txn,dbi,key,data)
	local index = build_MDB_val(key)
	local value
	if data then
		value = build_MDB_val(data)
	end

	local err = lmdb.mdb_del(txn,dbi,index,value)
	return MDB.error(err)
end

local DB = 
	{MDB_REVERSEKEY = 0x02
	,MDB_DUPSORT =    0x04
	,MDB_INTEGERKEY = 0x08
	,MDB_DUPFIXED =   0x10
	,MDB_INTEGERDUP = 0x20
	,MDB_REVERSEDUP = 0x40
	,MDB_CREATE =     0x40000}

function DB.open(txn,name,flags)

	local index = ffi.new("MDB_dbi[1]")
	local err = lmdb.mdb_dbi_open(txn,name,flags,index)
	return index[0],MDB.error(err)
end

function DB.close(txn,dbi)
	lmdb.mdb_dbi_close(txn,dbi)
end

function DB.flags(txn,dbi)
	local flags = ffi.new("unsigned int[1]")
	local err = lmdb.mdb_dbi_flags(txn,dbi,flags)
	return flags[0],MDB.error(err)
end

function DB.stat(txn,dbi)
	local stat = ffi.new("MDB_stat[1]")
	local err = lmdb.mdb_stat(txn,dbi,stat)
	return stat[0],MDB.error(err)
end

function DB.drop(txn,dbi,del)
	local err = lmdb.mdb_drop(txn,dbi,del)
	return MDB.error(err)
end

local Cursor = 
	{MDB_FIRST          = 0
	,MDB_FIRST_DUP      = 1
	,MDB_GET_BOTH       = 2
	,MDB_GET_BOTH_RANGE = 3
	,MDB_GET_CURRENT    = 4
	,MDB_GET_MULTIPLE   = 5
	,MDB_LAST           = 6
	,MDB_LAST_DUP       = 7
	,MDB_NEXT           = 8
	,MDB_NEXT_DUP       = 9
	,MDB_NEXT_MULTIPLE  = 10
	,MDB_NEXT_NODUP     = 11
	,MDB_PREV           = 12
	,MDB_PREV_DUP       = 13
	,MDB_PREV_NODUP     = 14
	,MDB_SET            = 15
	,MDB_SET_KEY        = 16
	,MDB_SET_RANGE      = 17}

function Cursor.open(txn,dbi)
	local cursor = ffi.new("MDB_cursor[1]",ffi.new("MDB_cursor"))
	local err = lmdb.mdb_cursor_open(txn,dbi,cursor)
	return cursor[0],MDB.error(err)
end

function Cursor.close(cursor)
	lmdb.mdb_cursor_close(cursor)
end

function Cursor.get(cursor,key,op,icast,cast)
	local index = build_MDB_val(key)
	local value = ffi.new("MDB_val[1]")
	value[0].mv_data = ffi.cast("void*",nil)
	value[0].mv_size = 0
	local err = lmdb.mdb_cursor_get(cursor,index,value,op)
	if not (err == 0) then
		return nil,nil,MDB.error(err)
	else
		index = build_return(index,icast)
		value = build_return(value,cast)
		return index,value,MDB.error(err)
	end
end

function build_return(value,cast)
	if cast then
		if not (ffi.sizeof(cast) == value[0].mv_size) then
			p("cast looses precision",cast,ffi.sizeof(cast),value[0].mv_size)
		end
		return ffi.cast(cast,value[0].mv_data)
	elseif value then
		return ffi.string(value[0].mv_data,value[0].mv_size)
	end
end

function build_MDB_val(elem)
	local value = ffi.new("MDB_val[1]")
	if type(elem) == "cdata" then
		value[0].mv_data = ffi.cast("void*",elem)
		value[0].mv_size = ffi.sizeof(elem)
	elseif type(elem) == "number" then
		value[0].mv_data = ffi.cast("void*",ffi.new("long[1]",elem))
		value[0].mv_size = ffi.sizeof("long")
	else
		value[0].mv_data = ffi.cast("void*",elem)
		if elem then
			value[0].mv_size = #elem
		else
			value[0].mv_size = 0
		end
	end
	return value
end

p("using Lightning Memory Mapped Database",MDB.version())

return {Env = Env,DB = DB, Txn = Txn,Cursor = Cursor}