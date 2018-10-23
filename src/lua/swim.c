/*
 * Copyright 2010-2018, Tarantool AUTHORS, please see AUTHORS file.
 *
 * Redistribution and use in source and binary forms, with or
 * without modification, are permitted provided that the following
 * conditions are met:
 *
 * 1. Redistributions of source code must retain the above
 *    copyright notice, this list of conditions and the
 *    following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above
 *    copyright notice, this list of conditions and the following
 *    disclaimer in the documentation and/or other materials
 *    provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY <COPYRIGHT HOLDER> ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
 * TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
 * <COPYRIGHT HOLDER> OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
 * BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF
 * THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

#include "utils.h"
#include "diag.h"
#include "swim/swim.h"
#include "small/ibuf.h"
#include "lua/info.h"
#include <info.h>

/** SWIM instances are pushed as cdata with this id. */
uint32_t CTID_STRUCT_SWIM_PTR;

/**
 * Get @a n-th value from a Lua stack as a struct swim pointer.
 * @param L Lua state.
 * @param n Where pointer is stored on Lua stack.
 *
 * @retval NULL The stack position does not exist or it is not a
 *         struct swim pointer.
 * @retval not NULL Valid SWIM pointer.
 */
static inline struct swim *
lua_swim_ptr(struct lua_State *L, int n)
{
	uint32_t ctypeid;
	if (lua_type(L, n) != LUA_TCDATA)
		return NULL;
	void *swim = luaL_checkcdata(L, n, &ctypeid);
	if (ctypeid != CTID_STRUCT_SWIM_PTR)
		return NULL;
	return *(struct swim **) swim;
}

/**
 * Delete SWIM instance passed via first Lua stack position. Used
 * by Lua GC.
 */
static int
lua_swim_gc(struct lua_State *L)
{
	struct swim *swim = lua_swim_ptr(L, 1);
	if (swim == NULL)
		return luaL_error(L, "SWIM gc expected struct swim *");
	swim_delete(swim);
	return 0;
}

/**
 * Configure @a swim instance using a table stored in @a ncfg-th
 * position on Lua stack.
 * @param L Lua state.
 * @param ncfg Where configuration is stored on Lua stack.
 * @param swim SWIM instance to configure.
 * @param funcname Caller function name to use in error messages.
 *
 * @retval 0 Success.
 * @retval -1 Error, stored in diagnostics area. Critical errors
 *         like OOM or incorrect usage can throw.
 */
static int
lua_swim_cfg_impl(struct lua_State *L, int ncfg, struct swim *swim,
		  const char *funcname)
{
	if (! lua_istable(L, ncfg)) {
		return luaL_error(L, "swim.%s: expected table config",
				  funcname);
	}

	const char *server_uri;
	lua_getfield(L, ncfg, "server");
	if (lua_isstring(L, -1)) {
		server_uri = lua_tostring(L, -1);
	} else {
		return luaL_error(L, "swim.%s: server should be string URI",
				  funcname);
	}
	lua_pop(L, 1);

	double heartbeat_rate;
	lua_getfield(L, ncfg, "heartbeat");
	if (lua_isnumber(L, -1)) {
		heartbeat_rate = lua_tonumber(L, -1);
		if (heartbeat_rate <= 0) {
			return luaL_error(L, "swim.%s: heartbeat should be "\
					  "positive number", funcname);
		}
	} else if (! lua_isnil(L, -1)) {
		return luaL_error(L, "swim.%s: heartbeat should be positive "\
				  "number", funcname);
	} else {
		heartbeat_rate = -1;
	}
	lua_pop(L, 1);

	return swim_cfg(swim, server_uri, heartbeat_rate, NULL);
}

static int
lua_swim_new(struct lua_State *L)
{
	int top = lua_gettop(L);
	if (top > 1)
		return luaL_error(L, "Usage: swim.new([{<config>}]");
	struct swim *swim = swim_new();
	if (swim != NULL) {
		*(struct swim **)luaL_pushcdata(L, CTID_STRUCT_SWIM_PTR) = swim;
		lua_pushcfunction(L, lua_swim_gc);
		luaL_setcdatagc(L, -2);
		if (top == 0 || lua_swim_cfg_impl(L, 1, swim, "new") == 0) 
			return 1;
		lua_pop(L, 1);
	}
	lua_pushnil(L);
	luaT_pusherror(L, diag_last_error(diag_get()));
	return 2;
}

static int
lua_swim_cfg(struct lua_State *L)
{
	struct swim *swim = lua_swim_ptr(L, 1);
	if (swim == NULL)
		return luaL_error(L, "Usage: swim:cfg({<config>})");
	if (lua_swim_cfg_impl(L, 2, swim, "cfg") != 0) {
		lua_pushnil(L);
		luaT_pusherror(L, diag_last_error(diag_get()));
		return 2;
	}
	lua_pushboolean(L, true);
	return 1;
}

static inline int
lua_swim_add_remove_member(struct lua_State *L, const char *funcname,
			   int (*action)(struct swim *, const char *))
{
	struct swim *swim = lua_swim_ptr(L, 1);
	if (lua_gettop(L) != 2 || swim == NULL)
		return luaL_error(L, "Usage: swim:%s(uri)", funcname);
	const char *member_uri;
	if (lua_isstring(L, -1)) {
		member_uri = lua_tostring(L, 1);
	} else {
		return luaL_error(L, "swim.%s: member URI should be array",
				  funcname);
	}

	if (action(swim, member_uri) != 0) {
		lua_pushnil(L);
		luaT_pusherror(L, diag_last_error(diag_get()));
		return 2;
	}
	lua_pushboolean(L, true);
	return 1;
}

static int
lua_swim_add_member(struct lua_State *L)
{
	return lua_swim_add_remove_member(L, "add_member", swim_add_member);
}

static int
lua_swim_remove_member(struct lua_State *L)
{
	return lua_swim_add_remove_member(L, "remove_member",
					  swim_remove_member);
}

static int
lua_swim_delete(struct lua_State *L)
{
	struct swim *swim = lua_swim_ptr(L, 1);
	if (swim == NULL)
		return luaL_error(L, "Usage: swim:delete()");
	swim_delete(swim);
	uint32_t ctypeid;
	struct swim **cdata = (struct swim **) luaL_checkcdata(L, 1, &ctypeid);
	assert(ctypeid == CTID_STRUCT_SWIM_PTR);
	*cdata = NULL;
	return 0;
}

static int
lua_swim_info(struct lua_State *L)
{
	struct swim *swim = lua_swim_ptr(L, 1);
	if (swim == NULL)
		return luaL_error(L, "Usage: swim:info()");
	struct info_handler info;
	luaT_info_handler_create(&info, L);
	swim_info(swim, &info);
	return 1;
}

void
tarantool_lua_swim_init(struct lua_State *L)
{
	static const struct luaL_Reg lua_swim_methods [] = {
		{"new", lua_swim_new},
		{"cfg", lua_swim_cfg},
		{"add_member", lua_swim_add_member},
		{"remove_member", lua_swim_remove_member},
		{"delete", lua_swim_delete},
		{"info", lua_swim_info},
		{NULL, NULL}
	};
	luaL_register_module(L, "swim", lua_swim_methods);
	lua_pop(L, 1);
	int rc = luaL_cdef(L, "struct swim;");
	assert(rc == 0);
	(void) rc;
	CTID_STRUCT_SWIM_PTR = luaL_ctypeid(L, "struct swim *");
	assert(CTID_STRUCT_SWIM_PTR != 0);
};
