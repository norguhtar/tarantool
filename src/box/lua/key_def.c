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

#include "box/lua/key_def.h"

#include <lua.h>
#include <lauxlib.h>
#include "diag.h"
#include "box/key_def.h"
#include "box/box.h"
#include "box/coll_id_cache.h"
#include "lua/utils.h"

struct key_def *
luaT_new_key_def(struct lua_State *L, int idx)
{
	if (lua_istable(L, idx) != 1) {
		luaL_error(L, "Bad params, use: luaT_new_key_def({"
				  "{fieldno = fieldno, type = type"
				  "[, is_nullable = is_nullable"
				  "[, collation_id = collation_id"
				  "[, collation = collation]]]}, ...}");
		unreachable();
		return NULL;
	}
	uint32_t key_parts_count = 0;
	uint32_t capacity = 8;

	const ssize_t parts_size = sizeof(struct key_part_def) * capacity;
	struct key_part_def *parts = NULL;
	parts = (struct key_part_def *) malloc(parts_size);
	if (parts == NULL) {
		diag_set(OutOfMemory, parts_size, "malloc", "parts");
		luaT_error(L);
		unreachable();
		return NULL;
	}

	while (true) {
		lua_pushinteger(L, key_parts_count + 1);
		lua_gettable(L, idx);
		if (lua_isnil(L, -1))
			break;

		/* Extend parts if necessary. */
		if (key_parts_count == capacity) {
			capacity *= 2;
			struct key_part_def *old_parts = parts;
			const ssize_t parts_size =
				sizeof(struct key_part_def) * capacity;
			parts = (struct key_part_def *) realloc(parts,
								parts_size);
			if (parts == NULL) {
				free(old_parts);
				diag_set(OutOfMemory, parts_size / 2, "malloc",
					 "parts");
				luaT_error(L);
				unreachable();
				return NULL;
			}
		}

		/* Set parts[key_parts_count].fieldno. */
		lua_pushstring(L, "fieldno");
		lua_gettable(L, -2);
		if (lua_isnil(L, -1)) {
			free(parts);
			luaL_error(L, "fieldno must not be nil");
			unreachable();
			return NULL;
		}
		/*
		 * Transform one-based Lua fieldno to zero-based
		 * fieldno to use in key_def_new().
		 */
		parts[key_parts_count].fieldno = lua_tointeger(L, -1) - 1;
		lua_pop(L, 1);

		/* Set parts[key_parts_count].type. */
		lua_pushstring(L, "type");
		lua_gettable(L, -2);
		if (lua_isnil(L, -1)) {
			free(parts);
			luaL_error(L, "type must not be nil");
			unreachable();
			return NULL;
		}
		size_t type_len;
		const char *type_name = lua_tolstring(L, -1, &type_len);
		lua_pop(L, 1);
		parts[key_parts_count].type = field_type_by_name(type_name,
								 type_len);
		if (parts[key_parts_count].type == field_type_MAX) {
			free(parts);
			luaL_error(L, "Unknown field type: %s", type_name);
			unreachable();
			return NULL;
		}

		/*
		 * Set parts[key_parts_count].is_nullable and
		 * parts[key_parts_count].nullable_action.
		 */
		lua_pushstring(L, "is_nullable");
		lua_gettable(L, -2);
		if (lua_isnil(L, -1)) {
			parts[key_parts_count].is_nullable = false;
			parts[key_parts_count].nullable_action =
				ON_CONFLICT_ACTION_DEFAULT;
		} else {
			parts[key_parts_count].is_nullable =
				lua_toboolean(L, -1);
			parts[key_parts_count].nullable_action =
				ON_CONFLICT_ACTION_NONE;
		}
		lua_pop(L, 1);

		/* Set parts[key_parts_count].coll_id using collation_id. */
		lua_pushstring(L, "collation_id");
		lua_gettable(L, -2);
		if (lua_isnil(L, -1))
			parts[key_parts_count].coll_id = COLL_NONE;
		else
			parts[key_parts_count].coll_id = lua_tointeger(L, -1);
		lua_pop(L, 1);

		/* Set parts[key_parts_count].coll_id using collation. */
		lua_pushstring(L, "collation");
		lua_gettable(L, -2);
		/* Check whether box.cfg{} was called. */
		if ((parts[key_parts_count].coll_id != COLL_NONE ||
		    !lua_isnil(L, -1)) && !box_is_configured()) {
			free(parts);
			luaL_error(L, "Cannot use collations: "
				      "please call box.cfg{}");
			unreachable();
			return NULL;
		}
		if (!lua_isnil(L, -1)) {
			if (parts[key_parts_count].coll_id != COLL_NONE) {
				free(parts);
				luaL_error(L, "Conflicting options: "
					      "collation_id and collation");
				unreachable();
				return NULL;
			}
			size_t coll_name_len;
			const char *coll_name = lua_tolstring(L, -1,
							      &coll_name_len);
			struct coll_id *coll_id = coll_by_name(coll_name,
							       coll_name_len);
			if (coll_id == NULL) {
				free(parts);
				luaL_error(L, "Unknown collation: \"%s\"",
					   coll_name);
				unreachable();
				return NULL;
			}
			parts[key_parts_count].coll_id = coll_id->id;
		}
		lua_pop(L, 1);

		/* Check coll_id. */
		struct coll_id *coll_id =
			coll_by_id(parts[key_parts_count].coll_id);
		if (parts[key_parts_count].coll_id != COLL_NONE &&
		    coll_id == NULL) {
			uint32_t collation_id = parts[key_parts_count].coll_id;
			free(parts);
			luaL_error(L, "Unknown collation_id: %d", collation_id);
			unreachable();
			return NULL;
		}

		/* Set parts[key_parts_count].sort_order. */
		parts[key_parts_count].sort_order = SORT_ORDER_ASC;

		++key_parts_count;
	}

	struct key_def *key_def = key_def_new(parts, key_parts_count);
	free(parts);
	if (key_def == NULL) {
		luaL_error(L, "Cannot create key_def");
		unreachable();
		return NULL;
	}
	return key_def;
}
