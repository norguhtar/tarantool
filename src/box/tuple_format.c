/*
 * Copyright 2010-2016, Tarantool AUTHORS, please see AUTHORS file.
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
#include <sys/uio.h>
#include "bit/bit.h"
#include "fiber.h"
#include "json/json.h"
#include "tuple_format.h"
#include "coll_id_cache.h"

/** Global table of tuple formats */
struct tuple_format **tuple_formats;
static intptr_t recycled_format_ids = FORMAT_ID_NIL;

static uint32_t formats_size = 0, formats_capacity = 0;

static struct tuple_field *
tuple_field_new(void)
{
	struct tuple_field *field = calloc(1, sizeof(struct tuple_field));
	if (field == NULL) {
		diag_set(OutOfMemory, sizeof(struct tuple_field), "malloc",
			 "tuple field");
		return NULL;
	}
	field->id = UINT32_MAX;
	field->token.type = JSON_TOKEN_END;
	field->type = FIELD_TYPE_ANY;
	field->offset_slot = TUPLE_OFFSET_SLOT_NIL;
	field->coll_id = COLL_NONE;
	field->nullable_action = ON_CONFLICT_ACTION_NONE;
	return field;
}

static void
tuple_field_delete(struct tuple_field *field)
{
	free(field);
}

/** Return path to a tuple field. Used for error reporting. */
static const char *
tuple_field_path(const struct tuple_field *field, bool json_only)
{
	assert(field->token.parent != NULL);
	char *path;
	if (!json_only && field->token.parent->type == JSON_TOKEN_END) {
		assert(field->token.type == JSON_TOKEN_NUM);
		path = int2str(field->token.num + TUPLE_INDEX_BASE);
	} else {
		path = tt_static_buf();
		MAYBE_UNUSED int rc =
			json_tree_snprint_path(path, TT_STATIC_BUF_LEN,
					       &field->token, TUPLE_INDEX_BASE);
		assert(rc > 0 && rc < TT_STATIC_BUF_LEN);
	}
	return path;
}

/**
 * Add corresponding format:fields for specified JSON path.
 * Return a pointer to the leaf field on success, NULL on memory
 * allocation error or type/nullability mistmatch error, diag
 * message is set.
 */
static struct tuple_field *
tuple_field_tree_add_path(struct tuple_format *format, const char *path,
			  uint32_t path_len, uint32_t fieldno)
{
	int rc = 0;
	struct json_tree *tree = &format->fields;
	struct tuple_field *parent = tuple_format_field(format, fieldno);
	struct tuple_field *field = tuple_field_new();
	if (field == NULL)
		goto fail;

	struct json_lexer lexer;
	uint32_t token_count = 0;
	json_lexer_create(&lexer, path, path_len, TUPLE_INDEX_BASE);
	while ((rc = json_lexer_next_token(&lexer, &field->token)) == 0 &&
	       field->token.type != JSON_TOKEN_END) {
		enum field_type expected_type =
			field->token.type == JSON_TOKEN_STR ?
			FIELD_TYPE_MAP : FIELD_TYPE_ARRAY;
		if (field_type1_contains_type2(parent->type, expected_type)) {
			parent->type = expected_type;
		} else if (!field_type1_contains_type2(expected_type,
						       parent->type)) {
			diag_set(ClientError, ER_INDEX_PART_TYPE_MISMATCH,
				 tuple_field_path(parent, false),
				 field_type_strs[parent->type],
				 field_type_strs[expected_type]);
			goto fail;
		}
		struct tuple_field *next =
			json_tree_lookup_entry(tree, &parent->token,
					       &field->token,
					       struct tuple_field, token);
		if (next == NULL) {
			rc = json_tree_add(tree, &parent->token, &field->token);
			if (rc != 0) {
				diag_set(OutOfMemory, sizeof(struct json_token),
					 "json_tree_add", "tree");
				goto fail;
			}
			next = field;
			field = tuple_field_new();
			if (field == NULL)
				goto fail;
		}
		parent = next;
		token_count++;
	}
	/* Path has been verified  key_def_decode_parts. */
	assert(rc == 0 && field->token.type == JSON_TOKEN_END);
	assert(parent != NULL);
	/* Update tree depth information. */
	format->max_path_tokens = MAX(format->max_path_tokens, token_count + 1);
end:
	tuple_field_delete(field);
	return parent;
fail:
	parent = NULL;
	goto end;
}

/**
 * Look up field metadata by identifier.
 *
 * Used only for error reporing so we can afford full field
 * tree traversal here.
 */
static struct tuple_field *
tuple_format_field_by_id(struct tuple_format *format, uint32_t id)
{
	struct tuple_field *field;
	json_tree_foreach_entry_preorder(field, &format->fields.root,
					 struct tuple_field, token) {
		if (field->id == id)
			return field;
	}
	return NULL;
}

static int
tuple_format_use_key_part(struct tuple_format *format, uint32_t field_count,
			  const struct key_part *part, bool is_sequential,
			  int *current_slot, char **paths)
{
	assert(part->fieldno < tuple_format_field_count(format));
	struct tuple_field *field;
	if (part->path == NULL) {
		field = tuple_format_field(format, part->fieldno);
	} else {
		assert(!is_sequential);
		/**
		 * Copy JSON path data to reserved area at the
		 * end of format allocation.
		 */
		memcpy(*paths, part->path, part->path_len);
		field = tuple_field_tree_add_path(format, *paths, part->path_len,
						  part->fieldno);
		if (field == NULL)
			return -1;
		*paths += part->path_len;
	}
	/*
		* If a field is not present in the space format,
		* inherit nullable action of the first key part
		* referencing it.
		*/
	if (part->fieldno >= field_count && !field->is_key_part)
		field->nullable_action = part->nullable_action;
	/*
	 * Field and part nullable actions may differ only
	 * if one of them is DEFAULT, in which case we use
	 * the non-default action *except* the case when
	 * the other one is NONE, in which case we assume
	 * DEFAULT. The latter is needed so that in case
	 * index definition and space format have different
	 * is_nullable flag, we will use the strictest option,
	 * i.e. DEFAULT.
	 */
	if (field->nullable_action == ON_CONFLICT_ACTION_DEFAULT) {
		if (part->nullable_action != ON_CONFLICT_ACTION_NONE)
			field->nullable_action = part->nullable_action;
	} else if (part->nullable_action == ON_CONFLICT_ACTION_DEFAULT) {
		if (field->nullable_action == ON_CONFLICT_ACTION_NONE)
			field->nullable_action = part->nullable_action;
	} else if (field->nullable_action != part->nullable_action) {
		diag_set(ClientError, ER_ACTION_MISMATCH,
			 tuple_field_path(field, false),
			 on_conflict_action_strs[field->nullable_action],
			 on_conflict_action_strs[part->nullable_action]);
		return -1;
	}

	/**
	 * Check that there are no conflicts between index part
	 * types and space fields. If a part type is compatible
	 * with field's one, then the part type is more strict
	 * and the part type must be used in tuple_format.
	 */
	if (field_type1_contains_type2(field->type,
					part->type)) {
		field->type = part->type;
	} else if (!field_type1_contains_type2(part->type,
					       field->type)) {
		int errcode;
		if (!field->is_key_part)
			errcode = ER_FORMAT_MISMATCH_INDEX_PART;
		else
			errcode = ER_INDEX_PART_TYPE_MISMATCH;
		diag_set(ClientError, errcode, tuple_field_path(field, false),
			 field_type_strs[field->type],
			 field_type_strs[part->type]);
		return -1;
	}
	field->is_key_part = true;
	/*
	 * In the tuple, store only offsets necessary to access
	 * fields of non-sequential keys. First field is always
	 * simply accessible, so we don't store an offset for it.
	 */
	if (field->offset_slot == TUPLE_OFFSET_SLOT_NIL &&
	    is_sequential == false &&
	    (part->fieldno > 0 || part->path != NULL)) {
		*current_slot = *current_slot - 1;
		field->offset_slot = *current_slot;
	}
	return 0;
}

/**
 * Get format:field parent field_type.
 * This routine is required as first-level fields has no parent
 * field so it could not be retrieved with json_tree_entry.
 */
static enum field_type
tuple_format_field_parent_type(struct tuple_format *format,
			       struct tuple_field *field)
{
	struct json_token *parent = field->token.parent;
	if (parent == &format->fields.root)
		return FIELD_TYPE_ARRAY;
	return json_tree_entry(parent, struct tuple_field, token)->type;
}

uint32_t
tuple_format_stmt_encode(struct tuple_format *format, char **offset,
			 char *tuple_raw, uint32_t *field_map,
			 struct iovec *iov)
{
	bool write = offset != NULL;
	uint32_t size = 0;
	struct tuple_field *field;
	json_tree_foreach_entry_preorder(field, &format->fields.root,
					 struct tuple_field, token) {
		enum field_type parent_type =
			tuple_format_field_parent_type(format, field);
		if (parent_type == FIELD_TYPE_ARRAY &&
		    field->token.sibling_idx > 0) {
			/*
			 * Write nil istead of omitted array
			 * members.
			 */
			struct json_token **neighbors =
				field->token.parent->children;
			for (uint32_t i = field->token.sibling_idx - 1;
			     neighbors[i] == NULL && i > 0; i--) {
				if (write)
					*offset = mp_encode_nil(*offset);
				size += mp_sizeof_nil();
			}
		} else if (parent_type == FIELD_TYPE_MAP) {
			/* Write map key string. */
			const char *str = field->token.str;
			uint32_t len = field->token.len;
			if (write)
				*offset = mp_encode_str(*offset, str, len);
			size += mp_sizeof_str(len);
		}
		/* Fill data. */
		uint32_t children_cnt = field->token.max_child_idx + 1;
		if (json_token_is_leaf(&field->token)) {
			if (!write || iov[field->id].iov_len == 0) {
				if (write)
					*offset = mp_encode_nil(*offset);
				size += mp_sizeof_nil();
			} else {
				memcpy(*offset, iov[field->id].iov_base,
				       iov[field->id].iov_len);
				uint32_t data_offset = *offset - tuple_raw;
				int32_t slot = field->offset_slot;
				if (slot != TUPLE_OFFSET_SLOT_NIL)
					field_map[slot] = data_offset;
				*offset += iov[field->id].iov_len;
				size += iov[field->id].iov_len;
			}
		} else if (field->type == FIELD_TYPE_ARRAY) {
			if (write)
				*offset = mp_encode_array(*offset, children_cnt);
			size += mp_sizeof_array(children_cnt);
		} else if (field->type == FIELD_TYPE_MAP) {
			if (write)
				*offset = mp_encode_map(*offset, children_cnt);
			size += mp_sizeof_map(children_cnt);
		}
	}
	return size;
}

/**
 * Extract all available type info from keys and field
 * definitions.
 */
static int
tuple_format_create(struct tuple_format *format, struct key_def * const *keys,
		    uint16_t key_count, const struct field_def *fields,
		    uint32_t field_count)
{
	format->min_field_count =
		tuple_format_min_field_count(keys, key_count, fields,
					     field_count);
	if (tuple_format_field_count(format) == 0) {
		format->field_map_size = 0;
		return 0;
	}
	/* Initialize defined fields */
	for (uint32_t i = 0; i < field_count; ++i) {
		struct tuple_field *field = tuple_format_field(format, i);
		field->type = fields[i].type;
		field->nullable_action = fields[i].nullable_action;
		struct coll *coll = NULL;
		uint32_t cid = fields[i].coll_id;
		if (cid != COLL_NONE) {
			struct coll_id *coll_id = coll_by_id(cid);
			if (coll_id == NULL) {
				diag_set(ClientError,ER_WRONG_COLLATION_OPTIONS,
					 i + 1, "collation was not found by ID");
				return -1;
			}
			coll = coll_id->coll;
		}
		field->coll = coll;
		field->coll_id = cid;
	}

	int current_slot = 0;

	/*
	 * Set pointer to reserved area in the format chunk
	 * allocated with tuple_format_alloc call.
	 */
	char *paths = (char *)format + sizeof(struct tuple_format);
	/* extract field type info */
	for (uint16_t key_no = 0; key_no < key_count; ++key_no) {
		const struct key_def *key_def = keys[key_no];
		bool is_sequential = key_def_is_sequential(key_def);
		const struct key_part *part = key_def->parts;
		const struct key_part *parts_end = part + key_def->part_count;

		for (; part < parts_end; part++) {
			if (tuple_format_use_key_part(format, field_count, part,
						      is_sequential,
						      &current_slot,
						      &paths) != 0)
				return -1;
		}
	}

	assert(tuple_format_field(format, 0)->offset_slot ==
	       TUPLE_OFFSET_SLOT_NIL);
	size_t field_map_size = -current_slot * sizeof(uint32_t);
	if (field_map_size > UINT16_MAX) {
		/** tuple->data_offset is 16 bits */
		diag_set(ClientError, ER_INDEX_FIELD_COUNT_LIMIT,
			 -current_slot);
		return -1;
	}
	format->field_map_size = field_map_size;

	size_t required_fields_sz = bitmap_size(format->total_field_count);
	format->required_fields = calloc(1, required_fields_sz);
	if (format->required_fields == NULL) {
		diag_set(OutOfMemory, required_fields_sz,
			 "malloc", "required field bitmap");
		return -1;
	}
	uint32_t id = 0;
	struct tuple_field *field;
	json_tree_foreach_entry_preorder(field, &format->fields.root,
					 struct tuple_field, token) {
		/* Set the unique field identifier. */
		field->id = id++;
		/*
		 * Mark all leaf non-nullable fields as required
		 * by setting the corresponding bit in the bitmap
		 * of required fields.
		 */
		if (json_token_is_leaf(&field->token) &&
		    !tuple_field_is_nullable(field))
			bit_set(format->required_fields, field->id);
	}
	/* Update format metadate for a new format:fields tree. */
	format->total_field_count = id;
	format->vy_stmt_size = tuple_format_stmt_encode(format, NULL, NULL,
							NULL, NULL);
	return 0;
}

static int
tuple_format_register(struct tuple_format *format)
{
	if (recycled_format_ids != FORMAT_ID_NIL) {

		format->id = (uint16_t) recycled_format_ids;
		recycled_format_ids = (intptr_t) tuple_formats[recycled_format_ids];
	} else {
		if (formats_size == formats_capacity) {
			uint32_t new_capacity = formats_capacity ?
						formats_capacity * 2 : 16;
			struct tuple_format **formats;
			formats = (struct tuple_format **)
				realloc(tuple_formats, new_capacity *
						       sizeof(tuple_formats[0]));
			if (formats == NULL) {
				diag_set(OutOfMemory,
					 sizeof(struct tuple_format), "malloc",
					 "tuple_formats");
				return -1;
			}

			formats_capacity = new_capacity;
			tuple_formats = formats;
		}
		if (formats_size == FORMAT_ID_MAX + 1) {
			diag_set(ClientError, ER_TUPLE_FORMAT_LIMIT,
				 (unsigned) formats_capacity);
			return -1;
		}
		format->id = formats_size++;
	}
	tuple_formats[format->id] = format;
	return 0;
}

static void
tuple_format_deregister(struct tuple_format *format)
{
	if (format->id == FORMAT_ID_NIL)
		return;
	tuple_formats[format->id] = (struct tuple_format *) recycled_format_ids;
	recycled_format_ids = format->id;
	format->id = FORMAT_ID_NIL;
}

/*
 * Dismantle the tuple field tree attached to the format and free
 * memory occupied by tuple fields.
 */
static void
tuple_format_destroy_fields(struct tuple_format *format)
{
	struct tuple_field *field, *tmp;
	json_tree_foreach_entry_safe(field, &format->fields.root,
				     struct tuple_field, token, tmp) {
		json_tree_del(&format->fields, &field->token);
		tuple_field_delete(field);
	}
	json_tree_destroy(&format->fields);
}

static struct tuple_format *
tuple_format_alloc(struct key_def * const *keys, uint16_t key_count,
		   uint32_t space_field_count, struct tuple_dictionary *dict)
{
	/* Size of area to store paths. */
	uint32_t paths_size = 0;
	uint32_t index_field_count = 0;
	/* find max max field no */
	for (uint16_t key_no = 0; key_no < key_count; ++key_no) {
		const struct key_def *key_def = keys[key_no];
		const struct key_part *part = key_def->parts;
		const struct key_part *pend = part + key_def->part_count;
		for (; part < pend; part++) {
			index_field_count = MAX(index_field_count,
						part->fieldno + 1);
			paths_size += part->path_len;
		}
	}
	uint32_t field_count = MAX(space_field_count, index_field_count);

	uint32_t allocation_size = sizeof(struct tuple_format) + paths_size;
	struct tuple_format *format = malloc(allocation_size);
	if (format == NULL) {
		diag_set(OutOfMemory, allocation_size, "malloc",
			 "tuple format");
		return NULL;
	}
	if (json_tree_create(&format->fields) != 0) {
		diag_set(OutOfMemory, 0, "json_lexer_create",
			 "tuple field tree");
		free(format);
		return NULL;
	}
	for (uint32_t fieldno = 0; fieldno < field_count; fieldno++) {
		struct tuple_field *field = tuple_field_new();
		if (field == NULL)
			goto error;
		field->token.num = fieldno;
		field->token.type = JSON_TOKEN_NUM;
		if (json_tree_add(&format->fields, &format->fields.root,
				  &field->token) != 0) {
			diag_set(OutOfMemory, 0, "json_tree_add",
				 "tuple field tree entry");
			tuple_field_delete(field);
			goto error;
		}
	}
	if (dict == NULL) {
		assert(space_field_count == 0);
		format->dict = tuple_dictionary_new(NULL, 0);
		if (format->dict == NULL)
			goto error;
	} else {
		format->dict = dict;
		tuple_dictionary_ref(dict);
	}
	format->total_field_count = field_count;
	format->required_fields = NULL;
	format->max_path_tokens = 1;
	format->vy_stmt_size = UINT32_MAX;
	format->refs = 0;
	format->id = FORMAT_ID_NIL;
	format->index_field_count = index_field_count;
	format->exact_field_count = 0;
	format->min_field_count = 0;
	return format;
error:
	tuple_format_destroy_fields(format);
	free(format);
	return NULL;
}

/** Free tuple format resources, doesn't unregister. */
static inline void
tuple_format_destroy(struct tuple_format *format)
{
	free(format->required_fields);
	tuple_format_destroy_fields(format);
	tuple_dictionary_unref(format->dict);
}

void
tuple_format_delete(struct tuple_format *format)
{
	tuple_format_deregister(format);
	tuple_format_destroy(format);
	free(format);
}

struct tuple_format *
tuple_format_new(struct tuple_format_vtab *vtab, struct key_def * const *keys,
		 uint16_t key_count, const struct field_def *space_fields,
		 uint32_t space_field_count, struct tuple_dictionary *dict)
{
	struct tuple_format *format =
		tuple_format_alloc(keys, key_count, space_field_count, dict);
	if (format == NULL)
		return NULL;
	format->vtab = *vtab;
	format->engine = NULL;
	format->is_temporary = false;
	if (tuple_format_register(format) < 0) {
		tuple_format_destroy(format);
		free(format);
		return NULL;
	}
	if (tuple_format_create(format, keys, key_count, space_fields,
				space_field_count) < 0) {
		tuple_format_delete(format);
		return NULL;
	}
	return format;
}

bool
tuple_format1_can_store_format2_tuples(struct tuple_format *format1,
				       struct tuple_format *format2)
{
	if (format1->exact_field_count != format2->exact_field_count)
		return false;
	struct tuple_field *field1;
	json_tree_foreach_entry_preorder(field1, &format1->fields.root,
					 struct tuple_field, token) {
next:;
		const char *path = tuple_field_path(field1, true);
		struct tuple_field *field2 =
			json_tree_lookup_path_entry(&format2->fields,
						    &format2->fields.root,
						    path, strlen(path),
						    TUPLE_INDEX_BASE,
						    struct tuple_field, token);
		/*
		 * The field has a data type in format1, but has
		 * no data type in format2.
		 */
		if (field2 == NULL) {
			/*
			 * The field can get a name added
			 * for it, and this doesn't require a data
			 * check.
			 * If the field is defined as not
			 * nullable, however, we need a data
			 * check, since old data may contain
			 * NULLs or miss the subject field.
			 */
			if (field1->type == FIELD_TYPE_ANY &&
			    tuple_field_is_nullable(field1)) {
				/* Skip subtree. */
				struct json_token *token = &field1->token;
				struct json_token *parent = token->parent;
				field1 = json_tree_child_next_entry(parent,
								    token,
								    struct
								    tuple_field,
								    token);
				if (field1 == NULL)
					break;
				goto next;
			} else {
				return false;
			}
		}
		if (! field_type1_contains_type2(field1->type, field2->type))
			return false;
		/*
		 * Do not allow transition from nullable to non-nullable:
		 * it would require a check of all data in the space.
		 */
		if (tuple_field_is_nullable(field2) &&
		    !tuple_field_is_nullable(field1))
			return false;
	}
	return true;
}

/**
 * Descriptor of the parsed msgpack frame.
 * Due to the fact that the msgpack has nested structures whose
 * length is stored in the frame header at the blob beginning, we
 * need to be able to determine that we have finished parsing the
 * current component and should move on to the next one.
 * For this purpose a stack of disassembled levels is organized,
 * where the type of the level, the total number of elements,
 * and the number of elements that have already been parsed are
 * stored.
 */
struct mp_frame {
	/** JSON token type representing frame data structure. */
	enum json_token_type child_type;
	/** Total count of MP members to process. */
	uint32_t total;
	/** Count of MP elements that already have parseed. */
	uint32_t curr;
};

/**
 * Emit token to analyze and do msgpack pointer shift using top
 * mp_stack frame. Return 0 on success, -1 when analyse step must
 * be skipped (on usuported term detection).
 */
static int
mp_frame_parse(struct mp_frame *mp_stack, uint32_t mp_stack_idx,
	       const char **pos, struct json_token *token)
{
	token->type = mp_stack[mp_stack_idx].child_type;
	++mp_stack[mp_stack_idx].curr;
	if (token->type == JSON_TOKEN_NUM) {
		token->num = mp_stack[mp_stack_idx].curr - TUPLE_INDEX_BASE;
	} else if (token->type == JSON_TOKEN_STR) {
		if (mp_typeof(**pos) != MP_STR) {
			/* Skip key. */
			mp_next(pos);
			return -1;
		}
		token->str = mp_decode_str(pos, (uint32_t *)&token->len);
	} else {
		unreachable();
	}
	return 0;
}

/**
 * Prepare mp_frame for futher iterations. Store container length
 * and child_type. Update parent token pointer and shift msgpack
 * pointer.
 */
static int
mp_frame_prepare(struct mp_frame *mp_stack, uint32_t *mp_stack_idx,
		 uint32_t mp_stack_total, struct json_token *token,
		 const char **pos, struct json_token **parent)
{
	enum mp_type type = mp_typeof(**pos);
	if (token != NULL && *mp_stack_idx + 1 < mp_stack_total &&
	    (type == MP_MAP || type == MP_ARRAY)) {
		uint32_t size = type == MP_ARRAY ? mp_decode_array(pos) :
				mp_decode_map(pos);
		if (size == 0)
			return 0;
		*parent = token;
		enum json_token_type child_type =
			type == MP_ARRAY ? JSON_TOKEN_NUM : JSON_TOKEN_STR;
		*mp_stack_idx = *mp_stack_idx + 1;
		mp_stack[*mp_stack_idx].child_type = child_type;
		mp_stack[*mp_stack_idx].total = size;
		mp_stack[*mp_stack_idx].curr = 0;
	} else {
		mp_next(pos);
		while (mp_stack[*mp_stack_idx].curr >=
			mp_stack[*mp_stack_idx].total) {
			assert(*parent != NULL);
			*parent = (*parent)->parent;
			if (*mp_stack_idx == 0)
				return -1;
			*mp_stack_idx = *mp_stack_idx - 1;
		}
	}
	return 0;
}

/** @sa declaration for details. */
int
tuple_init_field_map(struct tuple_format *format, uint32_t *field_map,
		     const char *tuple, bool validate)
{
	if (tuple_format_field_count(format) == 0)
		return 0; /* Nothing to initialize */

	struct region *region = &fiber()->gc;
	size_t region_svp = region_used(region);
	const char *pos = tuple;
	int rc = 0;

	/* Check to see if the tuple has a sufficient number of fields. */
	uint32_t field_count = mp_decode_array(&pos);
	if (validate && format->exact_field_count > 0 &&
	    format->exact_field_count != field_count) {
		diag_set(ClientError, ER_EXACT_FIELD_COUNT,
			 (unsigned) field_count,
			 (unsigned) format->exact_field_count);
		goto error;
	}
	/*
	 * Allocate a field bitmap that will be used for checking
	 * that all mandatory fields are present.
	 */
	void *required_fields = NULL;
	size_t required_fields_sz = 0;
	if (validate) {
		required_fields_sz = bitmap_size(format->total_field_count);
		required_fields = region_alloc(region, required_fields_sz);
		if (required_fields == NULL) {
			diag_set(OutOfMemory, required_fields_sz,
				 "region", "required field bitmap");
			goto error;
		}
		memcpy(required_fields, format->required_fields,
		       required_fields_sz);
	}
	/*
	 * Initialize the tuple field map and validate field types.
	 */
	if (field_count == 0) {
		/* Empty tuple, nothing to do. */
		goto skip;
	}
	uint32_t defined_field_count = MIN(field_count, validate ?
					   tuple_format_field_count(format) :
					   format->index_field_count);
	/*
	 * Nullify field map to be able to detect by 0,
	 * which key fields are absent in tuple_field().
	 */
	memset((char *)field_map - format->field_map_size, 0,
		format->field_map_size);
	uint32_t mp_stack_size =
		format->max_path_tokens * sizeof(struct mp_frame);
	struct mp_frame *mp_stack = region_alloc(region, mp_stack_size);
	if (mp_stack == NULL) {
		diag_set(OutOfMemory, mp_stack_size, "region_alloc",
			 "mp_stack");
		goto error;
	}
	struct tuple_field *field;
	mp_stack[0].child_type = JSON_TOKEN_NUM;
	mp_stack[0].total = defined_field_count;
	mp_stack[0].curr = 0;
	uint32_t mp_stack_idx = 0;
	struct json_tree *tree = (struct json_tree *)&format->fields;
	struct json_token *parent = &tree->root;
	while (mp_stack[0].curr <= mp_stack[0].total) {
		struct json_token token;
		if (mp_frame_parse(mp_stack, mp_stack_idx, &pos, &token) != 0) {
			/* Unsupported token. */
			goto finish_frame;
		}
		field = json_tree_lookup_entry(tree, parent, &token,
					       struct tuple_field, token);
		if (field != NULL) {
			bool is_nullable = tuple_field_is_nullable(field);
			if (validate &&
			    !field_mp_type_is_compatible(field->type,
							 mp_typeof(*pos),
							 is_nullable) != 0) {
				diag_set(ClientError, ER_FIELD_TYPE,
					 tuple_field_path(field, false),
					 field_type_strs[field->type]);
				goto error;
			}
			if (field->offset_slot != TUPLE_OFFSET_SLOT_NIL) {
				field_map[field->offset_slot] =
					(uint32_t)(pos - tuple);
			}
			if (required_fields != NULL)
				bit_clear(required_fields, field->id);
		}
finish_frame:
		/* Prepare stack info for next iteration. */
		if (mp_frame_prepare(mp_stack, &mp_stack_idx,
				     format->max_path_tokens,
				     field != NULL ? &field->token : NULL,
				     &pos, &parent) != 0)
			break;
	};
skip:
	/*
	 * Check the required field bitmap for missing fields.
	 */
	if (required_fields != NULL) {
		struct bit_iterator it;
		bit_iterator_init(&it, required_fields,
				  required_fields_sz, true);
		size_t id = bit_iterator_next(&it);
		if (id < SIZE_MAX) {
			/* A field is missing, report an error. */
			field = tuple_format_field_by_id(format, id);
			assert(field != NULL);
			diag_set(ClientError, ER_FIELD_MISSING,
				 tuple_field_path(field, false));
			goto error;
		}
	}
out:
	region_truncate(region, region_svp);
	return rc;
error:
	rc = -1;
	goto out;
}

uint32_t
tuple_format_min_field_count(struct key_def * const *keys, uint16_t key_count,
			     const struct field_def *space_fields,
			     uint32_t space_field_count)
{
	uint32_t min_field_count = 0;
	for (uint32_t i = 0; i < space_field_count; ++i) {
		if (! space_fields[i].is_nullable)
			min_field_count = i + 1;
	}
	for (uint32_t i = 0; i < key_count; ++i) {
		const struct key_def *kd = keys[i];
		for (uint32_t j = 0; j < kd->part_count; ++j) {
			const struct key_part *kp = &kd->parts[j];
			if (!key_part_is_nullable(kp) &&
			    kp->fieldno + 1 > min_field_count)
				min_field_count = kp->fieldno + 1;
		}
	}
	return min_field_count;
}

/** Destroy tuple format subsystem and free resourses */
void
tuple_format_free()
{
	/* Clear recycled ids. */
	while (recycled_format_ids != FORMAT_ID_NIL) {
		uint16_t id = (uint16_t) recycled_format_ids;
		recycled_format_ids = (intptr_t) tuple_formats[id];
		tuple_formats[id] = NULL;
	}
	for (struct tuple_format **format = tuple_formats;
	     format < tuple_formats + formats_size; format++) {
		/* Do not unregister. Only free resources. */
		if (*format != NULL) {
			tuple_format_destroy(*format);
			free(*format);
		}
	}
	free(tuple_formats);
}

void
box_tuple_format_ref(box_tuple_format_t *format)
{
	tuple_format_ref(format);
}

void
box_tuple_format_unref(box_tuple_format_t *format)
{
	tuple_format_unref(format);
}

/**
 * Propagate @a field to MessagePack(field)[index].
 * @param[in][out] field Field to propagate.
 * @param index 0-based index to propagate to.
 *
 * @retval  0 Success, the index was found.
 * @retval -1 Not found.
 */
static inline int
tuple_field_go_to_index(const char **field, uint64_t index)
{
	enum mp_type type = mp_typeof(**field);
	if (type == MP_ARRAY) {
		uint32_t count = mp_decode_array(field);
		if (index >= count)
			return -1;
		for (; index > 0; --index)
			mp_next(field);
		return 0;
	} else if (type == MP_MAP) {
		index += TUPLE_INDEX_BASE;
		uint64_t count = mp_decode_map(field);
		for (; count > 0; --count) {
			type = mp_typeof(**field);
			if (type == MP_UINT) {
				uint64_t value = mp_decode_uint(field);
				if (value == index)
					return 0;
			} else if (type == MP_INT) {
				int64_t value = mp_decode_int(field);
				if (value >= 0 && (uint64_t)value == index)
					return 0;
			} else {
				/* Skip key. */
				mp_next(field);
			}
			/* Skip value. */
			mp_next(field);
		}
	}
	return -1;
}

/**
 * Propagate @a field to MessagePack(field)[key].
 * @param[in][out] field Field to propagate.
 * @param key Key to propagate to.
 * @param len Length of @a key.
 *
 * @retval  0 Success, the index was found.
 * @retval -1 Not found.
 */
static inline int
tuple_field_go_to_key(const char **field, const char *key, int len)
{
	enum mp_type type = mp_typeof(**field);
	if (type != MP_MAP)
		return -1;
	uint64_t count = mp_decode_map(field);
	for (; count > 0; --count) {
		type = mp_typeof(**field);
		if (type == MP_STR) {
			uint32_t value_len;
			const char *value = mp_decode_str(field, &value_len);
			if (value_len == (uint)len &&
			    memcmp(value, key, len) == 0)
				return 0;
		} else {
			/* Skip key. */
			mp_next(field);
		}
		/* Skip value. */
		mp_next(field);
	}
	return -1;
}

int
tuple_field_go_to_path(const char **data, const char *path, uint32_t path_len)
{
	int rc;
	struct json_lexer lexer;
	struct json_token token;
	json_lexer_create(&lexer, path, path_len, TUPLE_INDEX_BASE);
	while ((rc = json_lexer_next_token(&lexer, &token)) == 0) {
		switch (token.type) {
		case JSON_TOKEN_NUM:
			rc = tuple_field_go_to_index(data, token.num);
			break;
		case JSON_TOKEN_STR:
			rc = tuple_field_go_to_key(data, token.str, token.len);
			break;
		default:
			assert(token.type == JSON_TOKEN_END);
			return 0;
		}
		if (rc != 0) {
			*data = NULL;
			return 0;
		}
	}
	return rc;
}

int
tuple_field_raw_by_path(struct tuple_format *format, const char *tuple,
                        const uint32_t *field_map, const char *path,
                        uint32_t path_len, uint32_t path_hash,
                        const char **field)
{
	assert(path_len > 0);
	uint32_t fieldno;
	/*
	 * It is possible, that a field has a name as
	 * well-formatted JSON. For example 'a.b.c.d' or '[1]' can
	 * be field name. To save compatibility at first try to
	 * use the path as a field name.
	 */
	if (tuple_fieldno_by_name(format->dict, path, path_len, path_hash,
				  &fieldno) == 0) {
		*field = tuple_field_raw(format, tuple, field_map, fieldno);
		return 0;
	}
	struct json_lexer lexer;
	struct json_token token;
	json_lexer_create(&lexer, path, path_len, TUPLE_INDEX_BASE);
	int rc = json_lexer_next_token(&lexer, &token);
	if (rc != 0)
		goto error;
	switch(token.type) {
	case JSON_TOKEN_NUM: {
		int index = token.num;
		*field = tuple_field_raw(format, tuple, field_map, index);
		if (*field == NULL)
			return 0;
		break;
	}
	case JSON_TOKEN_STR: {
		/* First part of a path is a field name. */
		uint32_t name_hash;
		if (path_len == (uint32_t) token.len) {
			name_hash = path_hash;
		} else {
			/*
			 * If a string is "field....", then its
			 * precalculated juajit hash can not be
			 * used. A tuple dictionary hashes only
			 * name, not path.
			 */
			name_hash = field_name_hash(token.str, token.len);
		}
		*field = tuple_field_raw_by_name(format, tuple, field_map,
						 token.str, token.len,
						 name_hash);
		if (*field == NULL)
			return 0;
		break;
	}
	default:
		assert(token.type == JSON_TOKEN_END);
		*field = NULL;
		return 0;
	}
	rc = tuple_field_go_to_path(field, path + lexer.offset,
				    path_len - lexer.offset);
	if (rc == 0)
		return 0;
	/* Setup absolute error position. */
	rc += lexer.offset;

error:
	assert(rc > 0);
	diag_set(ClientError, ER_ILLEGAL_PARAMS,
		 tt_sprintf("error in path on position %d", rc));
	return -1;
}

int
tuple_field_by_part_raw_slowpath(struct tuple_format *format, const char *data,
				 const uint32_t *field_map,
				 struct key_part *part, const char **raw)
{
	assert(part->path != NULL);
	struct tuple_field *field =
		tuple_format_field_by_path(format, part->fieldno, part->path,
					   part->path_len);
	if (field != NULL) {
		int32_t offset_slot = field->offset_slot;
		assert(-offset_slot * sizeof(uint32_t) <=
		       format->field_map_size);
		*raw = field_map[offset_slot] == 0 ?
		       NULL : data + field_map[offset_slot];
		return 0;
	}
	/*
	 * Format doesn't have field representing specified part.
	 * Make slow tuple parsing.
	 */
	*raw = tuple_field_raw(format, data, field_map, part->fieldno);
	if (*raw == NULL)
		return 0;
	return tuple_field_go_to_path(raw, part->path, part->path_len);
}
