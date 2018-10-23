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
#include "swim.h"
#include "swim_io.h"
#include "uri.h"
#include "assoc.h"
#include "fiber.h"
#include "msgpuck.h"
#include "info.h"

/**
 * SWIM - Scalable Weakly-consistent Infection-style Process Group
 * Membership Protocol. It consists of 2 components: events
 * dissemination and failure detection, and stores in memory a
 * table of known remote hosts - members. Also some SWIM
 * implementations have an additional component: anti-entropy -
 * periodical broadcast of a random subset of members table.
 *
 * Each SWIM component is different from others in both message
 * structures and goals, they even could be sent in different
 * messages. But SWIM describes piggybacking of messages: a ping
 * message can piggyback a dissemination's one. SWIM has a main
 * operating cycle during which it randomly chooses members from a
 * member table and sends them events + ping. Answers are
 * processed out of the main cycle asynchronously.
 *
 * Random selection provides even network load about ~1 message to
 * each member regardless of the cluster size. Without randomness
 * a member would get a network load of N messages each protocol
 * step, since all other members will choose the same member on
 * each step where N is the cluster size.
 *
 * Also SWIM describes a kind of fairness: when selecting a next
 * member to ping, the protocol prefers LRU members. In code it
 * would too complicated, so Tarantool's implementation is
 * slightly different, easier.
 *
 * Tarantool splits protocol operation into rounds. At the
 * beginning of a round all members are randomly reordered and
 * linked into a list. At each round step a member is popped from
 * the list head, a message is sent to him, and he waits for the
 * next round. In such implementation all random selection of the
 * original SWIM is executed once per round. The round is
 * 'planned' actually. A list is used instead of an array since
 * new members can be added to its tail without realloc, and dead
 * members can be removed as easy as that.
 *
 * Also Tarantool implements third component - anti-entropy. Why
 * is it needed and even vital? Consider the example: two SWIM
 * nodes, both are alive. Nothing happens, so the events list is
 * empty, only pings are being sent periodically. Then a third
 * node appears. It knows about one of existing nodes. How should
 * it learn about another one? Sure, its known counterpart can try
 * to notify another one, but it is UDP, so this event can lost.
 * Anti-entropy is an extra simple component, it just piggybacks
 * random part of members table with each regular ping. In the
 * example above the new node will learn about the third one via
 * anti-entropy messages of the second one soon or late.
 */

enum {
	/** How often to send membership messages and pings. */
	HEARTBEAT_RATE_DEFAULT = 1,
};

/**
 * Take a random number not blindly calculating a module, but
 * scaling random number down the given borders to save
 * distribution. A result belongs the range [start, end].
 */
static inline int
swim_scaled_rand(int start, int end)
{
	assert(end > start);
	return rand() / (RAND_MAX / (end - start + 1) + 1);
}

enum swim_member_status {
	/**
	 * The instance is ok, it responds to requests, sends its
	 * members table.
	 */
	MEMBER_ALIVE = 0,
	swim_member_status_MAX,
};

static const char *swim_member_status_strs[] = {
	"alive",
};

/**
 * A cluster member description. This structure describes the
 * last known state of an instance, that is updated periodically
 * via UDP according to SWIM protocol.
 */
struct swim_member {
	/**
	 * Member status. Since the communication goes via UDP,
	 * actual status can be different, as well as different on
	 * other SWIM nodes. But SWIM guarantees that each member
	 * will learn a real status of an instance sometime.
	 */
	enum swim_member_status status;
	/**
	 * Address of the instance to which send UDP packets.
	 * Unique identifier of the member.
	 */
	struct sockaddr_in addr;
	/**
	 * Position in a queue of members in the current round.
	 */
	struct rlist in_queue_round;
};

/**
 * SWIM instance. Each instance uses its own UDP port. Tarantool
 * can have multiple SWIMs.
 */
struct swim {
	/**
	 * Global hash of all known members of the cluster. Hash
	 * key is bitwise combination of ip and port, value is a
	 * struct member, describing a remote instance. The only
	 * purpose of such strange hash function is to be able to
	 * reuse mh_i64ptr_t instead of introducing one more
	 * implementation of mhash.
	 *
	 * Discovered members live here until they are
	 * unavailable - in such a case they are removed from the
	 * hash. But a subset of members are pinned - the ones
	 * added explicitly via API. When a member is pinned, it
	 * can not be removed from the hash, and the module will
	 * ping him constantly.
	 */
	struct mh_i64ptr_t *members;
	/**
	 * This node. Used to do not send messages to self, it's
	 * meaningless.
	 */
	struct swim_member *self;
	/**
	 * Members to which a message should be sent next during
	 * this round.
	 */
	struct rlist queue_round;
	/** Generator of round step events. */
	struct ev_periodic round_tick;
	/**
	 * Single round step task. It is impossible to have
	 * multiple round steps at the same time, so it is single
	 * and preallocated per SWIM instance.
	 */
	struct swim_task round_step_task;
	/** Transport to send/receive data. */
	const struct swim_transport *transport;
	/** Scheduler of output requests. */
	struct swim_scheduler scheduler;
	/**
	 * An array of members shuffled on each round. Its head it
	 * sent to each member during one round as an
	 * anti-entropy message.
	 */
	struct swim_member **shuffled_members;
};

static inline uint64_t
sockaddr_in_hash(const struct sockaddr_in *a)
{
	return ((uint64_t) a->sin_addr.s_addr << 16) | a->sin_port;
}

/**
 * Main round messages can carry merged failure detection
 * messages and anti-entropy. With these keys the components can
 * be distinguished from each other.
 */
enum swim_component_type {
	SWIM_ANTI_ENTROPY = 0,
};

/** {{{                  Anti-entropy component                 */

/**
 * Attributes of each record of a broadcasted member table. Just
 * the same as some of struct swim_member attributes.
 */
enum swim_member_key {
	SWIM_MEMBER_STATUS = 0,
	/**
	 * Now can only be IP. But in future UNIX sockets can be
	 * added.
	 */
	SWIM_MEMBER_ADDRESS,
	SWIM_MEMBER_PORT,
	swim_member_key_MAX,
};

/** SWIM anti-entropy MsgPack header template. */
struct PACKED swim_anti_entropy_header_bin {
	/** mp_encode_uint(SWIM_ANTI_ENTROPY) */
	uint8_t k_anti_entropy;
	/** mp_encode_array() */
	uint8_t m_anti_entropy;
	uint32_t v_anti_entropy;
};

static inline void
swim_anti_entropy_header_bin_create(struct swim_anti_entropy_header_bin *header,
				    int batch_size)
{
	header->k_anti_entropy = SWIM_ANTI_ENTROPY;
	header->m_anti_entropy = 0xdd;
	header->v_anti_entropy = mp_bswap_u32(batch_size);
}

/** SWIM member MsgPack template. */
struct PACKED swim_member_bin {
	/** mp_encode_map(3) */
	uint8_t m_header;

	/** mp_encode_uint(SWIM_MEMBER_STATUS) */
	uint8_t k_status;
	/** mp_encode_uint(enum member_status) */
	uint8_t v_status;

	/** mp_encode_uint(SWIM_MEMBER_ADDRESS) */
	uint8_t k_addr;
	/** mp_encode_uint(addr.sin_addr.s_addr) */
	uint8_t m_addr;
	uint32_t v_addr;

	/** mp_encode_uint(SWIM_MEMBER_PORT) */
	uint8_t k_port;
	/** mp_encode_uint(addr.sin_port) */
	uint8_t m_port;
	uint16_t v_port;
};

static inline void
swim_member_bin_reset(struct swim_member_bin *header,
		      struct swim_member *member)
{
	header->v_status = member->status;
	header->v_addr = mp_bswap_u32(member->addr.sin_addr.s_addr);
	header->v_port = mp_bswap_u16(member->addr.sin_port);
}

static inline void
swim_member_bin_create(struct swim_member_bin *header)
{
	header->m_header = 0x83;
	header->k_status = SWIM_MEMBER_STATUS;
	header->k_addr = SWIM_MEMBER_ADDRESS;
	header->m_addr = 0xce;
	header->k_port = SWIM_MEMBER_PORT;
	header->m_port = 0xcd;
}

/** }}}                  Anti-entropy component                 */

/**
 * SWIM message structure:
 * {
 *     SWIM_ANTI_ENTROPY: [
 *         {
 *             SWIM_MEMBER_STATUS: uint, enum member_status,
 *             SWIM_MEMBER_ADDRESS: uint, ip,
 *             SWIM_MEMBER_PORT: uint, port
 *         },
 *         ...
 *     ],
 * }
 */

/**
 * Remove the member from all queues, hashes, destroy it and free
 * the memory.
 */
static void
swim_member_delete(struct swim *swim, struct swim_member *member)
{
	uint64_t key = sockaddr_in_hash(&member->addr);
	mh_int_t rc = mh_i64ptr_find(swim->members, key, NULL);
	assert(rc != mh_end(swim->members));
	mh_i64ptr_del(swim->members, rc, NULL);
	rlist_del_entry(member, in_queue_round);

	free(member);
}

/**
 * Register a new member with a specified status. Here it is
 * added to the hash, to the 'next' queue.
 */
static struct swim_member *
swim_member_new(struct swim *swim, const struct sockaddr_in *addr,
		enum swim_member_status status)
{
	struct swim_member *member =
		(struct swim_member *) calloc(1, sizeof(*member));
	if (member == NULL) {
		diag_set(OutOfMemory, sizeof(*member), "calloc", "member");
		return NULL;
	}
	member->status = status;
	member->addr = *addr;
	struct mh_i64ptr_node_t node;
	node.key = sockaddr_in_hash(addr);
	node.val = member;
	mh_int_t rc = mh_i64ptr_put(swim->members, &node, NULL, NULL);
	if (rc == mh_end(swim->members)) {
		free(member);
		diag_set(OutOfMemory, sizeof(mh_int_t), "malloc", "node");
		return NULL;
	}
	rlist_add_entry(&swim->queue_round, member, in_queue_round);

	return member;
}

static inline struct swim_member *
swim_find_member(struct swim *swim, const struct sockaddr_in *addr)
{
	uint64_t hash = sockaddr_in_hash(addr);
	mh_int_t node = mh_i64ptr_find(swim->members, hash, NULL);
	if (node == mh_end(swim->members))
		return NULL;
	return (struct swim_member *) mh_i64ptr_node(swim->members, node)->val;
}

/** At the end of each round members table is shuffled. */
static int
swim_shuffle_members(struct swim *swim)
{
	struct mh_i64ptr_t *members = swim->members;
	struct swim_member **shuffled = swim->shuffled_members;
	int new_size = mh_size(members);
	int bsize = sizeof(shuffled[0]) * new_size;
	struct swim_member **new_shuffled =
		(struct swim_member **) realloc(shuffled, bsize);
	if (new_shuffled == NULL) {
		diag_set(OutOfMemory, bsize, "realloc", "new_shuffled");
		return -1;
	}
	shuffled = new_shuffled;
	swim->shuffled_members = new_shuffled;
	int i = 0;
	for (mh_int_t node = mh_first(members), end = mh_end(members);
	     node != end; node = mh_next(members, node), ++i) {
		shuffled[i] = (struct swim_member *)
			mh_i64ptr_node(members, node)->val;
		int j = swim_scaled_rand(0, i);
		SWAP(shuffled[i], shuffled[j]);
	}
	return 0;
}

/**
 * Shuffle, filter members. Build randomly ordered queue of
 * addressees. In other words, do all round preparation work.
 */
static int
swim_new_round(struct swim *swim)
{
	say_verbose("SWIM: start a new round");
	if (swim_shuffle_members(swim) != 0)
		return -1;
	rlist_create(&swim->queue_round);
	int size = mh_size(swim->members);
	for (int i = 0; i < size; ++i) {
		if (swim->shuffled_members[i] != swim->self) {
			rlist_add_entry(&swim->queue_round,
					swim->shuffled_members[i],
					in_queue_round);
		}
	}
	return 0;
}

/**
 * Encode anti-entropy header and members data as many as
 * possible to the end of a last packet.
 * @retval -1 Error.
 * @retval 0 Not error, but nothing is encoded.
 * @retval 1 Something is encoded.
 */
static int
swim_encode_anti_entropy(struct swim *swim, struct swim_msg *msg)
{
	struct swim_anti_entropy_header_bin ae_header_bin;
	struct swim_member_bin member_bin;
	struct swim_packet *packet = swim_msg_last_packet(msg);
	if (packet == NULL)
		return -1;
	char *header = swim_packet_alloc(packet, sizeof(ae_header_bin));
	if (header == NULL)
		return 0;
	int i = 0;

	swim_member_bin_create(&member_bin);
	for (; i < (int) mh_size(swim->members); ++i) {
		char *pos = swim_packet_alloc(packet, sizeof(member_bin));
		if (pos == NULL)
			break;
		struct swim_member *member = swim->shuffled_members[i];
		swim_member_bin_reset(&member_bin, member);
		memcpy(pos, &member_bin, sizeof(member_bin));
	}
	if (i == 0)
		return 0;
	swim_anti_entropy_header_bin_create(&ae_header_bin, i);
	memcpy(header, &ae_header_bin, sizeof(ae_header_bin));
	swim_packet_flush(packet);
	return 1;
}

/** Encode SWIM components into a sequence of UDP packets. */
static int
swim_encode_round_msg(struct swim *swim, struct swim_msg *msg)
{
	swim_msg_create(msg);
	struct swim_packet *packet = swim_msg_reserve(msg, 1);
	if (packet == NULL)
		return -1;
	char *header = swim_packet_alloc(packet, 1);
	int rc, map_size = 0;

	rc = swim_encode_anti_entropy(swim, msg);
	if (rc < 0)
		goto error;
	map_size += rc;

	assert(mp_sizeof_map(map_size) == 1);
	mp_encode_map(header, map_size);
	return 0;
error:
	swim_msg_destroy(msg);
	return -1;
}

/** Once per specified timeout trigger a next broadcast step. */
static void
swim_round_step_begin(struct ev_loop *loop, struct ev_periodic *p, int events)
{
	assert((events & EV_PERIODIC) != 0);
	(void) events;
	struct swim *swim = (struct swim *) p->data;
	if ((swim->shuffled_members == NULL ||
	     rlist_empty(&swim->queue_round)) && swim_new_round(swim) != 0) {
		diag_log();
		return;
	}
	/*
	 * Possibly empty, if no members but self are specified.
	 */
	if (rlist_empty(&swim->queue_round))
		return;

	struct swim_msg *msg = &swim->round_step_task.msg;
	if (swim_encode_round_msg(swim, msg) != 0) {
		diag_log();
		return;
	}
	struct swim_member *m =
		rlist_shift_entry(&swim->queue_round, struct swim_member,
				  in_queue_round);
	swim_task_schedule(&swim->round_step_task,
			   swim->transport->send_round_msg, &m->addr);
	ev_periodic_stop(loop, p);
}

static void
swim_round_step_complete(struct swim_task *task)
{
	struct swim *swim = container_of(task, struct swim, round_step_task);
	swim_msg_reset(&task->msg);
	ev_periodic_start(loop(), &swim->round_tick);
}

/**
 * SWIM member attributes from anti-entropy and dissemination
 * messages.
 */
struct swim_member_def {
	struct sockaddr_in addr;
	enum swim_member_status status;
};

static inline void
swim_member_def_create(struct swim_member_def *def)
{
	memset(def, 0, sizeof(*def));
	def->status = MEMBER_ALIVE;
}

static void
swim_process_member_update(struct swim *swim, struct swim_member_def *def)
{
	struct swim_member *member = swim_find_member(swim, &def->addr);
	/*
	 * Trivial processing of a new member - just add it to the
	 * members table.
	 */
	if (member == NULL) {
		member = swim_member_new(swim, &def->addr, def->status);
		if (member == NULL)
			diag_log();
	}
}

static int
swim_process_member_key(enum swim_member_key key, const char **pos,
			const char *end, const char *msg_pref,
			struct swim_member_def *def)
{
	switch(key) {
	case SWIM_MEMBER_STATUS:
		if (mp_typeof(**pos) != MP_UINT ||
		    mp_check_uint(*pos, end) > 0) {
			say_error("%s member status should be uint", msg_pref);
			return -1;
		}
		key = mp_decode_uint(pos);
		if (key >= swim_member_status_MAX) {
			say_error("%s unknown member status", msg_pref);
			return -1;
		}
		def->status = (enum swim_member_status) key;
		break;
	case SWIM_MEMBER_ADDRESS:
		if (mp_typeof(**pos) != MP_UINT ||
		    mp_check_uint(*pos, end) > 0) {
			say_error("%s member address should be uint", msg_pref);
			return -1;
		}
		def->addr.sin_addr.s_addr = mp_decode_uint(pos);
		break;
	case SWIM_MEMBER_PORT:
		if (mp_typeof(**pos) != MP_UINT ||
		    mp_check_uint(*pos, end) > 0) {
			say_error("%s member port should be uint", msg_pref);
			return -1;
		}
		uint64_t port = mp_decode_uint(pos);
		if (port > UINT16_MAX) {
			say_error("%s member port is invalid", msg_pref);
			return -1;
		}
		def->addr.sin_port = port;
		break;
	default:
		unreachable();
	}
	return 0;
}

/** Decode an anti-entropy message, update members table. */
static int
swim_process_anti_entropy(struct swim *swim, const char **pos, const char *end)
{
	const char *msg_pref = "Invalid SWIM anti-entropy message:";
	if (mp_typeof(**pos) != MP_ARRAY || mp_check_array(*pos, end) > 0) {
		say_error("%s message should be an array", msg_pref);
		return -1;
	}
	uint64_t size = mp_decode_array(pos);
	for (uint64_t i = 0; i < size; ++i) {
		if (mp_typeof(**pos) != MP_MAP ||
		    mp_check_map(*pos, end) > 0) {
			say_error("%s member should be map", msg_pref);
			return -1;
		}
		uint64_t map_size = mp_decode_map(pos);
		struct swim_member_def def;
		swim_member_def_create(&def);
		for (uint64_t j = 0; j < map_size; ++j) {
			if (mp_typeof(**pos) != MP_UINT ||
			    mp_check_uint(*pos, end) > 0) {
				say_error("%s member key should be uint",
					  msg_pref);
				return -1;
			}
			uint64_t key = mp_decode_uint(pos);
			if (key >= swim_member_key_MAX) {
				say_error("%s unknown member key", msg_pref);
				return -1;
			}
			if (swim_process_member_key(key, pos, end, msg_pref,
						    &def) != 0)
				return -1;
		}
		if (def.addr.sin_port == 0 || def.addr.sin_addr.s_addr == 0) {
			say_error("%s member address should be specified",
				  msg_pref);
			return -1;
		}
		swim_process_member_update(swim, &def);
	}
	return 0;
}

/** Receive and process a new message. */
static void
swim_on_input(struct swim_scheduler *scheduler,
	      const struct swim_packet *packet, const struct sockaddr_in *src)
{
	(void) src;
	const char *msg_pref = "Invalid SWIM message:";
	struct swim *swim = container_of(scheduler, struct swim, scheduler);
	const char *pos = packet->body;
	const char *end = packet->pos;
	if (mp_typeof(*pos) != MP_MAP || mp_check_map(pos, end) > 0) {
		say_error("%s expected map header", msg_pref);
		return;
	}
	uint64_t map_size = mp_decode_map(&pos);
	for (uint64_t i = 0; i < map_size; ++i) {
		if (mp_typeof(*pos) != MP_UINT || mp_check_uint(pos, end) > 0) {
			say_error("%s header should contain uint keys",
				  msg_pref);
			return;
		}
		uint64_t key = mp_decode_uint(&pos);
		switch(key) {
		case SWIM_ANTI_ENTROPY:
			say_verbose("SWIM: process anti-entropy");
			if (swim_process_anti_entropy(swim, &pos, end) != 0)
				return;
			break;
		default:
			say_error("%s unknown component type component is "\
				  "supported", msg_pref);
			return;
		}
	}
}

/**
 * Convert a string URI like "ip:port" to sockaddr_in structure.
 */
static int
uri_to_addr(const char *str, struct sockaddr_in *addr)
{
	struct uri uri;
	if (uri_parse(&uri, str) != 0 || uri.service == NULL)
		goto invalid_uri;
	in_addr_t iaddr;
	if (uri.host_len == strlen(URI_HOST_UNIX) &&
	    memcmp(uri.host, URI_HOST_UNIX, uri.host_len) == 0) {
		diag_set(IllegalParams, "Unix sockets are not supported");
		return -1;
	}
	if (uri.host_len == 0) {
		iaddr = htonl(INADDR_ANY);
	} else if (uri.host_len == 9 && memcmp("localhost", uri.host, 9) == 0) {
		iaddr = htonl(INADDR_LOOPBACK);
	} else {
		iaddr = inet_addr(tt_cstr(uri.host, uri.host_len));
		if (iaddr == (in_addr_t) -1)
			goto invalid_uri;
	}
	int port = htons(atoi(uri.service));
	memset(addr, 0, sizeof(*addr));
	addr->sin_family = AF_INET;
	addr->sin_addr.s_addr = iaddr;
	addr->sin_port = port;
	return 0;

invalid_uri:
	diag_set(SocketError, sio_socketname(-1), "invalid uri \"%s\"", str);
	return -1;
}

/**
 * Initialize the module. By default, the module is turned off and
 * does nothing. To start SWIM swim_cfg is used.
 */
struct swim *
swim_new(void)
{
	struct swim *swim = (struct swim *) calloc(1, sizeof(*swim));
	if (swim == NULL) {
		diag_set(OutOfMemory, sizeof(*swim), "calloc", "swim");
		return NULL;
	}
	swim->members = mh_i64ptr_new();
	if (swim->members == NULL) {
		free(swim);
		diag_set(OutOfMemory, sizeof(*swim->members), "malloc",
			 "members");
		return NULL;
	}
	rlist_create(&swim->queue_round);
	ev_init(&swim->round_tick, swim_round_step_begin);
	ev_periodic_set(&swim->round_tick, 0, HEARTBEAT_RATE_DEFAULT, NULL);
	swim->round_tick.data = (void *) swim;
	swim_task_create(&swim->round_step_task, &swim->scheduler,
			 swim_round_step_complete);
	swim->transport = &swim_udp_transport;
	swim_scheduler_create(&swim->scheduler, swim_on_input, swim->transport);
	return swim;
}

int
swim_cfg(struct swim *swim, const char *uri, double heartbeat_rate,
	 const struct swim_transport *new_transport)
{
	struct sockaddr_in addr;
	if (uri_to_addr(uri, &addr) != 0)
		return -1;
	struct swim_member *new_self = NULL;
	if (swim_find_member(swim, &addr) == NULL) {
		new_self = swim_member_new(swim, &addr, MEMBER_ALIVE);
		if (new_self == NULL)
			return -1;
	}
	if (swim_scheduler_bind(&swim->scheduler, &addr) != 0) {
		swim_member_delete(swim, new_self);
		return -1;
	}
	ev_periodic_start(loop(), &swim->round_tick);

	if (swim->round_tick.interval != heartbeat_rate && heartbeat_rate > 0)
		ev_periodic_set(&swim->round_tick, 0, heartbeat_rate, NULL);

	swim->self = new_self;
	if (new_transport != NULL) {
		swim->transport = new_transport;
		swim_scheduler_set_transport(&swim->scheduler, new_transport);
	}
	return 0;
}

int
swim_add_member(struct swim *swim, const char *uri)
{
	struct sockaddr_in addr;
	if (uri_to_addr(uri, &addr) != 0)
		return -1;
	struct swim_member *member = swim_find_member(swim, &addr);
	if (member == NULL) {
		member = swim_member_new(swim, &addr, MEMBER_ALIVE);
		if (member == NULL)
			return -1;
	}
	return 0;
}

int
swim_remove_member(struct swim *swim, const char *uri)
{
	struct sockaddr_in addr;
	if (uri_to_addr(uri, &addr) != 0)
		return -1;
	struct swim_member *member = swim_find_member(swim, &addr);
	if (member != NULL)
		swim_member_delete(swim, member);
	return 0;
}

void
swim_info(struct swim *swim, struct info_handler *info)
{
	info_begin(info);
	for (mh_int_t node = mh_first(swim->members),
	     end = mh_end(swim->members); node != end;
	     node = mh_next(swim->members, node)) {
		struct swim_member *member = (struct swim_member *)
			mh_i64ptr_node(swim->members, node)->val;
		info_table_begin(info,
				 sio_strfaddr((struct sockaddr *) &member->addr,
					      sizeof(member->addr)));
		info_append_str(info, "status",
				swim_member_status_strs[member->status]);
		info_table_end(info);
	}
	info_end(info);
}

void
swim_delete(struct swim *swim)
{
	swim_scheduler_destroy(&swim->scheduler);
	ev_periodic_stop(loop(), &swim->round_tick);
	swim_task_destroy(&swim->round_step_task);
	mh_int_t node = mh_first(swim->members);
	while (node != mh_end(swim->members)) {
		struct swim_member *m = (struct swim_member *)
			mh_i64ptr_node(swim->members, node)->val;
		swim_member_delete(swim, m);
		node = mh_first(swim->members);
	}
	mh_i64ptr_delete(swim->members);
	free(swim->shuffled_members);
}
