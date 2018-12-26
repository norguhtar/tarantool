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
	/**
	 * If a ping was sent, it is considered to be lost after
	 * this time without an ack.
	 */
	ACK_TIMEOUT = 1,
	/**
	 * If a member has not been responding to pings this
	 * number of times, it is considered to be dead.
	 */
	NO_ACKS_TO_DEAD = 3,
	/**
	 * If a not pinned member confirmed to be dead, it is
	 * removed from the membership after at least this number
	 * of failed pings.
	 */
	NO_ACKS_TO_GC = NO_ACKS_TO_DEAD + 2,
	/** Reserve 272 bytes for headers. */
	MAX_PAYLOAD_SIZE = 1200,
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
	/**
	 * The member is considered to be dead. It will disappear
	 * from the membership, if it is not pinned.
	 */
	MEMBER_DEAD,
	swim_member_status_MAX,
};

static const char *swim_member_status_strs[] = {
	"alive",
	"dead",
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
	/**
	 *
	 *               Failure detection component
	 */
	/**
	 * True, if the member is configured explicitly and can
	 * not disappear from the membership.
	 */
	bool is_pinned;
	/** Growing number to refute old messages. */
	uint64_t incarnation;
	/**
	 * How many pings did not receive an ack in a row. After
	 * a threshold the instance is marked as dead. After more
	 * it is removed from the table (if not pinned).
	 */
	int failed_pings;
	/** When the latest ping was sent to this member. */
	double ping_ts;
	/** Ready at hand regular ACK task. */
	struct swim_task ack_task;
	struct swim_task ping_task;
	/** Position in a queue of members waiting for an ack. */
	struct rlist in_queue_wait_ack;
	/**
	 *
	 *                 Dissemination component
	 *
	 * Dissemination component sends events. Event is a
	 * notification about member status update. So formally,
	 * this structure already has all the needed attributes.
	 * But also an event somehow should be sent to all members
	 * at least once according to SWIM, so it requires
	 * something like TTL for each type of event, which
	 * decrements on each send. And a member can not be
	 * removed from the global table until it gets dead and
	 * its status TTLs is 0, so as to allow other members
	 * learn its dead status.
	 */
	int status_ttl;
	/** Arbitrary user data, disseminated on each change. */
	char *payload;
	/** Useless formal comment: payload size. */
	int payload_size;
	/**
	 * TTL of payload. At most this number of times payload is
	 * sent as a part of dissemination component. Reset on
	 * each update.
	 */
	int payload_ttl;
	/**
	 * Events are put into a queue sorted by event occurrence
	 * time.
	 */
	struct rlist in_queue_events;
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
	/** True, if msg in round_step_task is up to date. */
	bool is_round_msg_valid;
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
	/**
	 *
	 *               Failure detection component
	 */
	/**
	 * Members waiting for an ACK. On too long absence of ACK
	 * a member is considered to be dead and is removed. The
	 * list is sorted by time in ascending order (tail is
	 * newer, head is older).
	 */
	struct rlist queue_wait_ack;
	/** Generator of ack checking events. */
	struct ev_periodic wait_ack_tick;
	/**
	 *
	 *                 Dissemination component
	 */
	/** Queue of events sorted by occurrence time. */
	struct rlist queue_events;
};

static inline uint64_t
sockaddr_in_hash(const struct sockaddr_in *a)
{
	return ((uint64_t) a->sin_addr.s_addr << 16) | a->sin_port;
}

static inline void
cached_round_msg_invalidate(struct swim *swim)
{
	swim->is_round_msg_valid = false;
}

/**
 * Main round messages can carry merged failure detection
 * messages and anti-entropy. With these keys the components can
 * be distinguished from each other.
 */
enum swim_component_type {
	SWIM_ANTI_ENTROPY = 0,
	SWIM_FAILURE_DETECTION,
	SWIM_DISSEMINATION,
};

/** {{{                Failure detection component              */

/** Possible failure detection keys. */
enum swim_fd_key {
	/** Type of the failure detection message: ping or ack. */
	SWIM_FD_MSG_TYPE,
	/**
	 * Incarnation of the sender. To make the member alive if
	 * it was considered to be dead, but ping/ack with greater
	 * incarnation was received from it.
	 */
	SWIM_FD_INCARNATION,
};

/**
 * Failure detection message now has only two types: ping or ack.
 * Indirect ping/ack are todo.
 */
enum swim_fd_msg_type {
	SWIM_FD_MSG_PING,
	SWIM_FD_MSG_ACK,
	swim_fd_msg_type_MAX,
};

static const char *swim_fd_msg_type_strs[] = {
	"ping",
	"ack",
};

/** SWIM failure detection MsgPack header template. */
struct PACKED swim_fd_header_bin {
	/** mp_encode_uint(SWIM_FAILURE_DETECTION) */
	uint8_t k_header;
	/** mp_encode_map(2) */
	uint8_t m_header;

	/** mp_encode_uint(SWIM_FD_MSG_TYPE) */
	uint8_t k_type;
	/** mp_encode_uint(enum swim_fd_msg_type) */
	uint8_t v_type;

	/** mp_encode_uint(SWIM_FD_INCARNATION) */
	uint8_t k_incarnation;
	/** mp_encode_uint(64bit incarnation) */
	uint8_t m_incarnation;
	uint64_t v_incarnation;
};

static inline void
swim_fd_header_bin_create(struct swim_fd_header_bin *header,
			  enum swim_fd_msg_type type, uint64_t incarnation)
{
	header->k_header = SWIM_FAILURE_DETECTION;
	header->m_header = 0x82;

	header->k_type = SWIM_FD_MSG_TYPE;
	header->v_type = type;

	header->k_incarnation = SWIM_FD_INCARNATION;
	header->m_incarnation = 0xcf;
	header->v_incarnation = mp_bswap_u64(incarnation);
}

static void
swim_member_schedule_ack_wait(struct swim *swim, struct swim_member *member)
{
	if (rlist_empty(&member->in_queue_wait_ack)) {
		member->ping_ts = fiber_time();
		rlist_add_tail_entry(&swim->queue_wait_ack, member,
				     in_queue_wait_ack);
	}
}

/** }}}               Failure detection component               */

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
	SWIM_MEMBER_INCARNATION,
	SWIM_MEMBER_PAYLOAD,
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
	/** mp_encode_map(5) */
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

	/** mp_encode_uint(SWIM_MEMBER_INCARNATION) */
	uint8_t k_incarnation;
	/** mp_encode_uint(64bit incarnation) */
	uint8_t m_incarnation;
	uint64_t v_incarnation;

	/** mp_encode_uint(SWIM_MEMBER_PAYLOAD) */
	uint8_t k_payload;
	/** mp_encode_bin(16bit bin header) */
	uint8_t m_payload_size;
	uint16_t v_payload_size;
	/** Payload data ... */
};

static inline void
swim_member_bin_reset(struct swim_member_bin *header,
		      struct swim_member *member)
{
	header->v_status = member->status;
	header->v_addr = mp_bswap_u32(member->addr.sin_addr.s_addr);
	header->v_port = mp_bswap_u16(member->addr.sin_port);
	header->v_incarnation = mp_bswap_u64(member->incarnation);
	header->v_payload_size = mp_bswap_u16(member->payload_size);
}

static inline void
swim_member_bin_create(struct swim_member_bin *header)
{
	header->m_header = 0x85;
	header->k_status = SWIM_MEMBER_STATUS;
	header->k_addr = SWIM_MEMBER_ADDRESS;
	header->m_addr = 0xce;
	header->k_port = SWIM_MEMBER_PORT;
	header->m_port = 0xcd;
	header->k_incarnation = SWIM_MEMBER_INCARNATION;
	header->m_incarnation = 0xcf;
	header->k_payload = SWIM_MEMBER_PAYLOAD;
	header->m_payload_size = 0xc5;
}

/** }}}                  Anti-entropy component                 */

/** {{{                 Dissemination component                 */

/** SWIM dissemination MsgPack template. */
struct PACKED swim_diss_header_bin {
	/** mp_encode_uint(SWIM_DISSEMINATION) */
	uint8_t k_header;
	/** mp_encode_array() */
	uint8_t m_header;
	uint32_t v_header;
};

static inline void
swim_diss_header_bin_create(struct swim_diss_header_bin *header, int batch_size)
{
	header->k_header = SWIM_DISSEMINATION;
	header->m_header = 0xdd;
	header->v_header = mp_bswap_u32(batch_size);
}

/** SWIM event MsgPack template. */
struct PACKED swim_event_bin {
	/** mp_encode_map(4 or 5) */
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

	/** mp_encode_uint(SWIM_MEMBER_INCARNATION) */
	uint8_t k_incarnation;
	/** mp_encode_uint(64bit incarnation) */
	uint8_t m_incarnation;
	uint64_t v_incarnation;
};

static inline void
swim_event_bin_create(struct swim_event_bin *header)
{
	header->k_status = SWIM_MEMBER_STATUS;
	header->k_addr = SWIM_MEMBER_ADDRESS;
	header->m_addr = 0xce;
	header->k_port = SWIM_MEMBER_PORT;
	header->m_port = 0xcd;
	header->k_incarnation = SWIM_MEMBER_INCARNATION;
	header->m_incarnation = 0xcf;
}

static inline void
swim_event_bin_reset(struct swim_event_bin *header, struct swim_member *member)
{
	header->m_header = 0x84 + member->payload_ttl > 0;
	header->v_status = member->status;
	header->v_addr = mp_bswap_u32(member->addr.sin_addr.s_addr);
	header->v_port = mp_bswap_u16(member->addr.sin_port);
	header->v_incarnation = mp_bswap_u64(member->incarnation);
}

static inline void
swim_schedule_event(struct swim *swim, struct swim_member *member)
{
	if (rlist_empty(&member->in_queue_events)) {
		rlist_add_tail_entry(&swim->queue_events, member,
				     in_queue_events);
	}
	member->status_ttl = mh_size(swim->members);
}

/** }}}                 Dissemination component                 */

/**
 * SWIM message structure:
 * {
 *     SWIM_FAILURE_DETECTION: {
 *         SWIM_FD_MSG_TYPE: uint, enum swim_fd_msg_type,
 *         SWIM_FD_INCARNATION: uint
 *     },
 *
 *                 OR/AND
 *
 *     SWIM_DISSEMINATION: [
 *         {
 *             SWIM_MEMBER_STATUS: uint, enum member_status,
 *             SWIM_MEMBER_ADDRESS: uint, ip,
 *             SWIM_MEMBER_PORT: uint, port,
 *             SWIM_MEMBER_INCARNATION: uint
 *         },
 *         ...
 *     ],
 *
 *                 OR/AND
 *
 *     SWIM_ANTI_ENTROPY: [
 *         {
 *             SWIM_MEMBER_STATUS: uint, enum member_status,
 *             SWIM_MEMBER_ADDRESS: uint, ip,
 *             SWIM_MEMBER_PORT: uint, port,
 *             SWIM_MEMBER_INCARNATION: uint
 *         },
 *         ...
 *     ],
 * }
 */

/**
 * Make all needed actions to process a member's update like a
 * change of its status, or incarnation, or both.
 */
static void
swim_member_status_is_updated(struct swim *swim, struct swim_member *member)
{
	swim_schedule_event(swim, member);
	cached_round_msg_invalidate(swim);
}

static void
swim_member_payload_is_updated(struct swim *swim, struct swim_member *member)
{
	swim_schedule_event(swim, member);
	member->payload_ttl = mh_size(swim->members);
	cached_round_msg_invalidate(swim);
}

/**
 * Update status and incarnation of the member if needed. Statuses
 * are compared as a compound key: {incarnation, status}. So @a
 * new_status can override an old one only if its incarnation is
 * greater, or the same, but its status is "bigger". Statuses are
 * compared by their identifier, so "alive" < "dead". This
 * protects from the case when a member is detected as dead on one
 * instance, but overriden by another instance with the same
 * incarnation "alive" message.
 */
static inline void
swim_member_update_status(struct swim *swim, struct swim_member *member,
			  enum swim_member_status new_status,
			  uint64_t incarnation)
{
	assert(member != swim->self);
	if (member->incarnation == incarnation) {
		if (member->status < new_status) {
			member->status = new_status;
			swim_member_status_is_updated(swim, member);
		}
	} else if (member->incarnation < incarnation) {
		member->status = new_status;
		member->incarnation = incarnation;
		swim_member_status_is_updated(swim, member);
	}
}

static inline int
swim_member_update_payload(struct swim *swim, struct swim_member *member,
			   uint64_t incarnation, const char *payload,
			   int payload_size)
{
	if (incarnation < member->incarnation)
		return 0;
	if (payload_size == member->payload_size &&
	    memcmp(payload, member->payload, payload_size) == 0)
		return 0;
	char *new_payload = (char *) realloc(member->payload, payload_size);
	if (new_payload == NULL) {
		diag_set(OutOfMemory, payload_size, "malloc", "new_payload");
		return -1;
	}
	memcpy(new_payload, payload, payload_size);
	member->payload = new_payload;
	member->payload_size = payload_size;
	swim_member_payload_is_updated(swim, member);
	return 0;
}

/**
 * Remove the member from all queues, hashes, destroy it and free
 * the memory.
 */
static void
swim_member_delete(struct swim *swim, struct swim_member *member)
{
	cached_round_msg_invalidate(swim);
	uint64_t key = sockaddr_in_hash(&member->addr);
	mh_int_t rc = mh_i64ptr_find(swim->members, key, NULL);
	assert(rc != mh_end(swim->members));
	mh_i64ptr_del(swim->members, rc, NULL);
	rlist_del_entry(member, in_queue_round);

	/* Failure detection component. */
	rlist_del_entry(member, in_queue_wait_ack);
	swim_task_destroy(&member->ack_task);
	swim_task_destroy(&member->ping_task);

	/* Dissemination component. */
	assert(rlist_empty(&member->in_queue_events));

	free(member);
}

/**
 * Register a new member with a specified status. Here it is
 * added to the hash, to the 'next' queue.
 */
static struct swim_member *
swim_member_new(struct swim *swim, const struct sockaddr_in *addr,
		enum swim_member_status status, uint64_t incarnation,
		const char *payload, int payload_size)
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

	/* Failure detection component. */
	member->incarnation = incarnation;
	rlist_create(&member->in_queue_wait_ack);
	swim_task_create(&member->ack_task, &swim->scheduler, swim_task_reset);
	swim_task_create(&member->ping_task, &swim->scheduler, swim_task_reset);

	/* Dissemination component. */
	rlist_create(&member->in_queue_events);
	swim_member_status_is_updated(swim, member);
	if (swim_member_update_payload(swim, member, incarnation, payload,
				       payload_size) != 0) {
		rlist_del_entry(member, in_queue_events);
		swim_member_delete(swim, member);
		return NULL;
	}

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
	cached_round_msg_invalidate(swim);
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
		struct swim_member *member = swim->shuffled_members[i];
		char *pos = swim_packet_alloc(packet, sizeof(member_bin) +
					      member->payload_size);
		if (pos == NULL)
			break;
		swim_member_bin_reset(&member_bin, member);
		memcpy(pos, &member_bin, sizeof(member_bin));
		pos += sizeof(member_bin);
		memcpy(pos, member->payload, member->payload_size);
	}
	if (i == 0)
		return 0;
	swim_anti_entropy_header_bin_create(&ae_header_bin, i);
	memcpy(header, &ae_header_bin, sizeof(ae_header_bin));
	swim_packet_flush(packet);
	return 1;
}

/**
 * Encode failure detection component.
 * @retval -1 Error.
 * @retval 1 Success, something is encoded.
 */
static int
swim_encode_failure_detection(struct swim *swim, struct swim_msg *msg,
			      enum swim_fd_msg_type type)
{
	struct swim_fd_header_bin fd_header_bin;
	int size = sizeof(fd_header_bin);
	struct swim_packet *packet = swim_msg_reserve(msg, size);
	if (packet == NULL)
		return -1;
	char *pos = swim_packet_alloc(packet, size);
	swim_fd_header_bin_create(&fd_header_bin, type,
				  swim->self->incarnation);
	memcpy(pos, &fd_header_bin, size);
	swim_packet_flush(packet);
	return 1;
}

/**
 * Encode a part of the dissemination component into a single SWIM
 * packet.
 * @retval -1 Error.
 * @retval 0 Not error, but nothing is encoded.
 * @retval 1 Something is encoded.
 */
static int
swim_encode_dissemination_packet(struct swim_msg *msg, struct rlist **queue_pos)
{
	struct swim_diss_header_bin diss_header_bin;
	int size = sizeof(diss_header_bin);
	struct swim_packet *packet = swim_msg_reserve(msg, size);
	if (packet == NULL)
		return -1;
	char *header = swim_packet_alloc(packet, size);

	int i = 0;
	struct swim_member *member, *prev = NULL;
	struct swim_event_bin event_bin;
	swim_event_bin_create(&event_bin);
	rlist_foreach_entry(member, *queue_pos, in_queue_events) {
		int size = sizeof(event_bin);
		if (member->payload_ttl > 0) {
			size += mp_sizeof_uint(SWIM_MEMBER_PAYLOAD) +
				mp_sizeof_bin(member->payload_size);
		}
		char *pos = swim_packet_alloc(packet, size);
		if (pos == NULL)
			break;
		swim_event_bin_reset(&event_bin, member);
		memcpy(pos, &event_bin, sizeof(event_bin));
		pos += sizeof(event_bin);
		if (member->payload_ttl > 0) {
			pos = mp_encode_uint(pos, SWIM_MEMBER_PAYLOAD);
			mp_encode_bin(pos, member->payload,
				      member->payload_size);
		}
		++i;
		prev = member;
	}
	if (i == 0)
		return 0;
	swim_diss_header_bin_create(&diss_header_bin, i);
	memcpy(header, &diss_header_bin, sizeof(diss_header_bin));
	swim_packet_flush(packet);
	return 1;
}

/**
 * Encode failure dissemination component.
 * @retval -1 Error.
 * @retval 1 Success, something is encoded.
 */
static int
swim_encode_dissemination(struct swim *swim, struct swim_msg *msg)
{
	struct rlist *pos;
	rlist_foreach(pos, &swim->queue_events) {
		if (swim_encode_dissemination_packet(msg, &pos) < 0)
			return -1;
	}
	return ! rlist_empty(&swim->queue_events);
}

/** Encode SWIM components into a sequence of UDP packets. */
static int
swim_encode_round_msg(struct swim *swim)
{
	if (swim->is_round_msg_valid)
		return 0;
	struct swim_msg *msg = &swim->round_step_task.msg;
	swim_msg_reset(msg);
	struct swim_packet *packet = swim_msg_reserve(msg, 1);
	if (packet == NULL)
		return -1;
	char *header = swim_packet_alloc(packet, 1);
	int rc, map_size = 0;

	rc = swim_encode_failure_detection(swim, msg, SWIM_FD_MSG_PING);
	if (rc < 0)
		goto error;
	map_size += rc;

	rc = swim_encode_dissemination(swim, msg);
	if (rc < 0)
		goto error;
	map_size += rc;

	rc = swim_encode_anti_entropy(swim, msg);
	if (rc < 0)
		goto error;
	map_size += rc;

	assert(mp_sizeof_map(map_size) == 1);
	mp_encode_map(header, map_size);
	return 0;
error:
	cached_round_msg_invalidate(swim);
	return -1;
}

/** Once per specified timeout trigger a next broadcast step. */
static void
swim_decrease_events_ttl(struct swim *swim)
{
	struct swim_member *member, *tmp;
	rlist_foreach_entry_safe(member, &swim->queue_events, in_queue_events,
				 tmp) {
		assert(member->status_ttl > 0);
		assert(member->status_ttl >= member->payload_ttl);
		if (member->payload_ttl > 0)
			--member->payload_ttl;
		if (--member->status_ttl == 0) {
			rlist_del_entry(member, in_queue_events);
			cached_round_msg_invalidate(swim);
		}
	}
}

/**
 * Do one round step. Send encoded components to a next member
 * from the queue.
 */
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
	if (swim_encode_round_msg(swim) != 0) {
		diag_log();
		return;
	}
	struct swim_member *m =
		rlist_first_entry(&swim->queue_round, struct swim_member,
				  in_queue_round);
	swim_task_schedule(&swim->round_step_task,
			   swim->transport->send_round_msg, &m->addr);
	swim_member_schedule_ack_wait(swim, m);
	swim_decrease_events_ttl(swim);
	ev_periodic_stop(loop, p);
}

static void
swim_round_step_complete(struct swim_task *task)
{
	struct swim *swim = container_of(task, struct swim, round_step_task);
	rlist_shift_entry(&swim->queue_round, struct swim_member,
			  in_queue_round);
	ev_periodic_start(loop(), &swim->round_tick);
}

/** Send a failure detection message. */
static void
swim_schedule_fd_request(struct swim *swim, struct swim_task *task,
			 struct swim_member *m, enum swim_fd_msg_type type,
			 swim_transport_send_f send)
{
	struct swim_msg *msg = &task->msg;
	int rc = swim_encode_failure_detection(swim, msg, type);
	if (rc < 0) {
		diag_log();
		swim_task_delete(task);
		return;
	}
	assert(rc > 0);
	say_verbose("SWIM: send %s to %s", swim_fd_msg_type_strs[type],
		    sio_strfaddr((struct sockaddr *) &m->addr,
				 sizeof(m->addr)));
	swim_task_schedule(task, send, &m->addr);
}

static inline void
swim_schedule_ack(struct swim *swim, struct swim_member *member)
{
	swim_schedule_fd_request(swim, &member->ack_task, member,
				 SWIM_FD_MSG_ACK, swim->transport->send_ack);
}

static inline void
swim_schedule_ping(struct swim *swim, struct swim_member *member)
{
	swim_schedule_fd_request(swim, &member->ping_task, member,
				 SWIM_FD_MSG_PING, swim->transport->send_ping);
	swim_member_schedule_ack_wait(swim, member);
}

/**
 * Check for failed pings. A ping is failed if an ack was not
 * received during ACK_TIMEOUT. A failed ping is resent here.
 */
static void
swim_check_acks(struct ev_loop *loop, struct ev_periodic *p, int events)
{
	assert((events & EV_PERIODIC) != 0);
	(void) loop;
	(void) events;
	struct swim *swim = (struct swim *) p->data;
	struct swim_member *m, *tmp;
	double current_time = fiber_time();
	rlist_foreach_entry_safe(m, &swim->queue_wait_ack, in_queue_wait_ack,
				 tmp) {
		if (current_time - m->ping_ts < ACK_TIMEOUT)
			break;
		++m->failed_pings;
		if (m->failed_pings >= NO_ACKS_TO_GC) {
			if (!m->is_pinned && m->status_ttl == 0)
				swim_member_delete(swim, m);
			continue;
		}
		if (m->failed_pings >= NO_ACKS_TO_DEAD) {
			m->status = MEMBER_DEAD;
			swim_member_status_is_updated(swim, m);
		}
		swim_schedule_ping(swim, m);
		rlist_del_entry(m, in_queue_wait_ack);
	}
}

/**
 * SWIM member attributes from anti-entropy and dissemination
 * messages.
 */
struct swim_member_def {
	struct sockaddr_in addr;
	uint64_t incarnation;
	enum swim_member_status status;
	const char *payload;
	int payload_size;
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
		member = swim_member_new(swim, &def->addr, def->status,
					 def->incarnation, def->payload,
					 def->payload_size);
		if (member == NULL)
			diag_log();
		return;
	}
	struct swim_member *self = swim->self;
	if (member != self) {
		swim_member_update_status(swim, member, def->status,
					  def->incarnation);
		if (swim_member_update_payload(swim, member, def->incarnation,
					       def->payload,
					       def->payload_size) != 0)
			diag_log();
		return;
	}
	uint64_t old_incarnation = self->incarnation;
	/*
	 * It is possible that other instances know a bigger
	 * incarnation of this instance - such thing happens when
	 * the instance restarts and loses its local incarnation
	 * number. It will be restored by receiving dissemination
	 * messages about self.
	 */
	if (self->incarnation < def->incarnation)
		self->incarnation = def->incarnation;
	if (def->status != MEMBER_ALIVE &&
	    def->incarnation == self->incarnation) {
		/*
		 * In the cluster a gossip exists that this
		 * instance is not alive. Refute this information
		 * with a bigger incarnation.
		 */
		self->incarnation++;
	}
	if (old_incarnation != self->incarnation)
		swim_member_status_is_updated(swim, self);
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
	case SWIM_MEMBER_INCARNATION:
		if (mp_typeof(**pos) != MP_UINT ||
		    mp_check_uint(*pos, end) > 0) {
			say_error("%s member incarnation should be uint",
				  msg_pref);
			return -1;
		}
		def->incarnation = mp_decode_uint(pos);
		break;
	case SWIM_MEMBER_PAYLOAD:
		if (mp_typeof(**pos) != MP_BIN ||\
		    mp_check_binl(*pos, end) > 0) {
			say_error("%s member payload should be bin", msg_pref);
			return -1;
		}
		uint32_t len;
		def->payload = mp_decode_bin(pos, &len);
		if (len > MAX_PAYLOAD_SIZE) {
			say_error("%s member payload size should be <= %d",
				  msg_pref, MAX_PAYLOAD_SIZE);
			return -1;
		}
		def->payload_size = (int) len;
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

/**
 * Decode a failure detection message. Schedule pings, process
 * acks.
 */
static int
swim_process_failure_detection(struct swim *swim, const char **pos,
			       const char *end, const struct sockaddr_in *src)
{
	const char *msg_pref = "Invalid SWIM failure detection message:";
	if (mp_typeof(**pos) != MP_MAP || mp_check_map(*pos, end) > 0) {
		say_error("%s root should be a map", msg_pref);
		return -1;
	}
	uint64_t size = mp_decode_map(pos);
	if (size != 2) {
		say_error("%s root map should have two keys - message type "\
			  "and incarnation", msg_pref);
		return -1;
	}
	enum swim_fd_msg_type type = swim_fd_msg_type_MAX;
	uint64_t incarnation = 0;
	for (int i = 0; i < (int) size; ++i) {
		if (mp_typeof(**pos) != MP_UINT ||
		    mp_check_uint(*pos, end) > 0) {
			say_error("%s a key should be uint", msg_pref);
			return -1;
		}
		uint64_t key = mp_decode_uint(pos);
		switch(key) {
		case SWIM_FD_MSG_TYPE:
			if (mp_typeof(**pos) != MP_UINT ||
			    mp_check_uint(*pos, end) > 0) {
				say_error("%s message type should be uint",
					  msg_pref);
				return -1;
			}
			key = mp_decode_uint(pos);
			if (key >= swim_fd_msg_type_MAX) {
				say_error("%s unknown message type", msg_pref);
				return -1;
			}
			type = key;
			break;
		case SWIM_FD_INCARNATION:
			if (mp_typeof(**pos) != MP_UINT ||
			    mp_check_uint(*pos, end) > 0) {
				say_error("%s incarnation should be uint",
					  msg_pref);
				return -1;
			}
			incarnation = mp_decode_uint(pos);
			break;
		default:
			say_error("%s unknown key", msg_pref);
			return -1;
		}
	}
	if (type == swim_fd_msg_type_MAX) {
		say_error("%s message type should be specified", msg_pref);
		return -1;
	}
	struct swim_member *sender = swim_find_member(swim, src);
	if (sender == NULL) {
		sender = swim_member_new(swim, src, MEMBER_ALIVE, incarnation,
					 NULL, 0);
		if (sender == NULL) {
			diag_log();
			return 0;
		}
	} else {
		swim_member_update_status(swim, sender, MEMBER_ALIVE,
					  incarnation);
	}
	if (type == SWIM_FD_MSG_PING) {
		swim_schedule_ack(swim, sender);
	} else {
		assert(type == SWIM_FD_MSG_ACK);
		if (incarnation >= sender->incarnation) {
			sender->failed_pings = 0;
			rlist_del_entry(sender, in_queue_wait_ack);
		}
	}
	return 0;
}

static int
swim_process_dissemination(struct swim *swim, const char **pos, const char *end)
{
	const char *msg_pref = "Invald SWIM dissemination message:";
	if (mp_typeof(**pos) != MP_ARRAY || mp_check_array(*pos, end) > 0) {
		say_error("%s message should be an array", msg_pref);
		return -1;
	}
	uint64_t size = mp_decode_array(pos);
	for (uint64_t i = 0; i < size; ++i) {
		if (mp_typeof(**pos) != MP_MAP ||
		    mp_check_map(*pos, end) > 0) {
			say_error("%s event should be map", msg_pref);
			return -1;
		}
		uint64_t map_size = mp_decode_map(pos);
		struct swim_member_def def;
		swim_member_def_create(&def);
		for (uint64_t j = 0; j < map_size; ++j) {
			if (mp_typeof(**pos) != MP_UINT ||
			    mp_check_uint(*pos, end) > 0) {
				say_error("%s event key should be uint",
					  msg_pref);
				return -1;
			}
			uint64_t key = mp_decode_uint(pos);
			if (key >= swim_member_key_MAX) {
				say_error("%s unknown event key", msg_pref);
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
		case SWIM_FAILURE_DETECTION:
			say_verbose("SWIM: process failure detection");
			if (swim_process_failure_detection(swim, &pos, end,
							   src) != 0)
				return;
			break;
		case SWIM_DISSEMINATION:
			say_verbose("SWIM: process dissemination");
			if (swim_process_dissemination(swim, &pos, end) != 0)
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

	/* Failure detection component. */
	rlist_create(&swim->queue_wait_ack);
	ev_init(&swim->wait_ack_tick, swim_check_acks);
	ev_periodic_set(&swim->wait_ack_tick, 0, ACK_TIMEOUT, NULL);
	swim->wait_ack_tick.data = (void *) swim;

	/* Dissemination events. */
	rlist_create(&swim->queue_events);

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
		new_self = swim_member_new(swim, &addr, MEMBER_ALIVE, 0, NULL,
					   0);
		if (new_self == NULL)
			return -1;
	}
	if (swim_scheduler_bind(&swim->scheduler, &addr) != 0) {
		swim_member_delete(swim, new_self);
		return -1;
	}
	ev_periodic_start(loop(), &swim->round_tick);
	ev_periodic_start(loop(), &swim->wait_ack_tick);

	if (swim->round_tick.interval != heartbeat_rate && heartbeat_rate > 0)
		ev_periodic_set(&swim->round_tick, 0, heartbeat_rate, NULL);

	if (new_self != NULL) {
		swim->self = new_self;
		cached_round_msg_invalidate(swim);
	}
	if (new_transport != NULL) {
		swim->transport = new_transport;
		swim_scheduler_set_transport(&swim->scheduler, new_transport);
	}
	return 0;
}

int
swim_set_payload(struct swim *swim, const char *payload, int payload_size)
{
	if (payload_size > MAX_PAYLOAD_SIZE) {
		diag_set(IllegalParams, "Payload should be <= %d",
			 MAX_PAYLOAD_SIZE);
		return -1;
	}
	return swim_member_update_payload(swim, swim->self,
					  swim->self->incarnation, payload,
					  payload_size);
}

int
swim_add_member(struct swim *swim, const char *uri)
{
	struct sockaddr_in addr;
	if (uri_to_addr(uri, &addr) != 0)
		return -1;
	struct swim_member *member = swim_find_member(swim, &addr);
	if (member == NULL) {
		member = swim_member_new(swim, &addr, MEMBER_ALIVE, 0, NULL, 0);
		if (member == NULL)
			return -1;
		member->is_pinned = true;
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
	if (member != NULL) {
		rlist_del_entry(member, in_queue_events);
		swim_member_delete(swim, member);
	}
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
		info_append_int(info, "incarnation",
				(int64_t) member->incarnation);
		info_table_end(info);
	}
	info_end(info);
}

void
swim_delete(struct swim *swim)
{
	swim_scheduler_destroy(&swim->scheduler);
	ev_periodic_stop(loop(), &swim->round_tick);
	ev_periodic_stop(loop(), &swim->wait_ack_tick);
	swim_task_destroy(&swim->round_step_task);
	mh_int_t node = mh_first(swim->members);
	while (node != mh_end(swim->members)) {
		struct swim_member *m = (struct swim_member *)
			mh_i64ptr_node(swim->members, node)->val;
		rlist_del_entry(m, in_queue_events);
		swim_member_delete(swim, m);
		node = mh_first(swim->members);
	}
	mh_i64ptr_delete(swim->members);
	free(swim->shuffled_members);
	cached_round_msg_invalidate(swim);
}
