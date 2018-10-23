#ifndef TARANTOOL_SWIM_IO_H_INCLUDED
#define TARANTOOL_SWIM_IO_H_INCLUDED
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
#include "trivia/util.h"
#include "small/rlist.h"
#include "salad/stailq.h"
#include "swim_transport.h"
#include "evio.h"
#include <stdbool.h>
#include <arpa/inet.h>

#if defined(__cplusplus)
extern "C" {
#endif

struct swim_task;
struct swim_scheduler;

/** UDP sendto/recvfrom implementation of swim_transport. */
extern struct swim_transport swim_udp_transport;

enum {
	/**
	 * Default MTU is 1500. MTU (when IPv4 is used) consists
	 * of IPv4 header, UDP header, Data. IPv4 has 20 bytes
	 * header, UDP - 8 bytes. So Data = 1500 - 20 - 8 = 1472.
	 * TODO: adapt to other MTUs which can be reduced in some
	 * networks by their admins.
	 */
	UDP_PACKET_SIZE = 1472,
};

/**
 * UDP body size is limited by definition. To be able to send a
 * big message it should be split into multiple packets. Each
 * packet is self-sufficient piece of data, which can withstand
 * loss of other packets and be processed independently.
 */
struct swim_packet {
	/** Place in the whole big message, struct swim_msg. */
	struct stailq_entry in_msg;
	/** Last valid position in the body. */
	char *pos;
	/** Position beyond pos, contains unfinished data. */
	char *next_pos;
	/** Packet body. */
	char body[UDP_PACKET_SIZE];
	/**
	 * Pointer to the end of the body. Just syntax sugar to do
	 * not write 'body + sizeof(body)' each time.
	 */
	char end[0];
};

struct swim_msg {
	struct stailq packets;
};

static inline bool
swim_packet_is_last(struct swim_packet *packet)
{
	return stailq_next(&packet->in_msg) == NULL;
}

static inline char *
swim_packet_reserve(struct swim_packet *packet, int size)
{
	return packet->next_pos + size > packet->end ? NULL : packet->next_pos;
}

static inline void
swim_packet_advance(struct swim_packet *packet, int size)
{
	assert(packet->next_pos + size <= packet->end);
	packet->next_pos += size;
}

static inline char *
swim_packet_alloc(struct swim_packet *packet, int size)
{
	char *res = swim_packet_reserve(packet, size);
	if (res == NULL)
		return NULL;
	swim_packet_advance(packet, size);
	return res;
}

static inline void
swim_packet_flush(struct swim_packet *packet)
{
	assert(packet->next_pos >= packet->pos);
	packet->pos = packet->next_pos;
}

static inline struct swim_packet *
swim_packet_next(struct swim_packet *packet)
{
	if (swim_packet_is_last(packet))
		return NULL;
	return stailq_next_entry(packet, in_msg);
}

static inline void
swim_packet_delete(struct swim_packet *packet)
{
	free(packet);
}

static inline void
swim_packet_create(struct swim_packet *packet, struct swim_msg *msg)
{
	stailq_add_tail_entry(&msg->packets, packet, in_msg);
	packet->pos = packet->body;
	packet->next_pos = packet->body;
}

struct swim_packet *
swim_packet_new(struct swim_msg *msg);

static inline bool
swim_msg_is_empty(struct swim_msg *msg)
{
	return stailq_empty(&msg->packets);
}

static inline struct swim_packet *
swim_msg_first_packet(struct swim_msg *msg)
{
	if (swim_msg_is_empty(msg))
		return NULL;
	return stailq_first_entry(&msg->packets, struct swim_packet, in_msg);
}

static inline struct swim_packet *
swim_msg_last_packet(struct swim_msg *msg)
{
	if (swim_msg_is_empty(msg))
		return NULL;
	return stailq_last_entry(&msg->packets, struct swim_packet, in_msg);
}

static inline void
swim_msg_create(struct swim_msg *msg)
{
	stailq_create(&msg->packets);
}

static inline void
swim_msg_destroy(struct swim_msg *msg)
{
	struct swim_packet *packet, *tmp;
	stailq_foreach_entry_safe(packet, tmp, &msg->packets, in_msg)
		swim_packet_delete(packet);
}

static inline void
swim_msg_reset(struct swim_msg *msg)
{
	swim_msg_destroy(msg);
	swim_msg_create(msg);
}

static inline struct swim_packet *
swim_msg_reserve(struct swim_msg *msg, int size)
{
	struct swim_packet *packet = swim_msg_last_packet(msg);
	assert(size <= (int) sizeof(packet->body));
	if (packet == NULL || swim_packet_reserve(packet, size) == NULL)
		return swim_packet_new(msg);
	return packet;
}

typedef void (*swim_scheduler_on_input_f)(struct swim_scheduler *scheduler,
					  const struct swim_packet *packet,
					  const struct sockaddr_in *src);

struct swim_scheduler {
	/** Transport used to receive packets. */
	const struct swim_transport *transport;
	/** Function called when a packet is received. */
	swim_scheduler_on_input_f on_input;
	/**
	 * Event dispatcher of incomming messages. Takes them from
	 * network.
	 */
	struct ev_io input;
	/**
	 * Event dispatcher of outcomming messages. Takes tasks
	 * from queue_output.
	 */
	struct ev_io output;
	/** Queue of output tasks ready to write now. */
	struct rlist queue_output;
};

void
swim_scheduler_create(struct swim_scheduler *scheduler,
		      swim_scheduler_on_input_f on_input,
		      const struct swim_transport *transport);

int
swim_scheduler_bind(struct swim_scheduler *scheduler, struct sockaddr_in *addr);

static inline void
swim_scheduler_set_transport(struct swim_scheduler *scheduler,
			     const struct swim_transport *transport)
{
	scheduler->transport = transport;
}

void
swim_scheduler_destroy(struct swim_scheduler *scheduler);

/**
 * Each SWIM component in a common case independently may want to
 * push some data into the network. Dissemination sends events,
 * failure detection sends pings, acks. Anti-entropy sends member
 * tables. The intention to send a data is called IO task and is
 * stored in a queue that is dispatched when output is possible.
 */
typedef void (*swim_task_f)(struct swim_task *);

struct swim_task {
	/** Function to send each packet. */
	swim_transport_send_f send;
	/** Function called when the task has completed. */
	swim_task_f complete;
	/** Message to send. */
	struct swim_msg msg;
	/** Destination address. */
	struct sockaddr_in dst;
	/** Place in a queue of tasks. */
	struct rlist in_queue_output;
	/** SWIM scheduler managing the task. */
	struct swim_scheduler *scheduler;
};

void
swim_task_schedule(struct swim_task *task, swim_transport_send_f send,
		   const struct sockaddr_in *dst);

static inline void
swim_task_create(struct swim_task *task, struct swim_scheduler *scheduler,
		 swim_task_f complete)
{
	memset(task, 0, sizeof(*task));
	task->complete = complete;
	swim_msg_create(&task->msg);
	rlist_create(&task->in_queue_output);
	task->scheduler = scheduler;
}

struct swim_task *
swim_task_new(struct swim_scheduler *scheduler, swim_task_f complete);

static inline bool
swim_task_is_active(struct swim_task *task)
{
	return ! rlist_empty(&task->in_queue_output);
}

static inline void
swim_task_reset(struct swim_task *task)
{
	swim_msg_reset(&task->msg);
}

static inline void
swim_task_destroy(struct swim_task *task)
{
	rlist_del_entry(task, in_queue_output);
	swim_msg_destroy(&task->msg);
}

static inline void
swim_task_delete(struct swim_task *task)
{
	swim_task_destroy(task);
	free(task);
}

#if defined(__cplusplus)
}
#endif

#endif /* TARANTOOL_SWIM_IO_H_INCLUDED */