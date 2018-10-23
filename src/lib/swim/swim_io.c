#include "swim_io.h"
#include "fiber.h"

static ssize_t
swim_udp_send_msg(int fd, const void *data, size_t size,
		  const struct sockaddr *addr, socklen_t addr_size)
{
	ssize_t ret = sio_sendto(fd, data, size, 0, addr, addr_size);
	if (ret == -1 && sio_wouldblock(errno))
		return 0;
	return ret;
}

static ssize_t
swim_udp_recv_msg(int fd, void *buffer, size_t size, struct sockaddr *addr,
		  socklen_t *addr_size)
{
	ssize_t ret = sio_recvfrom(fd, buffer, size, 0, addr, addr_size);
	if (ret == -1 && sio_wouldblock(errno))
		return 0;
	return ret;
}

struct swim_transport swim_udp_transport = {
	/* .send_round_msg = */ swim_udp_send_msg,
	/* .send_ping = */ swim_udp_send_msg,
	/* .send_ack = */ swim_udp_send_msg,
	/* .recv_msg = */ swim_udp_recv_msg,
};

struct swim_packet *
swim_packet_new(struct swim_msg *msg)
{
	struct swim_packet *res =
		(struct swim_packet *) malloc(sizeof(*res));
	if (res == NULL) {
		diag_set(OutOfMemory, sizeof(*res), "malloc", "res");
		return NULL;
	}
	swim_packet_create(res, msg);
	return res;
}

struct swim_task *
swim_task_new(struct swim_scheduler *scheduler, swim_task_f complete)
{
	struct swim_task *task = (struct swim_task *) malloc(sizeof(*task));
	if (task == NULL) {
		diag_set(OutOfMemory, sizeof(*task), "malloc", "task");
		return NULL;
	}
	swim_task_create(task, scheduler, complete);
	return task;
}

void
swim_task_schedule(struct swim_task *task, swim_transport_send_f send,
		   const struct sockaddr_in *dst)
{
	assert(! swim_task_is_active(task));
	task->send = send;
	task->dst = *dst;
	rlist_add_tail_entry(&task->scheduler->queue_output, task,
			     in_queue_output);
	ev_io_start(loop(), &task->scheduler->output);
}

static void
swim_scheduler_on_output(struct ev_loop *loop, struct ev_io *io, int events);

static void
swim_scheduler_on_input(struct ev_loop *loop, struct ev_io *io, int events);

void
swim_scheduler_create(struct swim_scheduler *scheduler,
		      swim_scheduler_on_input_f on_input,
		      const struct swim_transport *transport)
{
	ev_init(&scheduler->output, swim_scheduler_on_output);
	scheduler->output.data = (void *) scheduler;
	ev_init(&scheduler->input, swim_scheduler_on_input);
	scheduler->input.data = (void *) scheduler;
	rlist_create(&scheduler->queue_output);
	scheduler->on_input = on_input;
	swim_scheduler_set_transport(scheduler, transport);
}

int
swim_scheduler_bind(struct swim_scheduler *scheduler, struct sockaddr_in *addr)
{
	struct sockaddr_in cur_addr;
	socklen_t addrlen = sizeof(cur_addr);
	int old_fd = scheduler->input.fd;

	if (old_fd != -1 &&
	    getsockname(old_fd, (struct sockaddr *) &cur_addr, &addrlen) == 0 &&
	    addr->sin_addr.s_addr == cur_addr.sin_addr.s_addr &&
	    addr->sin_port == cur_addr.sin_port)
		return 0;

	int fd = sio_socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
	if (fd < 0)
		return -1;
	if (sio_bind(fd, (struct sockaddr *) addr, sizeof(*addr)) != 0 ||
	    evio_setsockopt_server(fd, AF_INET, SOCK_DGRAM) != 0) {
		if (errno == EADDRINUSE)
			diag_set(SocketError, sio_socketname(fd), "bind");
		close(fd);
		return -1;
	}
	close(old_fd);
	ev_io_set(&scheduler->input, fd, EV_READ);
	ev_io_set(&scheduler->output, fd, EV_WRITE);
	return 0;
}

void
swim_scheduler_destroy(struct swim_scheduler *scheduler)
{
	close(scheduler->input.fd);
	ev_io_stop(loop(), &scheduler->output);
	ev_io_stop(loop(), &scheduler->input);
}

static void
swim_scheduler_on_output(struct ev_loop *loop, struct ev_io *io, int events)
{
	assert((events & EV_WRITE) != 0);
	(void) events;
	struct swim_scheduler *scheduler = (struct swim_scheduler *) io->data;
	if (rlist_empty(&scheduler->queue_output)) {
		ev_io_stop(loop, io);
		return;
	}
	struct swim_task *task =
		rlist_shift_entry(&scheduler->queue_output, struct swim_task,
				  in_queue_output);
	say_verbose("SWIM: send to %s",
		    sio_strfaddr((struct sockaddr *) &task->dst,
				 sizeof(task->dst)));
	for (struct swim_packet *packet = swim_msg_first_packet(&task->msg);
	     packet != NULL; packet = swim_packet_next(packet)) {
		if (task->send(io->fd, packet->body, packet->pos - packet->body,
			       (struct sockaddr *) &task->dst,
			       sizeof(task->dst)) == -1)
			diag_log();
	}
	task->complete(task);
}

static void
swim_scheduler_on_input(struct ev_loop *loop, struct ev_io *io, int events)
{
	assert((events & EV_READ) != 0);
	(void) events;
	(void) loop;
	struct swim_scheduler *scheduler = (struct swim_scheduler *) io->data;
	struct sockaddr_in addr;
	socklen_t len = sizeof(addr);
	struct swim_packet packet;
	struct swim_msg msg;
	swim_msg_create(&msg);
	swim_packet_create(&packet, &msg);
	swim_transport_recv_f recv = scheduler->transport->recv_msg;
	ssize_t size = recv(io->fd, packet.body, packet.end - packet.body,
			    (struct sockaddr *) &addr, &len);
	if (size <= 0) {
		if (size < 0)
			diag_log();
		return;
	}
	swim_packet_advance(&packet, size);
	swim_packet_flush(&packet);
	say_verbose("SWIM: received from %s",
		    sio_strfaddr((struct sockaddr *) &addr, len));
	scheduler->on_input(scheduler, &packet, &addr);
}
