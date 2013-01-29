/*
    Copyright (c) 2011-2012 Rim Zaidullin <creator@bash.org.ru>
    Copyright (c) 2011-2012 Other contributors as noted in the AUTHORS file.

    This file is part of Cocaine.

    Cocaine is free software; you can redistribute it and/or modify
    it under the terms of the GNU Lesser General Public License as published by
    the Free Software Foundation; either version 3 of the License, or
    (at your option) any later version.

    Cocaine is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
    GNU Lesser General Public License for more details.

    You should have received a copy of the GNU Lesser General Public License
    along with this program. If not, see <http://www.gnu.org/licenses/>. 
*/

#include <algorithm>

#include <boost/lexical_cast.hpp>

#include "cocaine/dealer/core/handle.hpp"
#include "cocaine/dealer/utils/error.hpp"
#include "cocaine/dealer/utils/uuid.hpp"
#include "cocaine/dealer/utils/progress_timer.hpp"
#include "cocaine/dealer/storage/eblob_storage.hpp"

namespace cocaine {
namespace dealer {

handle_t::handle_t(const handle_info_t& info,
				   const std::set<cocaine_endpoint_t>& endpoints,
				   const boost::shared_ptr<context_t>& ctx,
				   bool logging_enabled) :
	dealer_object_t(ctx, logging_enabled),
	m_info(info),
	m_endpoints(endpoints),
	m_is_running(false),
	m_is_connected(false)
{
	log(PLOG_DEBUG, "CREATED HANDLE " + description());

	// create message cache
	m_message_cache.reset(new message_cache_t(context(), true));

	// create control socket
	m_control_socket.reset(new socket_t(context(), ZMQ_PAIR));

	m_control_socket->set_linger(0);
	m_control_socket->bind("inproc://service_control_");

	// run message dispatch thread
	m_is_running = true;
	m_thread = boost::thread(&handle_t::dispatch_messages, this);
}

handle_t::~handle_t() {
	kill();
}

void
handle_t::update_endpoints(const std::set<cocaine_endpoint_t>& endpoints) {
	if (!m_is_running || endpoints.empty()) {
		return;
	}

	boost::mutex::scoped_lock lock(m_mutex);
	m_endpoints = endpoints;
	lock.unlock();

	log(PLOG_DEBUG, "UPDATE HANDLE " + description());

	// connect to hosts
	int control_message = CONTROL_MESSAGE_UPDATE;
	zmq::message_t message(sizeof(int));
	memcpy((void *)message.data(), &control_message, sizeof(int));
	m_control_socket->send(message);
}

void
handle_t::kill() {
	if (!m_is_running) {
		return;
	}

	m_is_running = false;

	m_terminate->send();

	m_thread.join();

	m_prepare.reset();
	m_terminate.reset();
	m_control_watcher.reset();
	m_io_watcher.reset();
	m_deadline_timer.reset();

	m_event_loop.reset();

	log(PLOG_DEBUG, "DESTROYED HANDLE " + description());
}

void
handle_t::terminate(ev::async& as, int type) {
	m_control_watcher->stop();
	m_terminate->stop();
	m_prepare->stop();
	m_io_watcher->stop();
	m_deadline_timer->stop();

	m_event_loop->unloop(ev::ALL);

	m_control_socket_2->close();
	m_control_socket_2.reset();

	m_balancer.reset();

	m_control_socket->close();
	m_control_socket.reset();
}

void
handle_t::process_io_messages(ev::io& watcher, int type) {
	if (!m_is_running || type != ev::READ) {
		return;
	}

	try {
		while (m_balancer->socket()->pending()) {
			dispatch_next_available_response(*m_balancer);
		}
	}
	catch (const std::exception& ex) {
		std::string error_msg = "some very ugly shit happend while recv on io socket at ";
		error_msg += std::string(BOOST_CURRENT_FUNCTION);
		error_msg += " details: " + std::string(ex.what());
		throw internal_error(error_msg);
	}
}

void
handle_t::process_control_messages(ev::io& watcher, int type) {
	if (!m_is_running || type != ev::READ) {
		return;
	}

	try {
		while (m_control_socket_2->pending()) {
			zmq::message_t reply;
			if (!m_control_socket_2->recv(&reply, ZMQ_NOBLOCK)) {
				return;
			}
			
			int message = 0;
			memcpy((void *)&message, reply.data(), reply.size());

			dispatch_control_messages(message, *m_balancer);
		}
	}
	catch (const std::exception& ex) {
		std::string error_msg = "some very ugly shit happend while recv on control socket at ";
		error_msg += std::string(BOOST_CURRENT_FUNCTION);
		error_msg += " details: " + std::string(ex.what());
		throw internal_error(error_msg);
	}
}

void
handle_t::prepare(ev::prepare& as, int type) {
	if (m_control_socket_2->pending()) {
		m_event_loop->feed_fd_event(m_control_socket_2->fd(), ev::READ);
		m_event_loop->feed_fd_event(m_balancer->socket()->fd(), ev::READ);
	}
}

void
handle_t::dispatch_messages() {
	wuuid_t balancer_uuid;
	balancer_uuid.generate();
	std::string balancer_ident = m_info.as_string() + "." + balancer_uuid.as_human_readable_string();
	m_balancer.reset(new balancer_t(balancer_ident, m_endpoints, context()));
	m_is_connected = true;

	m_control_socket_2.reset(new socket_t(context(), ZMQ_PAIR));
	assert(m_control_socket_2);

	m_control_socket_2->set_linger(0);
	m_control_socket_2->connect("inproc://service_control_");

	m_event_loop.reset(new ev::dynamic_loop);
	m_terminate.reset(new ev::async(*m_event_loop));
	m_prepare.reset(new ev::prepare(*m_event_loop));
	m_control_watcher.reset(new ev::io(*m_event_loop));
	m_io_watcher.reset(new ev::io(*m_event_loop));
	m_deadline_timer.reset(new ev::timer(*m_event_loop));

	m_control_watcher->set<handle_t, &handle_t::process_control_messages>(this);
	m_control_watcher->start(m_control_socket_2->fd(), ev::READ);

	m_io_watcher->set<handle_t, &handle_t::process_io_messages>(this);
	m_io_watcher->start(m_balancer->socket()->fd(), ev::READ);

	m_terminate->set<handle_t, &handle_t::terminate>(this);
	m_terminate->start();

	m_deadline_timer->set<handle_t, &handle_t::process_deadlined_messages>(this);
    m_deadline_timer->start(0, 0.5);

	m_prepare->set<handle_t, &handle_t::prepare>(this);
	m_prepare->start();

	log(PLOG_DEBUG, "started message dispatch for " + description());
	m_event_loop->loop();

	//m_event_loop
	/*

	m_last_response_timer.reset();
	m_deadlined_messages_timer.reset();
	m_control_messages_timer.reset();

	// process messages
	while (m_is_running) {
		// process incoming control messages every 200 msec
		int control_message = 0;
		              
		if (m_control_messages_timer.elapsed().as_double() > 0.2f) {
		      control_message = receive_control_messages(control_socket, 0);
		      m_control_messages_timer.reset();
		}

		if (control_message > 0) {
			dispatch_control_messages(control_message, balancer);
		}

		// send new message if any
		if (m_is_running && m_is_connected) {
			for (int i = 0; i < 100; ++i) { // batching
				if (m_message_cache->new_messages_count() == 0) {
					break;
				}

				dispatch_next_available_message(balancer);
			}
		}

		// check for message responces
		bool received_response = false;

		int fast_poll_timeout = 30;		  // microsecs
		int long_poll_timeout = 300000;   // microsecs

		int response_poll_timeout = fast_poll_timeout;
		if (m_last_response_timer.elapsed().as_double() > 5.0f) {
			response_poll_timeout = long_poll_timeout;
		}

		if (m_is_connected && m_is_running) {
			received_response = balancer.check_for_responses(response_poll_timeout);

			// process received responce(s)
			while (received_response) {
				m_last_response_timer.reset();
				response_poll_timeout = fast_poll_timeout;

				dispatch_next_available_response(balancer);
				received_response = balancer.check_for_responses(response_poll_timeout);
			}
		}

		if (m_is_running) {
			if (m_deadlined_messages_timer.elapsed().as_double() > 1.0f) {
				process_deadlined_messages();
				m_deadlined_messages_timer.reset();
			}
		}
	}

	control_socket.reset();
	log(PLOG_DEBUG, "finished message dispatch for " + description());
	*/
}

void
handle_t::remove_from_persistent_storage(const boost::shared_ptr<response_chunk_t>& response) {
	if (config()->message_cache_type() != PERSISTENT) {
		return;
	}

	boost::shared_ptr<message_iface> sent_msg;
	if (false == m_message_cache->get_sent_message(response->route, response->uuid, sent_msg)) {
		return;
	}

	if (false == sent_msg->policy().persistent) {
		return;
	}

	// remove message from eblob
	boost::shared_ptr<eblob_t> eb = context()->storage()->get_eblob(sent_msg->path().service_alias);
	eb->remove_all(response->uuid.as_string());
}

void
handle_t::remove_from_persistent_storage(wuuid_t& uuid,
										 const message_policy_t& policy,
										 const std::string& alias)
{
	if (config()->message_cache_type() != PERSISTENT) {
		return;
	}

	if (false == policy.persistent) {
		return;
	}

	// remove message from eblob
	boost::shared_ptr<eblob_t> eb = context()->storage()->get_eblob(alias);
	eb->remove_all(uuid.as_string());
}

void
handle_t::dispatch_next_available_response(balancer_t& balancer) {
	boost::shared_ptr<response_chunk_t> response;

	if (!balancer.receive(response)) {
		return;
	}

	boost::shared_ptr<message_iface> sent_msg;

	switch (response->rpc_code) {
		case SERVER_RPC_MESSAGE_ACK:		
			if (m_message_cache->get_sent_message(response->route, response->uuid, sent_msg)) {
				sent_msg->set_ack_received(true);
			}
		break;

		case SERVER_RPC_MESSAGE_CHUNK:
			enqueue_response(response);
		break;

		case SERVER_RPC_MESSAGE_CHOKE:
			enqueue_response(response);

			remove_from_persistent_storage(response);
			m_message_cache->remove_message_from_cache(response->route, response->uuid);
		break;
		
		case SERVER_RPC_MESSAGE_ERROR: {
			// handle resource error
			if (response->error_code == resource_error) {
				if (m_message_cache->reshedule_message(response->route, response->uuid)) {
					notify_enqueued();

					if (log_flag_enabled(PLOG_WARNING)) {
						std::string message_str = "resheduled message with uuid: ";
						message_str += response->uuid.as_human_readable_string();
						message_str += " from " + description() + ", reason: error received, error code: %d";
						message_str += ", error message: " + response->error_message;
						log(PLOG_WARNING, message_str, response->error_code);
					}
				}
				else {
					enqueue_response(response);

					remove_from_persistent_storage(response);
					m_message_cache->remove_message_from_cache(response->route, response->uuid);

					if (log_flag_enabled(PLOG_ERROR)) {
						std::string message_str = "error received for message with uuid: ";
						message_str += response->uuid.as_human_readable_string();
						message_str += " from " + description() + ", error code: %d";
						message_str += ", error message: " + response->error_message;
						log(PLOG_ERROR, message_str, response->error_code);
					}					
				}
			}
			else {
				enqueue_response(response);

				remove_from_persistent_storage(response);
				m_message_cache->remove_message_from_cache(response->route, response->uuid);

				if (log_flag_enabled(PLOG_ERROR)) {
					std::string message_str = "error received for message with uuid: ";
					message_str += response->uuid.as_human_readable_string();
					message_str += " from " + description() + ", error code: %d";
					message_str += ", error message: " + response->error_message;
					log(PLOG_ERROR, message_str, response->error_code);
				}
			}
		}
		break;

		default: {
			enqueue_response(response);

			remove_from_persistent_storage(response);
			m_message_cache->remove_message_from_cache(response->route, response->uuid);

			if (log_flag_enabled(PLOG_ERROR)) {
				std::string message_str = "unknown RPC code received for message with uuid: ";
				message_str += response->uuid.as_human_readable_string();
				message_str += " from " + description() + ", code: %d";
				message_str += ", error message: " + response->error_message;
				log(PLOG_ERROR, message_str, response->error_code);
			}
		}
		break;
	}
}

namespace {
	struct resheduler {
		resheduler(const boost::shared_ptr<message_cache_t>& cache) : m_cache(cache) {
		}

		template <class T> void operator() (const T& obj) {
			m_cache->make_all_messages_new_for_route(obj.route);
		}

	private:
		boost::shared_ptr<message_cache_t> m_cache;
	};
}

void
handle_t::dispatch_control_messages(int type, balancer_t& balancer) {
	if (!m_is_running) {
		return;
	}

	switch (type) {
		case CONTROL_MESSAGE_UPDATE:
			if (m_is_connected) {
				std::set<cocaine_endpoint_t> missing_endpoints;
				balancer.update_endpoints(m_endpoints, missing_endpoints);

				if (!missing_endpoints.empty()) {
					std::for_each(missing_endpoints.begin(), missing_endpoints.end(), resheduler(m_message_cache));
					notify_enqueued();
					//m_message_cache->make_all_messages_new();
				}
			}
			break;

		case CONTROL_MESSAGE_ENQUEUE:
			if (m_is_connected) {
				int t = m_message_cache->new_messages_count();

				while (t > 0) {
					dispatch_next_available_message(balancer);
					t = m_message_cache->new_messages_count();
				}
			}
			break;
	}
}

boost::shared_ptr<message_cache_t>
handle_t::messages_cache() const {
	return m_message_cache;
}

void
handle_t::process_deadlined_messages(ev::timer& watcher, int type) {
	assert(m_message_cache);
	message_cache_t::message_queue_t expired_messages;
	m_message_cache->get_expired_messages(expired_messages);

	if (expired_messages.empty()) {
		return;
	}

	std::string enqued_timestamp_str;
	std::string sent_timestamp_str;
	std::string curr_timestamp_str;

	for (size_t i = 0; i < expired_messages.size(); ++i) {
		if (log_flag_enabled(PLOG_WARNING) || log_flag_enabled(PLOG_ERROR)) {
			enqued_timestamp_str = expired_messages.at(i)->enqued_timestamp().as_string();
			sent_timestamp_str = expired_messages.at(i)->sent_timestamp().as_string();
			curr_timestamp_str = time_value::get_current_time().as_string();
		}

		if (expired_messages.at(i)->is_deadlined()) {
			boost::shared_ptr<response_chunk_t> response(new response_chunk_t);
			response->uuid = expired_messages.at(i)->uuid();
			response->rpc_code = SERVER_RPC_MESSAGE_ERROR;
			response->error_code = deadline_error;
			response->error_message = "message expired in handle";
			enqueue_response(response);

			remove_from_persistent_storage(response->uuid,
										   expired_messages.at(i)->policy(),
										   expired_messages.at(i)->path().service_alias);

			if (log_flag_enabled(PLOG_ERROR)) {
				std::string log_str = "deadline policy exceeded, for message %s, (enqued: %s, sent: %s, curr: %s)";

				log(PLOG_ERROR,
					log_str,
					expired_messages.at(i)->uuid().as_human_readable_string().c_str(),
					enqued_timestamp_str.c_str(),
					sent_timestamp_str.c_str(),
					curr_timestamp_str.c_str());
			}
		}
		else if (expired_messages.at(i)->is_ack_timedout()) {
			if (expired_messages.at(i)->can_retry()) {
				expired_messages.at(i)->increment_retries_count();
				expired_messages.at(i)->reset_ack_timedout();
				m_message_cache->enqueue_with_priority(expired_messages.at(i));
				notify_enqueued();

				if (log_flag_enabled(PLOG_WARNING)) {
					std::string log_str = "no ACK, resheduled message %s, (enqued: %s, sent: %s, curr: %s)";

					log(PLOG_WARNING, log_str,
						expired_messages.at(i)->uuid().as_human_readable_string().c_str(),
						enqued_timestamp_str.c_str(),
						sent_timestamp_str.c_str(),
						curr_timestamp_str.c_str());
				}
			}
			else {
				boost::shared_ptr<response_chunk_t> response(new response_chunk_t);
				response->uuid = expired_messages.at(i)->uuid();
				response->rpc_code = SERVER_RPC_MESSAGE_ERROR;
				response->error_code = request_error;
				response->error_message = "server did not reply with ack in time";
				enqueue_response(response);

				remove_from_persistent_storage(response->uuid,
											   expired_messages.at(i)->policy(),
											   expired_messages.at(i)->path().service_alias);

				if (log_flag_enabled(PLOG_WARNING)) {
					std::string log_str = "reshedule message policy exceeded, did not receive ACK ";
					log_str += "for %s, (enqued: %s, sent: %s, curr: %s)";

					log(PLOG_WARNING, log_str,
						expired_messages.at(i)->uuid().as_human_readable_string().c_str(),
						enqued_timestamp_str.c_str(),
						sent_timestamp_str.c_str(),
						curr_timestamp_str.c_str());
				}
			}
		}
	}
}

void
handle_t::enqueue_response(boost::shared_ptr<response_chunk_t>& response) {
	if (m_response_callback && m_is_running) {
		m_response_callback(response);
	}
}

bool
handle_t::dispatch_next_available_message(balancer_t& balancer) {
	// send new message if any
	if (m_message_cache->new_messages_count() == 0) {
		return false;
	}

	boost::shared_ptr<message_iface> new_msg = m_message_cache->get_new_message();
	cocaine_endpoint_t endpoint;
	if (balancer.send(new_msg, endpoint)) {
		new_msg->mark_as_sent(true);
		m_message_cache->move_new_message_to_sent(endpoint.route);

		if (log_flag_enabled(PLOG_DEBUG)) {
			std::string log_msg = "sent msg with uuid: %s to endpoint: %s with route: %s (%s)";
			std::string sent_timestamp_str = new_msg->sent_timestamp().as_string();

			log(PLOG_DEBUG,
				log_msg.c_str(),
				new_msg->uuid().as_human_readable_string().c_str(),
				endpoint.endpoint.c_str(),
				description().c_str(),
				sent_timestamp_str.c_str());
		}

		return true;
	}
	else {
		log(PLOG_ERROR, "dispatch_next_available_message failed");		
	}

	return false;
}

const handle_info_t&
handle_t::info() const {
	return m_info;
}

std::string
handle_t::description() {
	return m_info.as_string();
}

void
handle_t::make_all_messages_new() {
	assert (m_message_cache);
	m_message_cache->make_all_messages_new();
}

void
handle_t::assign_message_queue(const message_cache_t::message_queue_ptr_t& message_queue) {
	assert (m_message_cache);
	m_message_cache->append_message_queue(message_queue);

	notify_enqueued();
}

void
handle_t::set_responce_callback(responce_callback_t callback) {
	boost::mutex::scoped_lock lock(m_mutex);
	m_response_callback = callback;
}

void
handle_t::enqueue_message(const boost::shared_ptr<message_iface>& message) {
	m_message_cache->enqueue(message);
	notify_enqueued();
}

void
handle_t::notify_enqueued() {
	int control_message = CONTROL_MESSAGE_ENQUEUE;
	zmq::message_t msg(sizeof(int));
	memcpy((void *)msg.data(), &control_message, sizeof(int));
	m_control_socket->send(msg);
}

} // namespace dealer
} // namespace cocaine
