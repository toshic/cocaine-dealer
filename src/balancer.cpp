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

#include <msgpack.hpp>

#include <boost/thread.hpp>

#include "cocaine/dealer/utils/error.hpp"
#include "cocaine/dealer/utils/networking.hpp"
#include "cocaine/dealer/core/balancer.hpp"

namespace cocaine {
namespace dealer {

balancer_t::balancer_t(const std::string& identity,
					   const std::set<cocaine_endpoint_t>& endpoints,
					   const boost::shared_ptr<context_t>& ctx,
					   bool logging_enabled) :
	dealer_object_t(ctx, logging_enabled),
	m_endpoints(endpoints),
	m_current_endpoint_index(0),
	m_socket_identity(identity)
{
	create_socket();

	std::set<cocaine_endpoint_t>::iterator it = m_endpoints.begin();
	for (; it != m_endpoints.end(); ++it) {
		m_endpoints_vec.push_back(*it);
	}

	// if (log_enabled(PLOG_DEBUG)) {
	// 	log(PLOG_DEBUG, "connect " + m_socket_identity);
	// }

	if (m_endpoints.empty()) {
		return;
	}

	connect_socket(m_endpoints);
}

balancer_t::~balancer_t() {
	if (!m_socket) {
		return;
	}

	if (log_enabled(PLOG_DEBUG)) {
		log(PLOG_DEBUG, "disconnect balancer " + m_socket_identity);
	}

	m_socket.reset();
}

void
balancer_t::connect_socket(const std::set<cocaine_endpoint_t>& endpoints) {
	assert(m_socket);

	if (endpoints.empty()) {
		return;
	}

	std::string connection_str;
	try {
		std::set<cocaine_endpoint_t>::const_iterator it = endpoints.begin();

		log(PLOG_DEBUG, "connected %s to endpoints: ", m_socket_identity.c_str());
		for (; it != endpoints.end(); ++it) {
			connection_str = it->endpoint;
			log(PLOG_DEBUG, connection_str);
			m_socket->connect(connection_str.c_str());
		}
	}
	catch (const std::exception& ex) {
		std::string error_msg = "balancer with identity " + m_socket_identity + " could not connect to ";
		error_msg += connection_str + " at " + std::string(BOOST_CURRENT_FUNCTION);
		throw internal_error(error_msg);
	}
}

void
balancer_t::disconnect_socket(const std::set<cocaine_endpoint_t>& endpoints) {
	assert(m_socket);

	if (endpoints.empty()) {
		return;
	}

	std::string connection_str;
	try {
		std::set<cocaine_endpoint_t>::const_iterator it = endpoints.begin();

		log(PLOG_DEBUG, "disconnected %s from endpoints: ", m_socket_identity.c_str());
		for (; it != endpoints.end(); ++it) {
			connection_str = it->endpoint;
			log(PLOG_DEBUG, connection_str);
			m_socket->disconnect(connection_str.c_str());
		}
	}
	catch (const std::exception& ex) {
		std::string error_msg = "balancer with identity " + m_socket_identity + " could not disconnect from ";
		error_msg += connection_str + " at " + std::string(BOOST_CURRENT_FUNCTION);
		throw internal_error(error_msg);
	}
}

void
balancer_t::update_endpoints(const std::set<cocaine_endpoint_t>& updated_endpoints,
							 std::set<cocaine_endpoint_t>& missing_endpoints)
{
	static const double delta = 0.00001;
	std::set<cocaine_endpoint_t> new_endpoints;

	// get missing endpoints
	std::set<cocaine_endpoint_t>::iterator curr_it = m_endpoints.begin();
	for (; curr_it != m_endpoints.end(); ++curr_it) {

		std::set<cocaine_endpoint_t>::iterator upd_it = updated_endpoints.find(*curr_it);
		
		if (upd_it != updated_endpoints.end()) {
			if (curr_it->weight > delta && upd_it->weight < delta) {
				missing_endpoints.insert(*upd_it);
			}
		}
		else {
			if (curr_it->weight > delta) {
				missing_endpoints.insert(*upd_it);
			}
		}
	}

	// get new endpoints
	std::set<cocaine_endpoint_t>::iterator upd_it = updated_endpoints.begin();
	for (; upd_it != updated_endpoints.end(); ++upd_it) {
	
		std::set<cocaine_endpoint_t>::iterator curr_it = m_endpoints.find(*upd_it);

		if (curr_it != m_endpoints.end()) {
		 	if (curr_it->weight < delta && upd_it->weight > delta) {
				new_endpoints.insert(*upd_it);
			}
		}
		else {
			if (upd_it->weight > delta) {
				new_endpoints.insert(*upd_it);
			}	
		}
	}

	// replace current endpoints data
	m_endpoints.clear();
	m_endpoints.insert(updated_endpoints.begin(), updated_endpoints.end());

	m_endpoints_vec.clear();
	curr_it = m_endpoints.begin();
	for (; curr_it != m_endpoints.end(); ++curr_it) {

		if (curr_it->weight > delta) {
			m_endpoints_vec.push_back(*curr_it);
		}
	}

	connect_socket(new_endpoints);
	disconnect_socket(missing_endpoints);

	m_current_endpoint_index = 0;
}

balancer_t::shared_socket_t
balancer_t::socket() {
	return m_socket;
}

void
balancer_t::create_socket() {
	if (log_enabled(PLOG_DEBUG)) {
		log(PLOG_DEBUG, "create_socket " + m_socket_identity);
	}

	try {
		int timeout = balancer_t::socket_timeout;
		m_socket.reset(new socket_t(context(), ZMQ_ROUTER));
		m_socket->set_sockopt(ZMQ_LINGER, &timeout, sizeof(timeout));

		#if ZMQ_VERSION_MAJOR < 3
			int64_t hwm = balancer_t::socket_hwm;
			m_socket->set_sockopt(ZMQ_HWM, &hwm, sizeof(hwm));
		#else
			int hwm = balancer_t::socket_hwm;
			m_socket->set_sockopt(ZMQ_SNDHWM, &hwm, sizeof(hwm));
			m_socket->set_sockopt(ZMQ_RCVHWM, &hwm, sizeof(hwm));
		#endif

		m_socket->set_sockopt(ZMQ_IDENTITY, m_socket_identity.c_str(), m_socket_identity.length());
	}
	catch (const zmq::error_t& ex) {
		log(PLOG_ERROR, "could not recreate socket, details: %s", ex.what());
		throw(ex);
	}
	catch (const std::exception& ex) {
		log(PLOG_ERROR, "could not recreate socket, details: %s", ex.what());
		throw(ex);
	}

	m_fd = m_socket->fd();
}

cocaine_endpoint_t&
balancer_t::get_next_endpoint() {
	// increment iter
	if (m_current_endpoint_index < m_endpoints_vec.size() - 1) {
		++m_current_endpoint_index;
	}
	else {
		m_current_endpoint_index = 0;
	}
	
	return m_endpoints_vec[m_current_endpoint_index];
}

bool
balancer_t::check_for_responses(int poll_timeout) const {
	assert(m_socket);

	// poll for responce
	zmq_pollitem_t poll_items[1];
	poll_items[0].socket = m_socket->zmq_socket();
	poll_items[0].fd = 0;
	poll_items[0].events = ZMQ_POLLIN;
	poll_items[0].revents = 0;

	int socket_response = zmq_poll(poll_items, 1, poll_timeout);

	if (socket_response <= 0) {
		return false;
	}

	// in case we received response
	if ((ZMQ_POLLIN & poll_items[0].revents) == ZMQ_POLLIN) {
		return true;
	}

    return false;
}

bool
balancer_t::send(boost::shared_ptr<message_iface>& message, cocaine_endpoint_t& endpoint) {
	assert(m_socket);

	try {
		// send ident
		endpoint = get_next_endpoint();
		message->set_destination_endpoint(endpoint.as_string());

		if (true != m_socket->send(endpoint.route, ZMQ_SNDMORE)) {
			return false;
		}

		// send header
		if (true != m_socket->send_empty(ZMQ_SNDMORE)) {
			return false;
		}

		// send message uuid
		if (true != m_socket->send_packed(message->uuid().as_string(), ZMQ_SNDMORE)) {
			return false;
		}

		// send message policy
		policy_t server_policy = message->policy().server_policy();

		if (server_policy.deadline > 0.0) {
			// awful semantics! convert deadline [timeout value] to actual [deadline time]
			time_value server_deadline = message->enqued_timestamp();
			server_deadline += server_policy.deadline;
			server_policy.deadline = server_deadline.as_double();
		}

		if (true != m_socket->send_packed(server_policy, ZMQ_SNDMORE)) {
			return false;
		}

		// send data
		size_t data_size = message->size();
		zmq::message_t data_chunk(data_size);

		if (data_size > 0) {
			message->load_data();
			memcpy((void *)data_chunk.data(), message->data(), data_size);
			message->unload_data();
		}

		if (true != m_socket->send(data_chunk)) {
			return false;
		}
	}
	catch (const std::exception& ex) {
		std::string error_msg = "balancer with identity " + m_socket_identity;
		error_msg += " could not send message, details: ";
		error_msg += ex.what();
		throw internal_error(error_msg);
	}

	return true;
}

bool
balancer_t::is_valid_rpc_code(int rpc_code) {
	switch (rpc_code) {
		case SERVER_RPC_MESSAGE_ACK:
		case SERVER_RPC_MESSAGE_CHUNK:
		case SERVER_RPC_MESSAGE_CHOKE:
		case SERVER_RPC_MESSAGE_ERROR:
			return true;
		default:
			return false;
	}

	return false;
}

bool
balancer_t::receive(boost::shared_ptr<response_chunk_t>& response) {
	std::string			route;
	std::string			uuid;
	std::string			data;
	int					rpc_code;

	std::string 		error_message;

	// receive route
	if (!m_socket->recv(route, ZMQ_NOBLOCK)) {
		return false;
	}

	// receive rpc code
	if (!m_socket->recv_packed(rpc_code, ZMQ_NOBLOCK)) {
		return false;
	}

	if (!is_valid_rpc_code(rpc_code)) {
		m_socket->drop();
		return false;
	}

	// receive uuid
	if (!m_socket->recv_packed(uuid, ZMQ_NOBLOCK)) {
		return false;
	}

	// init response
	response.reset(new response_chunk_t);
	response->uuid		= uuid;
	response->route		= route;
	response->rpc_code	= rpc_code;

	// receive all data
	switch (rpc_code) {
		case SERVER_RPC_MESSAGE_CHUNK: {
			zmq::message_t chunk;
			if (!m_socket->recv(&chunk, ZMQ_NOBLOCK)) {
				return false;
			}

			response->data = data_container(chunk.data(), chunk.size());
		}
		break;

		case SERVER_RPC_MESSAGE_ERROR: {
			// receive error code
			if (!m_socket->recv_packed(response->error_code, ZMQ_NOBLOCK)) {
				return false;
			}

			// receive error message
			if (!m_socket->recv_packed(response->error_message, ZMQ_NOBLOCK)) {
				return false;
			}
		}
		break;

		default:
		break;
	}

	m_socket->drop();

	// log the info we received
	std::string message = "response from: %s for msg with uuid: %s, type: ";

	switch (rpc_code) {
		case SERVER_RPC_MESSAGE_ACK: {
			if (log_enabled(PLOG_DEBUG)) {
				wuuid_t id(uuid);
				std::string readable_uuid = id.as_human_readable_string();
				
				std::string times = time_value::get_current_time().as_string();
				message += "ACK (%s)";

				log(PLOG_DEBUG,
					message,
					route.c_str(),
					readable_uuid.c_str(),
					times.c_str());
			}
		}
		break;

		case SERVER_RPC_MESSAGE_CHUNK: {
			if (log_enabled(PLOG_DEBUG)) {
				wuuid_t id(uuid);
				std::string readable_uuid = id.as_human_readable_string();

				std::string times = time_value::get_current_time().as_string();
				message += "CHUNK (%s)";

				log(PLOG_DEBUG,
					message,
					route.c_str(),
					readable_uuid.c_str(),
					times.c_str());
			}
		}
		break;

		case SERVER_RPC_MESSAGE_CHOKE: {
			if (log_enabled(PLOG_DEBUG)) {
				wuuid_t id(uuid);
				std::string readable_uuid = id.as_human_readable_string();

				std::string times = time_value::get_current_time().as_string();
				message += "CHOKE (%s)";

				log(PLOG_DEBUG,
					message,
					route.c_str(),
					readable_uuid.c_str(),
					times.c_str());
			}
		}
		break;

		case SERVER_RPC_MESSAGE_ERROR: {
			if (log_enabled(PLOG_ERROR)) {
				wuuid_t id(uuid);
				std::string readable_uuid = id.as_human_readable_string();

				std::string times = time_value::get_current_time().as_string();
				message += "ERROR (%s), error message: %s, error code: %d";

				log(PLOG_ERROR,
					message,
					route.c_str(),
					readable_uuid.c_str(),
					times.c_str(),
					response->error_message.c_str(),
					response->error_code);
			}
		}
		break;

		default:
			return false;
	}

	return true;
}

} // namespace dealer
} // namespace cocaine
