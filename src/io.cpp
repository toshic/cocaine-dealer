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

#include "cocaine/dealer/defaults.hpp"
#include "cocaine/dealer/core/io.hpp"

namespace cocaine {
namespace dealer {

socket_t::socket_t(const boost::shared_ptr<context_t>& context, int type):
	m_socket(*(context->zmq_context()), type),
	m_type(type) {}

socket_t::socket_t(const boost::shared_ptr<context_t>& context, int type, const std::string& ident):
	m_socket(*(context->zmq_context()), type),
	m_type(type),
	m_identity(ident)
{
	set_sockopt(ZMQ_IDENTITY, ident.data(), ident.size());
}

void socket_t::bind(const inetv4_endpoint_t& endpoint) {
	m_socket.bind(endpoint.as_connection_string().c_str());
}

void socket_t::bind(const std::string& endpoint) {
	m_socket.bind(endpoint.c_str());
}

void socket_t::connect(const inetv4_endpoint_t& endpoint) {
	if (can_connect(endpoint)) {
		std::cout << "connected: " << endpoint.as_connection_string() << std::endl;
		m_socket.connect(endpoint.as_connection_string().c_str());
	}
}

void socket_t::connect(const std::string& endpoint) {
	if (can_connect(endpoint)) {
		std::cout << "connected: " << endpoint << std::endl;
		m_socket.connect(endpoint.c_str());
	}
}

void socket_t::disconnect(const inetv4_endpoint_t& endpoint) {
	m_socket.disconnect(endpoint.as_connection_string().c_str());
	
	std::set<inetv4_endpoint_t>::const_iterator it = m_endpoints.find(endpoint);
	if (it != m_endpoints.end()) {
		m_endpoints.erase(it);
	}
}

void socket_t::disconnect(const std::string& endpoint) {
	m_socket.disconnect(endpoint.c_str());

	inetv4_endpoint_t v4_endpoint(endpoint);
	std::set<inetv4_endpoint_t>::const_iterator it = m_endpoints.find(v4_endpoint);
	if (it != m_endpoints.end()) {
		m_endpoints.erase(it);
	}
}

void socket_t::drop() {
	zmq::message_t null;

	while(more()) {
		recv(&null, ZMQ_NOBLOCK);
	}
}

} // namespace dealer
} // namespace cocaine
