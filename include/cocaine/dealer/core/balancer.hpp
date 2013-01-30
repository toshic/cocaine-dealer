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

#ifndef _COCAINE_DEALER_BALANCER_HPP_INCLUDED_
#define _COCAINE_DEALER_BALANCER_HPP_INCLUDED_

#include <vector>
#include <string>

#include <boost/shared_ptr.hpp>
#include <boost/ptr_container/ptr_vector.hpp>

#include "cocaine/dealer/core/io.hpp"

#include "cocaine/dealer/core/message_iface.hpp"
#include "cocaine/dealer/core/dealer_object.hpp"
#include "cocaine/dealer/response_chunk.hpp"
#include "cocaine/dealer/core/cocaine_endpoint.hpp"

namespace cocaine {
namespace dealer {

class balancer_t : private boost::noncopyable, public dealer_object_t {
public:
	typedef boost::shared_ptr<socket_t> shared_socket_t;
	
	balancer_t(const std::string& identity,
			   const std::set<cocaine_endpoint_t>& endpoints,
			   const boost::shared_ptr<context_t>& ctx,
			   bool logging_enabled = true);

	virtual ~balancer_t();

	void update_endpoints(const std::set<cocaine_endpoint_t>& endpoints,
						  std::set<cocaine_endpoint_t>& missing_endpoints);

	bool send(boost::shared_ptr<message_iface>& message, cocaine_endpoint_t& endpoint);
	bool receive(boost::shared_ptr<response_chunk_t>& response);
	bool check_for_responses(int poll_timeout) const;

	shared_socket_t	socket();
	int fd() { return m_fd; }

	static const int socket_timeout = 0;

	#if ZMQ_VERSION_MAJOR < 3
		static const int64_t socket_hwm = 0;
	#else
		static const int socket_hwm = 0;
	#endif

	static bool is_valid_rpc_code(int rpc_code);

private:
	void create_socket();
	void connect_socket(const std::set<cocaine_endpoint_t>& endpoints);

	cocaine_endpoint_t& get_next_endpoint();

private:
	shared_socket_t					m_socket;
	std::set<cocaine_endpoint_t>	m_endpoints;
	std::vector<cocaine_endpoint_t>	m_endpoints_vec;
	size_t							m_current_endpoint_index;
	std::string						m_socket_identity;
	int m_fd;
};

} // namespace dealer
} // namespace cocaine

#endif // _COCAINE_DEALER_BALANCER_HPP_INCLUDED_
