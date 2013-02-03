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

#ifndef _COCAINE_DEALER_SERVICE_HPP_INCLUDED_
#define _COCAINE_DEALER_SERVICE_HPP_INCLUDED_

#include <string>
#include <memory>
#include <map>
#include <vector>

#include <ev++.h>

#include <boost/shared_ptr.hpp>
#include <boost/utility.hpp>
#include <boost/thread/thread.hpp>
#include <boost/function.hpp>

#include "cocaine/dealer/response.hpp"

#include "cocaine/dealer/core/handle.hpp"
#include "cocaine/dealer/core/context.hpp"
#include "cocaine/dealer/core/service_info.hpp"
#include "cocaine/dealer/core/dealer_object.hpp"
#include "cocaine/dealer/core/message_iface.hpp"
#include "cocaine/dealer/core/cocaine_endpoint.hpp"

#include "cocaine/dealer/storage/eblob.hpp"

namespace cocaine {
namespace dealer {

class service_t : private boost::noncopyable, public dealer_object_t {
public:
	typedef std::vector<handle_info_t>			handles_info_list_t;
	typedef boost::shared_ptr<handle_t>			shared_handle_t;
	typedef boost::shared_ptr<response_t>		shared_response_t;
	typedef boost::shared_ptr<message_iface>	shared_message_t;
	typedef std::deque<shared_message_t>		messages_deque_t;
	typedef boost::shared_ptr<messages_deque_t> shared_messages_deque_t;

	typedef std::map<std::string, shared_messages_deque_t> 			unhandled_messages_map_t;
	typedef std::map<std::string, std::vector<cocaine_endpoint_t> > handles_endpoints_t;

public:
	service_t(const service_info_t& info,
			  const boost::shared_ptr<context_t>& ctx,
			  bool logging_enabled = true);

	virtual ~service_t();

	void create_handle(const handle_info_t& handle_info, const std::set<cocaine_endpoint_t>& endpoints);
	void update_handle(const handle_info_t& handle_info, const std::set<cocaine_endpoint_t>& endpoints);
	void destroy_handle(const handle_info_t& handle_info);

	boost::shared_ptr<response_t> send_message(const shared_message_t& message);

	service_info_t info() const;

private:
	void remove_outstanding_handles(const handles_info_list_t& handles_info);

	void enqueue_responce(boost::shared_ptr<response_chunk_t>& response);

	bool enque_to_handle(const shared_message_t& message);
	void enque_to_unhandled(const shared_message_t& message);
	
	void append_to_unhandled(const std::string& handle_name,
							 const shared_messages_deque_t& handle_queue);

	void get_outstanding_handles(const handles_endpoints_t& handles_endpoints,
								 handles_info_list_t& outstanding_handles);

	void get_new_handles(const handles_endpoints_t& handles_endpoints,
						 handles_info_list_t& new_handles);

	shared_messages_deque_t get_and_remove_unhandled_queue(const std::string& handle_name);

	// async callbacks
	void harvest_responces(ev::timer& timer, int type);
	void harvest_messages(ev::timer& timer, int type);

private:
	service_info_t m_info;

	std::unique_ptr<ev::timer>	m_message_harvester;
	std::unique_ptr<ev::timer>	m_responces_harvester;

	// service messages for non-existing handles <handle name, handle ptr>
	unhandled_messages_map_t m_unhandled_messages;

	// responces map <uuid, response>
	std::map<std::string, shared_response_t>	m_responses;

	// handles map (handle name, handle ptr)
	std::map<std::string, shared_handle_t>		m_handles;

	boost::mutex				m_responces_mutex;
	boost::mutex				m_handles_mutex;
	boost::mutex				m_unhandled_mutex;

	static const double message_harvest_interval;
	static const double responces_harvest_interval;
};

} // namespace dealer
} // namespace cocaine

#endif // _COCAINE_DEALER_SERVICE_HPP_INCLUDED_
