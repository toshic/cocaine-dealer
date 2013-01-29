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

#ifndef _COCAINE_DEALER_HANDLE_HPP_INCLUDED_
#define _COCAINE_DEALER_HANDLE_HPP_INCLUDED_

#include <string>
#include <map>
#include <memory>
#include <cerrno>

#include <ev++.h>

#include <msgpack.hpp>

#include <boost/shared_ptr.hpp>
#include <boost/utility.hpp>
#include <boost/thread/thread.hpp>
#include <boost/date_time.hpp>
#include <boost/shared_ptr.hpp>

#include "json/json.h"

#include "cocaine/dealer/core/balancer.hpp"
#include "cocaine/dealer/core/handle_info.hpp"
#include "cocaine/dealer/core/message_iface.hpp"
#include "cocaine/dealer/core/message_cache.hpp"
#include "cocaine/dealer/core/dealer_object.hpp"
#include "cocaine/dealer/response_chunk.hpp"
#include "cocaine/dealer/core/cocaine_endpoint.hpp"
#include "cocaine/dealer/utils/progress_timer.hpp"
#include "cocaine/dealer/core/io.hpp"

namespace cocaine {
namespace dealer {

#define CONTROL_MESSAGE_UPDATE 1
#define CONTROL_MESSAGE_ENQUEUE 2

// predeclaration
class handle_t : private boost::noncopyable, public dealer_object_t {
public:
	typedef std::vector<cocaine_endpoint_t> endpoints_list_t;

	typedef boost::shared_ptr<response_chunk_t> response_chunk_prt_t;
	typedef boost::function<void(response_chunk_prt_t)> responce_callback_t;

	typedef boost::shared_ptr<socket_t> shared_socket_t;

public:
	handle_t(const handle_info_t& info,
			 const std::set<cocaine_endpoint_t>& endpoints,
			 const boost::shared_ptr<context_t>& context,
			 bool logging_enabled = true);

	~handle_t();

	// networking
	void connect();
	void update_endpoints(const std::set<cocaine_endpoint_t>& endpoints);

	// responses consumer
	void set_responce_callback(responce_callback_t callback);

	// message processing
	void enqueue_message(const boost::shared_ptr<message_iface>& message);
	void make_all_messages_new();
	void assign_message_queue(const message_cache_t::message_queue_ptr_t& message_queue);

	// info retrieval
	const handle_info_t& info() const;
	std::string description();

	boost::shared_ptr<message_cache_t> messages_cache() const;
	void kill();

private:
	void dispatch_messages();
	void notify_enqueued();
	
	// working with control messages
	void dispatch_control_messages(int type, balancer_t& balancer);
	bool reshedule_message(const std::string& route, const std::string& uuid);

	// working with messages
	bool dispatch_next_available_message(balancer_t& balancer);
	void dispatch_next_available_response(balancer_t& balancer);

	// working with responces
	void enqueue_response(boost::shared_ptr<response_chunk_t>& response);
	void remove_from_persistent_storage(const boost::shared_ptr<response_chunk_t>& response);
	void remove_from_persistent_storage(wuuid_t& uuid,
										const message_policy_t& policy,
										const std::string& alias);

	// event loop callbacks
	void process_control_messages(ev::io& watcher, int type);
	void process_io_messages(ev::io& watcher, int type);
	void terminate(ev::async& as, int type);
	void prepare(ev::prepare& as, int type);
	void process_deadlined_messages(ev::timer& watcher, int type);

private:
	handle_info_t	m_info;
	boost::thread	m_thread;
	boost::mutex	m_mutex;
	volatile bool	m_is_running;
	volatile bool	m_is_connected;

	boost::shared_ptr<ev::async>		m_terminate;
	boost::shared_ptr<ev::prepare>		m_prepare;
	boost::shared_ptr<ev::io>			m_control_watcher;
	boost::shared_ptr<ev::io>			m_io_watcher;
	boost::shared_ptr<ev::timer>		m_deadline_timer;
	boost::shared_ptr<ev::dynamic_loop>	m_event_loop;

	//ev::timer			m_harvester_timer;
	//ev::io			m_balancer_watcher;

	std::set<cocaine_endpoint_t>		m_endpoints;
	boost::shared_ptr<message_cache_t>	m_message_cache;
	boost::shared_ptr<balancer_t>		m_balancer;

	shared_socket_t m_control_socket;
	shared_socket_t m_control_socket_2;

	responce_callback_t m_response_callback;

	progress_timer m_last_response_timer;
	progress_timer m_deadlined_messages_timer;
	progress_timer m_control_messages_timer;
};

} // namespace dealer
} // namespace cocaine

#endif // _COCAINE_DEALER_HANDLE_HPP_INCLUDED_
