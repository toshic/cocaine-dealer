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

#ifndef _COCAINE_DEALER_OVERSEER_HPP_INCLUDED_
#define _COCAINE_DEALER_OVERSEER_HPP_INCLUDED_

#include <string>
#include <map>
#include <set>

#include <boost/thread.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/date_time.hpp>
#include <boost/bind.hpp>
#include <boost/tokenizer.hpp>
#include <boost/function.hpp>
#include <boost/lexical_cast.hpp>

#include "json/json.h"

#include <zmq.hpp>

#include "cocaine/dealer/defaults.hpp"
#include "cocaine/dealer/utils/error.hpp"
#include "cocaine/dealer/utils/uuid.hpp"
#include "cocaine/dealer/utils/progress_timer.hpp"
#include "cocaine/dealer/core/handle_info.hpp"
#include "cocaine/dealer/core/inetv4_host.hpp"
#include "cocaine/dealer/core/dealer_object.hpp"
#include "cocaine/dealer/core/cocaine_endpoint.hpp"
#include "cocaine/dealer/heartbeats/hosts_fetcher_iface.hpp"
#include "cocaine/dealer/cocaine_node_info/cocaine_node_info.hpp"

namespace cocaine {
namespace dealer {

struct announce_t {
	std::string hostname;
	std::string info;
};

class overseer_t : private boost::noncopyable, public dealer_object_t {
public:
	overseer_t(const boost::shared_ptr<context_t>& ctx, bool logging_enabled = true);
	virtual ~overseer_t();

	void run();
	void stop();

	static const int socket_poll_timeout = 6000000; // seconds

	typedef std::vector<cocaine_node_info_t> cocaine_node_list_t;
	typedef std::set<cocaine_endpoint_t> endpoints_set_t;

	// <handle name, endpoint>
	typedef std::map<std::string, endpoints_set_t> handle_endpoints_t;

	// <service name, handles endpoints>
	typedef std::map<std::string, handle_endpoints_t> routing_table_t;

private:
	typedef boost::shared_ptr<hosts_fetcher_iface> hosts_fetcher_ptr;
	typedef boost::shared_ptr<zmq::socket_t> socket_ptr;

	bool fetch_endpoints();
	void main_loop();

	void create_sockets();
	void connect_sockets();
	void kill_sockets();

	std::vector<std::string> poll_sockets();

	void read_from_sockets(const std::vector<std::string>& responded_sockets_ids,
						   std::map<std::string, std::vector<announce_t> >& responces);

	void parse_responces(const std::map<std::string, std::vector<announce_t> >& responces,
						 std::map<std::string, std::vector<cocaine_node_info_t> >& parsed_responses);

	void update_routing_table(const std::map<std::string, cocaine_node_list_t>& parsed_responses);

	// used for debug only
	void print_all_fetched_endpoints();
	void print_routing_table();

private:
	std::vector<hosts_fetcher_ptr> m_endpoints_fetchers;

	// <service, endpoints>
	std::map<std::string, std::set<inetv4_endpoint_t> > m_endpoints;

	// <service, socket>
	std::map<std::string, socket_ptr>	m_sockets;
	routing_table_t						m_routing_table;

	boost::mutex		m_mutex;
	boost::thread		m_thread;
	wuuid_t				m_uuid;
	volatile bool		m_stopping;

	progress_timer		m_last_fetch_timer;
};

} // namespace dealer
} // namespace cocaine

#endif // _COCAINE_DEALER_OVERSEER_HPP_INCLUDED_
