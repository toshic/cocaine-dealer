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
#include <memory>
#include <map>
#include <set>

#include <ev++.h>
    
#include "json/json.h"

#include <boost/shared_ptr.hpp>
#include <boost/date_time.hpp>
#include <boost/bind.hpp>
#include <boost/tokenizer.hpp>
#include <boost/function.hpp>

#include "cocaine/dealer/defaults.hpp"
#include "cocaine/dealer/utils/error.hpp"
#include "cocaine/dealer/utils/uuid.hpp"
#include "cocaine/dealer/core/handle_info.hpp"
#include "cocaine/dealer/core/inetv4_host.hpp"
#include "cocaine/dealer/core/dealer_object.hpp"
#include "cocaine/dealer/core/io.hpp"
#include "cocaine/dealer/core/cocaine_endpoint.hpp"
#include "cocaine/dealer/heartbeats/hosts_fetcher_iface.hpp"
#include "cocaine/dealer/cocaine_node_info/cocaine_node_info.hpp"

namespace cocaine {
namespace dealer {

struct announce_t {
	std::string hostname;
	std::string info;
};

enum e_overseer_event {
	CREATE_HANDLE = 1,
	UPDATE_HANDLE,
	DESTROY_HANDLE
};

class overseer_t : private boost::noncopyable, public dealer_object_t {
public:
	// TYPES
	typedef std::vector<cocaine_node_info_t> cocaine_node_list_t;
	typedef std::set<cocaine_endpoint_t> endpoints_set_t;

	// <handle name, endpoint>
	typedef std::map<std::string, endpoints_set_t> handle_endpoints_t;

	// <service name, handles endpoints>
	typedef std::map<std::string, handle_endpoints_t> routing_table_t;

	typedef boost::function<void(e_overseer_event event_type,
								 const std::string& service_name,
								 const std::string& handle_name,
								 const endpoints_set_t& endpoints)> callback_t;

	// API
	overseer_t(const boost::shared_ptr<context_t>& ctx, bool logging_enabled = true);
	virtual ~overseer_t();

	void run();
	void stop();
	void set_callback(callback_t callback);

private:
	typedef boost::shared_ptr<hosts_fetcher_iface> hosts_fetcher_ptr;
	typedef boost::shared_ptr<socket_t> shared_socket_t;

	bool fetch_endpoints(std::map<std::string, std::set<inetv4_endpoint_t> >& new_hosts,
						 std::map<std::string, std::set<inetv4_endpoint_t> >& missing_hosts);

	void create_sockets();
	void connect_sockets(std::map<std::string, std::set<inetv4_endpoint_t> >& new_endpoints);
	void kill_sockets();

	void read_from_sockets(std::map<std::string, std::vector<announce_t> >& responces);

	void parse_responces(const std::map<std::string, std::vector<announce_t> >& responces,
						 std::map<std::string, std::vector<cocaine_node_info_t> >& parsed_responses);

	void routing_table_from_responces(const std::map<std::string, cocaine_node_list_t>& parsed_responses,
									  routing_table_t& routing_table);
	
	void update_main_routing_table(routing_table_t& routing_table_update);

	bool handle_exists_for_service(routing_table_t& routing_table,
								   const std::string& service_name,
								   const std::string& handle_name,
								   handle_endpoints_t::iterator& it);

	bool service_from_table(routing_table_t& routing_table,
							const std::string& service_name,
							routing_table_t::iterator& it);

	
	bool endpoints_set_equal(const endpoints_set_t& lhs, const endpoints_set_t& rhs);
	bool all_endpoints_dead(const endpoints_set_t& endpoints);
	
	void reset_routing_table(routing_table_t& routing_table);
	
	// async callbacks
	void request(ev::io& watcher, int type);
	void check_for_timedout_endpoints(ev::timer& timer, int type);
	void fetch_and_process_endpoints(ev::timer& watcher, int type);

	// used for debug only
	void print_all_fetched_endpoints();
	void print_routing_table();

private:
	typedef boost::shared_ptr<ev::io> 	shared_ev_io_t;
	typedef std::set<inetv4_endpoint_t> inetv4_endpoints_t;

	// <service, hosts>
	std::map<std::string, inetv4_endpoints_t>	m_service_hosts;
	std::vector<hosts_fetcher_ptr>				m_hosts_fetchers;

	// <service, socket>
	std::map<std::string, shared_socket_t>		m_sockets;

	std::unique_ptr<ev::timer>	m_fetcher_timer;
	std::unique_ptr<ev::timer>	m_timeout_timer;
	
	routing_table_t				m_routing_table;
	callback_t					m_callback;
	std::vector<shared_ev_io_t>	m_watchers;
	wuuid_t						m_uuid;
};

} // namespace dealer
} // namespace cocaine

#endif // _COCAINE_DEALER_OVERSEER_HPP_INCLUDED_
