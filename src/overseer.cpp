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

#include <memory>

#include <boost/tuple/tuple.hpp>

#include "cocaine/dealer/heartbeats/overseer.hpp"

#include "cocaine/dealer/heartbeats/file_hosts_fetcher.hpp"
#include "cocaine/dealer/heartbeats/http_hosts_fetcher.hpp"
#include "cocaine/dealer/core/inetv4_endpoint.hpp"
#include "cocaine/dealer/cocaine_node_info/cocaine_node_info_parser.hpp"
#include "cocaine/dealer/utils/progress_timer.hpp"

#include <cocaine/traits.hpp>
#include <cocaine/dealer/utils/json.hpp>

namespace cocaine {
namespace dealer {

overseer_t::overseer_t(const boost::shared_ptr<context_t>& ctx, bool logging_enabled) :
	dealer_object_t(ctx, logging_enabled)
{
	m_uuid.generate();
}

overseer_t::~overseer_t() {
}

void
overseer_t::run() {
	log_debug("overseer — started.");

	const std::map<std::string, service_info_t>& services_list = config()->services_list();
	std::map<std::string, service_info_t>::const_iterator it = services_list.begin();
	
	for (; it != services_list.end(); ++it) {
		// create host fetcher for service
		hosts_fetcher_ptr fetcher;

		switch (it->second.discovery_type) {
			case AT_FILE:
				fetcher.reset(new file_hosts_fetcher_t(it->second));
				break;

			case AT_HTTP:
				fetcher.reset(new http_hosts_fetcher_t(it->second));
				break;

			default: {
				std::string error_msg = "unknown autodiscovery type defined for service ";
				error_msg += "\"" + it->second.name + "\"";
				throw internal_error(error_msg);
			}
		}

		m_hosts_fetchers.push_back(fetcher);
	}

	reset_routing_table(m_routing_table);

	// init
	create_socket();

	// fetch endpoints every 15 secs
	ev::dynamic_loop& loop = context()->event_loop();

	m_fetcher_timer.reset(new ev::timer(loop));
	m_fetcher_timer->set<overseer_t, &overseer_t::fetch_and_process_hosts>(this);
	m_fetcher_timer->start(0, 15);

	m_timeout_timer.reset(new ev::timer(loop));
	m_timeout_timer->set<overseer_t, &overseer_t::check_for_timedout_hosts>(this);
	m_timeout_timer->start(0, 0.5);
}


void
overseer_t::stop() {
	if (m_fetcher_timer) {
		m_fetcher_timer->stop();
		m_fetcher_timer.reset();
	}

	if (m_timeout_timer) {
		m_timeout_timer->stop();
		m_timeout_timer.reset();
	}

	
	m_watcher->stop();
	m_watcher.reset();

	kill_socket();

	for (size_t i = 0; i < m_hosts_fetchers.size(); ++i) {
		m_hosts_fetchers[i].reset();
	}

	log_debug("overseer — stopped.");
}

void
overseer_t::set_callback(callback_t callback) {
	m_callback = callback;
}

void
overseer_t::reset_routing_table(routing_table_t& routing_table) {
	routing_table.clear();

	const std::map<std::string, service_info_t>& services_list = config()->services_list();
	std::map<std::string, service_info_t>::const_iterator it = services_list.begin();

	// prepare routing table
	for (; it != services_list.end(); ++it) {
		handle_endpoints_t handle_endpoints;
		routing_table[it->second.name] = handle_endpoints;
	}
}

void
overseer_t::print_all_fetched_endpoints() {
	std::map<std::string, std::set<inetv4_endpoint_t> >::iterator it;
	it = m_service_hosts.begin();

	for (; it != m_service_hosts.end(); ++it) {
		std::cout << "service: " << it->first << std::endl;

		std::set<inetv4_endpoint_t>::iterator its;
		its = it->second.begin();

		for (; its != it->second.end(); ++its) {
			std::cout << "\thost: " << its->as_string() << std::endl;
		}
	}
}

void
overseer_t::fetch_and_process_hosts(ev::timer& watcher, int type) {
	std::map<std::string, std::set<inetv4_endpoint_t> > new_hosts;
	std::map<std::string, std::set<inetv4_endpoint_t> > missing_hosts;
	fetch_hosts(new_hosts, missing_hosts);

	// collect all hosts for all services
	std::set<inetv4_endpoint_t> hosts;
	std::map<std::string, std::set<inetv4_endpoint_t> >::iterator it;
	it = new_hosts.begin();

	for (; it != new_hosts.end(); ++it) {
		hosts.insert(it->second.begin(), it->second.end());
	}

	connect_socket(hosts);
	//disconnect_socket(missing_hosts);
}

void
overseer_t::request(ev::io& watcher, int type) {
	if (type != ev::READ) {
		return;
	}

	progress_timer t;
	std::vector<announce_t> responces;
	read_from_socket(responces);

	if (responces.size() == 0) {
		return;
	}

	// parse nodes responses
	std::map<inetv4_host_t, cocaine_node_info_t> parsed_responses;
	parse_responces(responces, parsed_responses);

	// get update
	routing_table_t routing_table_update;
	reset_routing_table(routing_table_update);
	routing_table_from_responces(parsed_responses, routing_table_update);

	// merge update with routing table, gen create/update handle events
	update_main_routing_table(routing_table_update);
}

void
overseer_t::update_main_routing_table(routing_table_t& routing_table_update) {
	// for each service in update
	routing_table_t::iterator updated_service_it = routing_table_update.begin();
	for (; updated_service_it != routing_table_update.end(); ++updated_service_it) {

		std::string				service_name = updated_service_it->first;
		handle_endpoints_t& 	updated_handles = updated_service_it->second;

		// get handle in update, see whether it exists in main table
		handle_endpoints_t::iterator updated_handles_it = updated_handles.begin();

		for (; updated_handles_it != updated_handles.end(); ++updated_handles_it) {
			std::string 	handle_name = updated_handles_it->first;
			endpoints_set_t	new_endpoints_set = updated_handles_it->second;

			handle_endpoints_t::iterator main_table_handle_it;
			bool handle_exists = handle_exists_for_service(m_routing_table,
														   service_name,
														   handle_name,
														   main_table_handle_it);

			bool was_dead = true;

			if (handle_exists) {
				was_dead = all_endpoints_dead(main_table_handle_it->second);
			}

			if (!handle_exists || was_dead) {
				routing_table_t::iterator service_it;
				if (service_from_table(m_routing_table, service_name, service_it)) {

					handle_endpoints_t& handles = service_it->second;

					// create new handle
					if (!handle_exists) {
						endpoints_set_t new_set;
						handles[handle_name] = new_set;
					}

					endpoints_set_t& existing_endpoints_set = handles[handle_name];

					new_endpoints_set.insert(existing_endpoints_set.begin(),
											 existing_endpoints_set.end());

					existing_endpoints_set.clear();
					existing_endpoints_set.insert(new_endpoints_set.begin(), new_endpoints_set.end());

					if (m_callback) {
						m_callback(CREATE_HANDLE, service_name, handle_name, existing_endpoints_set);
					}
				}
			}
			else {
				endpoints_set_t& existing_endpoints_set = main_table_handle_it->second;

				new_endpoints_set.insert(existing_endpoints_set.begin(),
										 existing_endpoints_set.end());

				bool sets_equal = endpoints_set_equal(new_endpoints_set, existing_endpoints_set);

				existing_endpoints_set.clear();
				existing_endpoints_set.insert(new_endpoints_set.begin(), new_endpoints_set.end());

				assert(m_callback);

				if (all_endpoints_dead(existing_endpoints_set)) {
					existing_endpoints_set.clear();
					m_callback(DESTROY_HANDLE, service_name, handle_name, existing_endpoints_set);
				}
				else {
					if (!sets_equal) {
						m_callback(UPDATE_HANDLE, service_name, handle_name, main_table_handle_it->second);
					}
				}
			}
		}
	}
}

bool
overseer_t::all_endpoints_dead(const endpoints_set_t& endpoints) {
	endpoints_set_t::const_iterator it = endpoints.begin();

	for (; it != endpoints.end(); ++it) {
		if (it->weight > 0) {
			return false;
		}
	}

	return true;
}

void
overseer_t::check_for_timedout_hosts(ev::timer& timer, int type) {
	// service
	routing_table_t::iterator it = m_routing_table.begin();
	for (; it != m_routing_table.end(); ++it) {

		std::string						service_name = it->first;
		handle_endpoints_t&				handle_endpoints = it->second;
		handle_endpoints_t::iterator	hit = handle_endpoints.begin();

		// handle
		for (; hit != handle_endpoints.end(); ++hit) {

			std::string 				handle_name = hit->first;
			endpoints_set_t&			endpoints_set = hit->second;
			endpoints_set_t				updated_endpoints_set;
			endpoints_set_t::iterator	eit = endpoints_set.begin();
			
			bool some_endpoints_timed_out = false;
			
			for (; eit != endpoints_set.end(); ++eit) {
				cocaine_endpoint_t	endpoint = *eit;
				double				elapsed = endpoint.announce_timer.elapsed().as_double();

				if (endpoint.weight > 0 &&
					elapsed > config()->endpoint_timeout())
				{
					some_endpoints_timed_out = true;
					endpoint.weight = 0;
				}

				updated_endpoints_set.insert(endpoint);
			}

			assert(m_callback);
			
			if (some_endpoints_timed_out && all_endpoints_dead(updated_endpoints_set)) {
				endpoints_set.clear();
				m_callback(DESTROY_HANDLE, service_name, handle_name, endpoints_set);
				continue;
			}

			if (some_endpoints_timed_out) {
				bool sets_equal = endpoints_set_equal(endpoints_set, updated_endpoints_set);
				endpoints_set.clear();
				endpoints_set.insert(updated_endpoints_set.begin(), updated_endpoints_set.end());

				if (!sets_equal) {
					m_callback(UPDATE_HANDLE, service_name, handle_name, endpoints_set);
				}
			}
		}
	}
}

bool
overseer_t::handle_exists_for_service(routing_table_t& routing_table,
									  const std::string& service_name,
									  const std::string& handle_name,
									  handle_endpoints_t::iterator& it)
{
	routing_table_t::iterator sit;
	if (service_from_table(routing_table, service_name, sit)) {
		handle_endpoints_t& handle_endpoints = sit->second;

		handle_endpoints_t::iterator eit = handle_endpoints.find(handle_name);
		if (eit != handle_endpoints.end()) {
			it = eit;
			return true;
		}
	}

	return false;
}

bool
overseer_t::service_from_table(routing_table_t& routing_table,
							   const std::string& service_name,
							   routing_table_t::iterator& it)
{
	routing_table_t::iterator fit = routing_table.find(service_name);
	if (fit != routing_table.end()) {
		it = fit;
		return true;
	}
	else {
		log_error("overseer is terribly broken! service %s is missing in routing table",
				  service_name.c_str());
	}
}

bool
overseer_t::endpoints_set_equal(const endpoints_set_t& lhs, const endpoints_set_t& rhs) {
	endpoints_set_t::iterator it = lhs.begin();
	for (; it != lhs.end(); ++it) {
		endpoints_set_t::iterator it2 = rhs.find(*it);

		if (it2 == rhs.end() || it->weight != it2->weight) {
			return false;
		}
	}

	return true;
}

void
overseer_t::routing_table_from_responces(const std::map<inetv4_host_t, cocaine_node_info_t>& parsed_responses,
										 routing_table_t& routing_table)
{
	const std::map<std::string, service_info_t>& services_list = config()->services_list();
	std::map<std::string, service_info_t>::const_iterator it = services_list.begin();

	for (; it != services_list.end(); ++it) {
		const std::string& service_name = it->first;
		const service_info_t& service_info = it->second;

		std::set<inetv4_endpoint_t> endpoints = get_cached_hosts_for_service(service_name);
		std::set<inetv4_endpoint_t>::iterator ite = endpoints.begin();

		for (; ite != endpoints.end(); ++ite) {
			std::map<inetv4_host_t, cocaine_node_info_t>::const_iterator host_response_it;
			host_response_it = parsed_responses.find(ite->host);

			if (host_response_it != parsed_responses.end()) {
				const cocaine_node_info_t& cocaine_host_info = host_response_it->second;
				update_routing_table_from_host_info(service_name, service_info, cocaine_host_info, routing_table);
			}
		}
	}
}

void
overseer_t::update_routing_table_from_host_info(const std::string& service_name,
												const service_info_t& service_info,
												const cocaine_node_info_t& cocaine_host_info,
												routing_table_t& routing_table)
{
	const std::string& app_name = service_info.app;
	cocaine_node_app_info_t app;
	if (!cocaine_host_info.app_by_name(app_name, app)) {
		return;
	}

	// verify app tasks
	std::string app_info_at_host = "overseer — service: " + service_name + ", app: ";
	app_info_at_host += app_name + " at host: " + cocaine_host_info.host.as_string();

	if (app.tasks.size() == 0) {
		log_warning(app_info_at_host + " has no tasks!");
		return;
	}

	int weight = 0;

	// verify app status
	switch (app.status) {
		case APP_STATUS_UNKNOWN:
			log_error(app_info_at_host + " has unknown status!");
			return;

		case APP_STATUS_RUNNING:
			weight = 1;
			break;

		case APP_STATUS_STOPPING:
			weight = 0;
			break;

		case APP_STATUS_STOPPED:
			log_warning(app_info_at_host + " is stopped!");
			return;

		case APP_STATUS_BROKEN:
			log_warning(app_info_at_host + " is broken!");
			return;

		default:
			log_error(app_info_at_host + " has undefined status!");
			return;
	}

	// insert endpoints into routing table
	cocaine_node_app_info_t::application_tasks::const_iterator task_it;
	task_it = app.tasks.begin();

	for (; task_it != app.tasks.end(); ++task_it) {
		std::string handle_name = task_it->second.name;

		// create endpoint
		cocaine_endpoint_t endpoint(task_it->second.endpoint,
									task_it->second.route,
									weight);

		// find specific service->handle routing table:
		// first, find service
		routing_table_t::iterator rit;
		if (!service_from_table(routing_table, service_name, rit)) {
			continue;
		}

		// secondly find handle
		handle_endpoints_t& handle_endpoints = rit->second;
		handle_endpoints_t::iterator hit = handle_endpoints.find(handle_name);

		if (hit == handle_endpoints.end()) {
			endpoints_set_t endpoints_set;
			endpoints_set.insert(endpoint);
			handle_endpoints[handle_name] = endpoints_set;
		}
		else {
			hit->second.insert(endpoint);
		}
	}
}

void
overseer_t::print_routing_table() {
	routing_table_t::iterator it = m_routing_table.begin();
	for (; it != m_routing_table.end(); ++it) {
		std::cout << "service: " << it->first << std::endl;

		handle_endpoints_t& handle_endpoints = it->second;
		handle_endpoints_t::iterator hit = handle_endpoints.begin();

		for (; hit != handle_endpoints.end(); ++hit) {
			std::cout << "\thandle: " << hit->first << std::endl;

			endpoints_set_t& endpoints_set = hit->second;
			endpoints_set_t::iterator eit = endpoints_set.begin();

			for (; eit != endpoints_set.end(); ++eit) {
				std::cout << "\t\t" << eit->as_string() << std::endl;
			}
		}
	}
}

void
overseer_t::parse_responces(const std::vector<announce_t>& responces,
							std::map<inetv4_host_t, cocaine_node_info_t>& parsed_responses)
{
	for (size_t i = 0; i < responces.size(); ++i) {
		cocaine_node_info_t node_info;
		cocaine_node_info_parser_t parser(context());

		if (!parser.parse(responces[i].info, node_info)) {
			continue;
		}

		node_info.host = responces[i].host;
		parsed_responses[responces[i].host] = node_info;
	}
}

void
overseer_t::read_from_socket(std::vector<announce_t>& responces) {
	if (!m_socket) {
		log_error("overseer is terribly broken! can't read from bad socket");
		return;
	}
		
	while (true) {
		announce_t		announce;
		std::string		host_str;
		zmq::message_t	reply;
		bool data_ok;

		if (m_socket->recv(&reply, ZMQ_NOBLOCK)) {
			host_str = std::string(static_cast<char*>(reply.data()), reply.size());
		}
		else {
			break;
		}

		if (m_socket->recv(&reply, ZMQ_NOBLOCK)) {
					msgpack::object obj;
					msgpack::unpacked up;

					if (0 == reply.size()) {
						data_ok = false;
					}

					msgpack::unpack(&up, (char*)reply.data(), reply.size());
					obj = up.get();

					Json::Value val;
					cocaine::io::type_traits<Json::Value>::unpack(obj, val);
					announce.info = val;
			//announce.info = std::string(static_cast<char*>(reply.data()), reply.size());
		}
		else {
			break;
		}

		if (!host_str.empty() && !announce.info.empty()) {
			inetv4_endpoint_t endpoint(host_str);
			announce.host = endpoint.host;
			responces.push_back(announce);
		}

		m_socket->drop();
	}
}

void
overseer_t::create_socket() {
	m_socket.reset(new socket_t(context(), ZMQ_SUB));
	m_socket->set_linger(0);

	#if ZMQ_VERSION_MAJOR < 3
		int64_t hwm = 5;
		m_socket->set_sockopt(ZMQ_HWM, &hwm, sizeof(hwm));
	#else
		int hwm = 5;
		m_socket->set_sockopt(ZMQ_SNDHWM, &hwm, sizeof(hwm));
		m_socket->set_sockopt(ZMQ_RCVHWM, &hwm, sizeof(hwm));
	#endif

	m_socket->set_identity("overseer_", true);
	m_socket->subscribe();

	if (m_socket->fd()) {
		ev::dynamic_loop& loop = context()->event_loop();
		m_watcher.reset(new ev::io(loop));
		m_watcher->set<overseer_t, &overseer_t::request>(this);
		m_watcher->start(m_socket->fd(), ev::READ);
	}
	else {
		log_error("overseer - could create socket");
	}
}

void
overseer_t::kill_socket() {
	m_socket.reset();

	if (m_watcher) {
		m_watcher->stop();
		m_watcher.reset();
	}
}

void
overseer_t::connect_socket(std::set<inetv4_endpoint_t>& hosts) {
	if (hosts.empty()) {
		return;
	}

	std::set<inetv4_endpoint_t>::iterator it = hosts.begin();

	for (; it != hosts.end(); ++it) {
		if (m_socket) {
			try {
				//std::cout << "connect host: " << host_it->as_string() << std::endl;
				m_socket->connect(*it);
			}
			catch (const std::exception& ex) {
				log_error("overseer - could not connect socket to host %s, details: %s",
						  it->as_string().c_str(),
						  ex.what());
			}
		}
		else {
			log_error("overseer - invalid socket");
		}
	}
}

void
overseer_t::disconnect_socket(std::set<inetv4_endpoint_t>& hosts) {
	if (hosts.empty()) {
		return;
	}

	std::set<inetv4_endpoint_t>::iterator it = hosts.begin();

	for (; it != hosts.end(); ++it) {
		if (m_socket) {
			try {
				//std::cout << "disconnect host: " << host_it->as_string() << std::endl;
				m_socket->connect(*it);
			}
			catch (const std::exception& ex) {
				log_error("overseer - could not disconnect socket from host %s, details: %s",
						  it->as_string().c_str(),
						  ex.what());
			}
		}
		else {
			log_error("overseer - invalid socket");
		}
	}
}

std::set<inetv4_endpoint_t>&
overseer_t::get_cached_hosts_for_service(const std::string& service_name) {
	std::map<std::string, std::set<inetv4_endpoint_t> >::iterator it;
	it = m_service_hosts.find(service_name);

	if (it == m_service_hosts.end()) {
		std::set<inetv4_endpoint_t> new_set;
		m_service_hosts[service_name] = new_set;
	}

	return m_service_hosts[service_name];
}

void
overseer_t::fetch_hosts(std::map<std::string, std::set<inetv4_endpoint_t> >& new_hosts,
						std::map<std::string, std::set<inetv4_endpoint_t> >& missing_hosts)
{
	// for each hosts fetcher
	for (size_t i = 0; i < m_hosts_fetchers.size(); ++i) {
		hosts_fetcher_iface::inetv4_endpoints_t fetched_hosts;
		service_info_t service_info;

		try {
			// get service hosts from source
			if (m_hosts_fetchers[i]->get_hosts(fetched_hosts, service_info)) {
				if (fetched_hosts.empty()) {
					std::string error_msg = "overseer - fetcher returned no hosts for service %s";
					log_error(error_msg.c_str(), service_info.name.c_str());
					continue;
				}

				std::set<inetv4_endpoint_t>& cached_hosts = get_cached_hosts_for_service(service_info.name);
				std::set<inetv4_endpoint_t>::iterator it;

				// check for new endpoints
				std::set<inetv4_endpoint_t> new_hosts_for_service;

				for (it = fetched_hosts.begin(); it != fetched_hosts.end(); ++it) {
					std::set<inetv4_endpoint_t>::iterator it2 = cached_hosts.find(*it);
					if (it2 == cached_hosts.end()) {
						new_hosts_for_service.insert(*it);
					}
				}

				if (!new_hosts_for_service.empty()) {
					new_hosts[service_info.name] = new_hosts_for_service;
				}

				// check for missing endpoints
				std::set<inetv4_endpoint_t> missing_hosts_for_service;

				for (it = cached_hosts.begin(); it != cached_hosts.end(); ++it) {
					std::set<inetv4_endpoint_t>::iterator it2 = fetched_hosts.find(*it);
					if (it2 == fetched_hosts.end()) {
						missing_hosts_for_service.insert(*it);
					}
				}

				if (!missing_hosts_for_service.empty()) {
					missing_hosts[service_info.name] = missing_hosts_for_service;
				}

				// store fetched hosts
				if (!fetched_hosts.empty()) {
					cached_hosts.clear();
					cached_hosts.insert(fetched_hosts.begin(), fetched_hosts.end());
				}
			}
		}
		catch (const std::exception& ex) {
			std::string error_msg = "overseer - failed fo retrieve hosts list, details: %s";
			log_error(error_msg.c_str(), ex.what());
		}
		catch (...) {
			std::string error_msg = "overseer - failed fo retrieve hosts list, no further details available.";
			log_error(error_msg.c_str());
		}
	}
}

} // namespace dealer
} // namespace cocaine
