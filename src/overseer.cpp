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

namespace cocaine {
namespace dealer {

overseer_t::overseer_t(const boost::shared_ptr<context_t>& ctx, bool logging_enabled) :
	dealer_object_t(ctx, logging_enabled),
	m_stopping(false)
{
	m_uuid.generate();

	socket_t sock(context(), ZMQ_SUB);
}

overseer_t::~overseer_t() {
	stop();
}

void
overseer_t::run() {
	log(PLOG_DEBUG, "overseer — started.");

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

		m_endpoints_fetchers.push_back(fetcher);
	}

	reset_routing_table(m_routing_table);

	// create main overseer loop
	boost::function<void()> f = boost::bind(&overseer_t::main_loop, this);
	m_thread = boost::thread(f);
}


void
overseer_t::stop() {
	m_stopping = true;
	
	m_fetcher_timer.stop();
	m_timeout_timer.stop();

	for (size_t i = 0; i < m_watchers.size(); ++i) {
		m_watchers[i]->stop();
	}

	m_watchers.clear();

	m_event_loop.unloop(ev::ALL);
	m_thread.join();

	log(PLOG_DEBUG, "overseer — stopped");
}

void
overseer_t::main_loop() {
	// init
	create_sockets();

	std::map<std::string, std::set<inetv4_endpoint_t> > new_endpoints;
	fetch_endpoints(new_endpoints);
	connect_sockets(new_endpoints);

	// fetch endpoints every 15 secs
	m_fetcher_timer.set<overseer_t, &overseer_t::fetch_and_process_endpoints>(this);
	m_timeout_timer.set<overseer_t, &overseer_t::check_for_timedout_endpoints>(this);

    m_fetcher_timer.start(15, 15);
    m_timeout_timer.start(0, 0.5);

	if (!m_stopping) {
		m_event_loop.loop();
	}
	
	kill_sockets();

	for (size_t i = 0; i < m_endpoints_fetchers.size(); ++i) {
		m_endpoints_fetchers[i].reset();
	}
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
	it = m_endpoints.begin();

	for (; it != m_endpoints.end(); ++it) {
		std::cout << "service: " << it->first << std::endl;

		std::set<inetv4_endpoint_t>::iterator its;
		its = it->second.begin();

		for (; its != it->second.end(); ++its) {
			std::cout << "\thost: " << its->as_string() << std::endl;
		}
	}
}

void
overseer_t::fetch_and_process_endpoints(ev::timer& watcher, int type) {
	std::map<std::string, std::set<inetv4_endpoint_t> > new_endpoints;
	bool found_missing_endpoints = fetch_endpoints(new_endpoints);

	// if (found_missing_endpoints) {
	// 	kill_sockets();
	// 	create_sockets();
	// }

	connect_sockets(new_endpoints);
}

void
overseer_t::request(ev::io& watcher, int type) {
	if (type != ev::READ) {
		return;
	}

	progress_timer t;

	std::map<std::string, std::vector<announce_t> > responces;
	read_from_sockets(responces);

	if (responces.size() == 0) {
		return;
	}

	// parse nodes responses
	std::map<std::string, cocaine_node_list_t> parsed_responses;
	parse_responces(responces, parsed_responses);

	// get update
	routing_table_t routing_table_update;
	reset_routing_table(routing_table_update);
	routing_table_from_responces(parsed_responses, routing_table_update);

	// merge update with routing table, gen create/update handle events
	update_main_routing_table(routing_table_update);

	log(PLOG_ERROR, "received and parsed in: %.6f", t.elapsed().as_double());
}

void
overseer_t::update_main_routing_table(routing_table_t& routing_table_update) {
	// for each service in update
	routing_table_t::iterator it = routing_table_update.begin();
	for (; it != routing_table_update.end(); ++it) {

		std::string						service_name = it->first;
		handle_endpoints_t& 			handle_endpoints = it->second;

		// get handle in update, see whether it exists in main table
		handle_endpoints_t::iterator 	hit = handle_endpoints.begin();
		for (; hit != handle_endpoints.end(); ++hit) {
			std::string 					handle_name = hit->first;
			endpoints_set_t					new_endpoints_set = hit->second;
			handle_endpoints_t::iterator 	main_table_hit;

			bool handle_exists = handle_exists_for_service(m_routing_table,
														   service_name,
														   handle_name,
														   main_table_hit);

			bool was_dead = true;

			if (handle_exists) {
				was_dead = all_endpoints_dead(main_table_hit->second);
			}

			if (!handle_exists || was_dead) {
				routing_table_t::iterator sit;
				if (service_from_table(m_routing_table, service_name, sit)) {

					new_endpoints_set.insert(sit->second[handle_name].begin(),
											 sit->second[handle_name].end());

					sit->second[handle_name].clear();
					sit->second[handle_name].insert(new_endpoints_set.begin(), new_endpoints_set.end());

					if (m_callback) {
						m_callback(CREATE_HANDLE, service_name, handle_name, new_endpoints_set);
					}
				}
			}
			else {
				new_endpoints_set.insert(main_table_hit->second.begin(), main_table_hit->second.end());

				bool sets_equal = endpoints_set_equal(new_endpoints_set, main_table_hit->second);

				main_table_hit->second.clear();
				main_table_hit->second.insert(new_endpoints_set.begin(), new_endpoints_set.end());

				if (m_callback && !sets_equal) {
					m_callback(UPDATE_HANDLE, service_name, handle_name, main_table_hit->second);
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
overseer_t::check_for_timedout_endpoints(ev::timer& timer, int type) {
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

			if (some_endpoints_timed_out && all_endpoints_dead(updated_endpoints_set) && m_callback) {
				endpoints_set.clear();
				endpoints_set.insert(updated_endpoints_set.begin(), updated_endpoints_set.end());

				endpoints_set_t empty_set;
				m_callback(DESTROY_HANDLE, service_name, handle_name, empty_set);
			}
			else if (some_endpoints_timed_out && m_callback) {
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
		log(PLOG_ERROR,
			"overseer is terribly broken! service %s is missing in routing table",
			service_name.c_str());
	}
}

bool
overseer_t::endpoints_set_equal(const endpoints_set_t& lhs, const endpoints_set_t& rhs) {
	if (lhs != rhs) {
		return false;
	}

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
overseer_t::routing_table_from_responces(const std::map<std::string, cocaine_node_list_t>& parsed_responses,
										 routing_table_t& routing_table)
{
	const std::map<std::string, service_info_t>& services_list = config()->services_list();
	std::map<std::string, cocaine_node_list_t>::const_iterator it = parsed_responses.begin();

	// for each <service / nodes list>
	for (; it != parsed_responses.end(); ++it) {
		std::string 		service_name;
		std::string			app_name;
		handle_endpoints_t	handle_endpoints;

		{
			// get app name from service info
			service_name = it->first;
			std::map<std::string, service_info_t>::const_iterator its = services_list.find(service_name);

			if (its == services_list.end()) {
				continue;
			}

			app_name = its->second.app;
		}

		// process service nodes
		const cocaine_node_list_t& service_node_list = it->second;
		for (size_t i = 0; i < service_node_list.size(); ++i) {
			// find app in node
			cocaine_node_app_info_t app;
			if (!service_node_list[i].app_by_name(app_name, app)) {
				continue;
			}

			// verify app tasks
			std::string app_info_at_host = "overseer — service: " + service_name + ", app: ";
			app_info_at_host += app_name + " at host: " + service_node_list[i].hostname;

			if (app.tasks.size() == 0) {
				log(PLOG_WARNING, app_info_at_host + " has no tasks!");
				continue;
			}

			int weight = 0;

			// verify app status
			switch (app.status) {
				case APP_STATUS_UNKNOWN:
					log(PLOG_WARNING, app_info_at_host + " has unknown status!");
					continue;
					break;

				case APP_STATUS_RUNNING:
					weight = 1;
					break;

				case APP_STATUS_STOPPING:
					weight = 0;
					break;

				case APP_STATUS_STOPPED:
					log(PLOG_WARNING, app_info_at_host + " is stopped!");
					continue;
					break;

				case APP_STATUS_BROKEN:
					log(PLOG_WARNING, app_info_at_host + " is broken!");
					continue;
					break;

				default:
					continue;
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

				endpoints_set_t endpoints_set;
				endpoints_set.insert(endpoint);

				if (hit == handle_endpoints.end()) {
					handle_endpoints[handle_name] = endpoints_set;
				}
				else {
					hit->second.insert(endpoint);
				}
			}
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
overseer_t::parse_responces(const std::map<std::string, std::vector<announce_t> >& responces,
							std::map<std::string, cocaine_node_list_t>& parsed_responses)
{
	std::map<std::string, std::vector<announce_t> >::const_iterator it = responces.begin();

	for (; it != responces.end(); ++it) {
		std::vector<cocaine_node_info_t> parsed_nodes_for_service;

		for (size_t i = 0; i < it->second.size(); ++i) {
			cocaine_node_info_t node_info;
			cocaine_node_info_parser_t parser(context());

			if (!parser.parse(it->second[i].info, node_info)) {
				continue;
			}

			node_info.hostname = it->second[i].hostname;
			parsed_nodes_for_service.push_back(node_info);
		}

		if (!parsed_nodes_for_service.empty()) {
			parsed_responses[it->first] = parsed_nodes_for_service;
		}
	}
}

void
overseer_t::read_from_sockets(std::map<std::string, std::vector<announce_t> >& responces) {
	std::map<std::string, shared_socket_t>::iterator it = m_sockets.begin();
	for (; it != m_sockets.end(); ++it) {
		std::string service_name = it->first;

		std::set<inetv4_endpoint_t>& service_endpoints = m_endpoints[service_name];
		shared_socket_t sock = m_sockets[it->first];

		if (!sock) {
			log_error("overseer is terribly broken! bad socket for service %s, can't read from socket",
					  service_name.c_str());
			continue;
		}

		std::vector<announce_t> socket_responces;
		
		while (true) {
			announce_t		announce;
			zmq::message_t	reply;

			if (sock->recv(&reply, ZMQ_NOBLOCK)) {
				announce.hostname = std::string(static_cast<char*>(reply.data()), reply.size());
			}
			else {
				break;
			}

			if (sock->recv(&reply, ZMQ_NOBLOCK)) {
				announce.info = std::string(static_cast<char*>(reply.data()), reply.size());
			}
			else {
				break;
			}

			if (!announce.hostname.empty() && !announce.info.empty()) {
				socket_responces.push_back(announce);
			}
		}

		if (!socket_responces.empty()) {
			responces[service_name] = socket_responces;
		}
	}
}

void
overseer_t::create_sockets() {
	//std::cout << "create_sockets\n";

	const std::map<std::string, service_info_t>& services_list = config()->services_list();
	std::map<std::string, service_info_t>::const_iterator it = services_list.begin();
	
	// create sockets
	for (; it != services_list.end(); ++it) {
		const std::string& service_name = it->second.name;

		shared_socket_t sock(new socket_t(context(), ZMQ_SUB));
		sock->set_linger(0);
		sock->set_identity("[" + service_name + "]_overseer_", true);
		sock->subscribe();

		m_sockets[service_name] = sock;
	}
}

void
overseer_t::kill_sockets() {
	//std::cout << "kill_sockets\n";
	std::map<std::string, shared_socket_t>::iterator it = m_sockets.begin();
	
	// create sockets
	for (; it != m_sockets.end(); ++it) {
		it->second.reset();
	}

	m_sockets.clear();

	for (size_t i = 0; i < m_watchers.size(); ++i) {
		m_watchers[i]->stop();
	}

	m_watchers.clear();
}

void
overseer_t::connect_sockets(std::map<std::string, std::set<inetv4_endpoint_t> >& new_endpoints) {
	if (new_endpoints.empty()) {
		return;
	}

	//std::cout << "connect_sockets\n";

	// // kill watchers
	// for (size_t i = 0; i < m_watchers.size(); ++i) {
	// 	m_watchers[i]->stop();
	// }
	// m_watchers.clear();

	// create sockets
	std::map<std::string, shared_socket_t>::iterator it = m_sockets.begin();
	for (; it != m_sockets.end(); ++it) {
		const std::string& service_name = it->first;

		std::set<inetv4_endpoint_t>& service_endpoints = new_endpoints[service_name];
		shared_socket_t sock = m_sockets[service_name];

		if (sock) {
			std::set<inetv4_endpoint_t>::iterator endpoint_it = service_endpoints.begin();
			for (; endpoint_it != service_endpoints.end(); ++endpoint_it) {
				try {
					sock->connect(*endpoint_it);

					//create watcher
					if (sock->fd()) {
						ev_io_ptr watcher(new ev::io);
						watcher->set<overseer_t, &overseer_t::request>(this);
	    				watcher->start(sock->fd(), ev::READ);
	    				m_watchers.push_back(watcher);
	    			}
				}
				catch (const std::exception& ex) {
					log_error("overseer - could not connect socket for service %s, details: %s",
							  it->first.c_str(),
								ex.what());
				}
			}
		}
		else {
			log_error("overseer - invalid socket for service %s",
					  it->first.c_str());
		}
	}
}

bool
overseer_t::fetch_endpoints(std::map<std::string, std::set<inetv4_endpoint_t> >& new_endpoints) {
	bool found_missing_endpoints = false;

	// for each hosts fetcher
	for (size_t i = 0; i < m_endpoints_fetchers.size(); ++i) {
		hosts_fetcher_iface::inetv4_endpoints_t endpoints;
		service_info_t service_info;

		try {
			// get service endpoints list
			if (m_endpoints_fetchers[i]->get_hosts(endpoints, service_info)) {
				if (endpoints.empty()) {
					std::string error_msg = "overseer - fetcher returned no endpoints for service %s";
					log(PLOG_ERROR, error_msg.c_str(), service_info.name.c_str());
					continue;
				}

				// get storage for endpoints
				std::map<std::string, std::set<inetv4_endpoint_t> >::iterator it;
				it = m_endpoints.find(service_info.name);

				if (it == m_endpoints.end()) {
					std::set<inetv4_endpoint_t> new_set;
					m_endpoints[service_info.name] = new_set;
				}

				std::set<inetv4_endpoint_t>& service_endpoints = m_endpoints[service_info.name];
				std::set<inetv4_endpoint_t> new_service_endpoints;

				// update endpoints with default values
				for (size_t j = 0; j < endpoints.size(); ++j) {
					if (endpoints[j].port == 0) {
						endpoints[j].port = defaults_t::control_port;
					}

					if (endpoints[j].transport == TRANSPORT_UNDEFINED) {
						endpoints[j].transport = TRANSPORT_TCP;
					}

					new_service_endpoints.insert(endpoints[j]);
				}

				// check for missing endpoints
				// std::set<inetv4_endpoint_t>::iterator ite = service_endpoints.begin();
				// for (; ite != service_endpoints.end(); ++ite) {
				// 	std::set<inetv4_endpoint_t>::iterator nit = new_service_endpoints.find(*ite);
				// 	if (nit == new_service_endpoints.end()) {
				// 		found_missing_endpoints = true;
				// 		break;
				// 	}
				// }

				std::set<inetv4_endpoint_t> new_enpoints_set;

				// check for new endpoints
				std::set<inetv4_endpoint_t>::iterator ite = new_service_endpoints.begin();
				for (; ite != new_service_endpoints.end(); ++ite) {
					std::set<inetv4_endpoint_t>::iterator nit = service_endpoints.find(*ite);
					if (nit == service_endpoints.end()) {
						new_enpoints_set.insert(*ite);
					}
				}

				if (!new_enpoints_set.empty()) {
					new_endpoints[service_info.name] = new_enpoints_set;
				}
				
				service_endpoints.clear();
				service_endpoints.insert(new_service_endpoints.begin(), new_service_endpoints.end());
			}
		}
		catch (const std::exception& ex) {
			std::string error_msg = "overseer - failed fo retrieve hosts list, details: %s";
			log(PLOG_ERROR, error_msg.c_str(), ex.what());
		}
		catch (...) {
			std::string error_msg = "overseer - failed fo retrieve hosts list, no further details available.";
			log(PLOG_ERROR, error_msg.c_str());
		}
	}

	return found_missing_endpoints;
}

} // namespace dealer
} // namespace cocaine
