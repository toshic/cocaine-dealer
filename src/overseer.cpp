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

namespace cocaine {
namespace dealer {

overseer_t::overseer_t(const boost::shared_ptr<context_t>& ctx,
					   bool logging_enabled) :
	dealer_object_t(ctx, logging_enabled),
	m_stopping(false)
{
	m_uuid.generate();
}

overseer_t::~overseer_t() {
	stop();
	log(PLOG_DEBUG, "overseer — killed.");
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

	// prepare routing table
	it = services_list.begin();
	for (; it != services_list.end(); ++it) {
		handle_endpoints_t handle_endpoints;
		m_routing_table[it->second.name] = handle_endpoints;
	}

	// create main overseer loop
	boost::function<void()> f = boost::bind(&overseer_t::main_loop, this);
	m_thread = boost::thread(f);
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
overseer_t::stop() {
	log(PLOG_DEBUG, "overseer — stopping...");

	for (size_t i = 0; i < m_endpoints_fetchers.size(); ++i) {
		m_endpoints_fetchers[i].reset();
	}

	m_stopping = true;
	m_thread.join();

	log(PLOG_DEBUG, "overseer — stopped.");
}

void
overseer_t::main_loop() {
	// init
	m_last_fetch_timer.reset();

	create_sockets();
	fetch_endpoints();
	connect_sockets();

	while (!m_stopping) {

		// process endpoints connections
		if (m_last_fetch_timer.elapsed().as_double() > 15.0) {
			bool found_missing_endpoints = fetch_endpoints();

			if (found_missing_endpoints) {
				kill_sockets();
				create_sockets();
			}

			connect_sockets();
			m_last_fetch_timer.reset();
		}

		// gather announced responces
		std::vector<std::string> responded_sockets_ids = poll_sockets();
		std::map<std::string, std::vector<announce_t> > responces;

		if (!responded_sockets_ids.empty()) {
			read_from_sockets(responded_sockets_ids, responces);
		}

		// parse nodes responses
		std::map<std::string, cocaine_node_list_t> parsed_responses;
		parse_responces(responces, parsed_responses);

		update_routing_table(parsed_responses);
		check_for_timedout_endpoints();

		print_routing_table();

		/*
		std::map<std::string, cocaine_node_list_t>::iterator it = parsed_responses.begin();
		for (; it != parsed_responses.end(); ++it) {
			std::cout << "service: " << it->first << std::endl;

			for (size_t i = 0; i < it->second.size(); ++i) {
				std::cout << "\tinfo: " << it->second[i] << std::endl;
			}
		}
		*/
	}

	kill_sockets();
}

void
overseer_t::check_for_timedout_endpoints() {
	// service
	routing_table_t::iterator it = m_routing_table.begin();
	for (; it != m_routing_table.end(); ++it) {

		handle_endpoints_t& handle_endpoints = it->second;
		handle_endpoints_t::iterator hit = handle_endpoints.begin();

		// handle
		for (; hit != handle_endpoints.end(); ++hit) {

			// endpoints set
			endpoints_set_t updated_endpoints_set;
			endpoints_set_t& endpoints_set = hit->second;
			endpoints_set_t::iterator eit = endpoints_set.begin();
			
			for (; eit != endpoints_set.end(); ++eit) {
				cocaine_endpoint_t endpoint = *eit;
				double elapsed = endpoint.announce_timer.elapsed().as_double();

				if (elapsed > config()->endpoint_timeout()) {
					endpoint.weight = 0;
				}

				updated_endpoints_set.insert(endpoint);
			}

			endpoints_set.clear();
			endpoints_set.insert(updated_endpoints_set.begin(), updated_endpoints_set.end());
		}
	}
}

void
overseer_t::update_routing_table(const std::map<std::string, cocaine_node_list_t>& parsed_responses) {
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
				routing_table_t::iterator rit = m_routing_table.find(service_name);

				if (rit == m_routing_table.end()) {
					log(PLOG_ERROR,
						"overseer is terribly broken! service %s is missing in routing table",
						service_name.c_str());
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
					std::pair<endpoints_set_t::iterator, bool> res;
					res = hit->second.insert(endpoint);
					hit->second.erase(res.first);
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
				std::string error_msg = "overseer - could not parse metainfo for service: %s from node: %s";
				log(PLOG_WARNING, error_msg, it->first.c_str(), node_info.hostname.c_str());

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
overseer_t::read_from_sockets(const std::vector<std::string>& responded_sockets_ids,
							  std::map<std::string, std::vector<announce_t> >& responces)
{
	for (size_t i = 0; i < responded_sockets_ids.size(); ++i) {
		socket_ptr sock_ptr = m_sockets[responded_sockets_ids[i]];

		zmq::message_t reply;
		std::string enpoint_info_string;

		std::vector<announce_t> socket_responces;

		announce_t announce;

		if (sock_ptr->recv(&reply, ZMQ_NOBLOCK)) {
			announce.hostname = std::string(static_cast<char*>(reply.data()), reply.size());
		}
		else {
			return;
		}

		if (sock_ptr->recv(&reply, ZMQ_NOBLOCK)) {
			announce.info = std::string(static_cast<char*>(reply.data()), reply.size());
		}
		else {
			return;
		}

		socket_responces.push_back(announce);

		if (!socket_responces.empty()) {
			responces[responded_sockets_ids[i]] = socket_responces;
		}
	}
}

std::vector<std::string>
overseer_t::poll_sockets() {
	zmq_pollitem_t* poll_items = NULL;
	poll_items = new zmq_pollitem_t[m_sockets.size()];

	std::vector<std::string> responded_sockets_ids;

	if (!poll_items) {
		return responded_sockets_ids;
	}

	size_t counter = 0;
	std::map<std::string, socket_ptr>::iterator it = m_sockets.begin();
	for (; it != m_sockets.end(); ++it) {
		socket_ptr sock = it->second;
		poll_items[counter].socket = *sock;
		poll_items[counter].fd = 0;
		poll_items[counter].events = ZMQ_POLLIN;
		poll_items[counter].revents = 0;
		++counter;
	}

	int res = zmq_poll(poll_items, m_sockets.size(), socket_poll_timeout);
	if (res == 0) {
		log(PLOG_DEBUG, "overseer - did not get response timely from endpoints");
		delete[] poll_items;
		return responded_sockets_ids;
	}
	else if (res < 0) {
		log(PLOG_DEBUG, "overseer - error code: %d while polling sockets", errno);
		delete[] poll_items;
		return responded_sockets_ids;
	}

	counter = 0;
	it = m_sockets.begin();
	for (; it != m_sockets.end(); ++it) {
		if ((ZMQ_POLLIN & poll_items[counter].revents) != ZMQ_POLLIN) {
			continue;
		}

		responded_sockets_ids.push_back(it->first);
		++counter;
	}

	delete[] poll_items;
	return responded_sockets_ids;
}

void
overseer_t::create_sockets() {
	const std::map<std::string, service_info_t>& services_list = config()->services_list();
	std::map<std::string, service_info_t>::const_iterator it = services_list.begin();
	
	// create sockets
	for (; it != services_list.end(); ++it) {
		zmq::socket_t* sock = new zmq::socket_t(*(context()->zmq_context()), ZMQ_SUB);
		
		int timeout = 0;
		sock->setsockopt(ZMQ_LINGER, &timeout, sizeof(timeout));

		wuuid_t sock_uuid;
		sock_uuid.generate();
		std::string ident = "[" + it->second.name + "]_overseer_";
		ident += sock_uuid.as_human_readable_string();
		std::cout << "ident: " << ident << std::endl;
		sock->setsockopt(ZMQ_IDENTITY, ident.c_str(), ident.length());

		std::string subscription_filter = "";
		sock->setsockopt(ZMQ_SUBSCRIBE, subscription_filter.c_str(), subscription_filter.length());
		socket_ptr sock_ptr(sock);
		m_sockets[it->second.name] = sock_ptr;
	}
}

void
overseer_t::kill_sockets() {
	std::map<std::string, socket_ptr>::iterator it = m_sockets.begin();
	
	// create sockets
	for (; it != m_sockets.end(); ++it) {
		it->second.reset();
	}

	m_sockets.clear();
}

void
overseer_t::connect_sockets() {
	std::map<std::string, socket_ptr>::iterator it = m_sockets.begin();

	// create sockets
	for (; it != m_sockets.end(); ++it) {
		std::set<inetv4_endpoint_t>& service_endpoints = m_endpoints[it->first];
		socket_ptr sock = m_sockets[it->first];

		if (sock) {
			std::set<inetv4_endpoint_t>::iterator sit = service_endpoints.begin();
			for (; sit != service_endpoints.end(); ++sit) {
				try {
					sock->connect(sit->as_connection_string().c_str());
				}
				catch (const std::exception& ex) {
					log(PLOG_ERROR,
						"overseer - could not connect socket for service %s, details: %s",
						it->first.c_str(),
						ex.what());
				}
			}
		}
		else {
			log(PLOG_ERROR,
				"overseer - invalid socket for service %s",
				it->first.c_str());
		}
	}
}

bool
overseer_t::fetch_endpoints() {
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
				std::set<inetv4_endpoint_t>::iterator ite = service_endpoints.begin();
				for (; ite != service_endpoints.end(); ++ite) {
					std::set<inetv4_endpoint_t>::iterator nit = new_service_endpoints.find(*ite);
					if (nit == new_service_endpoints.end()) {
						found_missing_endpoints = true;
						break;
					}
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
