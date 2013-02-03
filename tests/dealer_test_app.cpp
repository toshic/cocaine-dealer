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

#include <iostream>
#include <set>

#include <boost/program_options.hpp>
#include <boost/thread.hpp>
#include <boost/bind.hpp>
#include <boost/ptr_container/ptr_vector.hpp>
#include <boost/regex.hpp>

#include "cocaine/dealer/dealer.hpp"
#include "cocaine/dealer/utils/progress_timer.hpp"
#include "cocaine/dealer/utils/error.hpp"
#include "cocaine/dealer/utils/refresher.hpp"
#include "cocaine/dealer/utils/networking.hpp"
#include "cocaine/dealer/utils/math.hpp"

#include <eblob/eblob.hpp>

using namespace cocaine::dealer;
using namespace boost::program_options;

int sent_messages = 0;
progress_timer timer;
static bool first_responce_received = false;

void worker(dealer_t* d,
			std::vector<int>* dealer_messages_count,
			int dealer_index)
{
	message_path_t path("server_time", "add_time_func");
	std::string payload = "response chunk: ";

	while ((*dealer_messages_count)[dealer_index] >= 0) {
		try {
			boost::shared_ptr<response_t> resp;

			if (d) {
				resp = d->send_message(payload.data(), payload.size(), path);
			}

			data_container data;
			while (resp->get(&data)) {
				//std::cout << std::string(reinterpret_cast<const char*>(data.data()), 0, data.size()) << std::endl;
			}

			if (!first_responce_received) {
				timer.reset();
				first_responce_received = true;
			}
		}
		catch (const dealer_error& err) {
			std::cout << "error code: " << err.code() << ", error message: " << err.what() << std::endl;
		}
		catch (const std::exception& ex) {
			std::cout << "error message: " << ex.what() << std::endl;
		}
		catch (...) {
			std::cout << "caught exception, no error message." << std::endl;
		}

		(*dealer_messages_count)[dealer_index] = (*dealer_messages_count)[dealer_index] - 1;
		sent_messages++;
	}
}

void create_client(size_t dealers_count, size_t threads_per_dealer, size_t messages_count) {
	std::string config_path = "../tests/config.json";

	typedef boost::ptr_vector<boost::thread> thread_pool;
	typedef boost::ptr_vector<thread_pool> thread_pools_list;

	std::cout << "----------------------------------- test info -------------------------------------------\n";
	std::cout << "sending " << dealers_count * messages_count << " messages using ";
	std::cout << dealers_count << " dealers with " << threads_per_dealer << " threads each.\n";
	std::cout << "-----------------------------------------------------------------------------------------\n";

	std::vector<int> dealer_messages_count;
	boost::ptr_vector<dealer_t> dealers;

	for (size_t i = 0; i < dealers_count; ++i) {
		dealers.push_back(new dealer_t(config_path));
		dealer_messages_count.push_back(messages_count);
	}

	//std::cout << "preparing dealers...\n";
	//sleep(5);

	// create threads
	std::cout << "sending messages...\n";

	thread_pools_list pools;
	for (size_t i = 0; i < dealers_count; ++i) {
		thread_pool* pool = new thread_pool;

		for (size_t j = 0; j < threads_per_dealer; ++j) {
			boost::thread* th;
			th = new boost::thread(&worker,
								   &(dealers[i]),
								   &dealer_messages_count,
								   i);
			pool->push_back(th);
		}

		pools.push_back(pool);
	}

	for (size_t i = 0; i < dealers_count; ++i) {
		for (size_t j = 0; j < threads_per_dealer; ++j) {
			pools[i][j].join();
		}
	}

	std::cout << "sending messages done.\n";

	std::cout << "----------------------------------- test results ----------------------------------------\n";
	std::cout << "elapsed: " << timer.elapsed().as_double() << std::endl;
	std::cout << "sent: " << sent_messages << " messages.\n";
	std::cout << "approx performance: " << sent_messages / timer.elapsed().as_double() << " rps." << std::endl;
	
	std::cout << "----------------------------------- shutting dealers down -------------------------------\n";
}

int
main(int argc, char** argv) {
	/*
	dealer_t d("../tests/config.json");

	message_path_t		path("server_time", "add_time_func");
	std::string			payload = "message ";
	boost::shared_ptr<response_t> responce;

	try {
		responce = d.send_message(payload.data(), payload.size(), path);

		data_container data;
		while (responce->get(&data)) {
			std::cout << std::string(reinterpret_cast<const char*>(data.data()), 0, data.size()) << std::endl;
		}
	}
	catch (const dealer_error& err) {
		std::cout << "error code: " << err.code() << ", error message: " << err.what() << std::endl;
	}
	catch (const std::exception& ex) {
		std::cout << "error message: " << ex.what() << std::endl;
	}
	catch (...) {
		std::cout << "caught exception, no error message." << std::endl;
	}

	return EXIT_SUCCESS;
	*/

	/*
	zmq::context_t context(1);
	zmq::socket_t zmq_socket(context, ZMQ_SUB);
	
	int timeout = 0;
	zmq_socket.setsockopt(ZMQ_LINGER, &timeout, sizeof(timeout));

	std::string ident = "sjfknsdkjfnlsdf";
	zmq_socket.setsockopt(ZMQ_IDENTITY, ident.c_str(), ident.length());

	std::string subscription_filter = "";
	zmq_socket.setsockopt(ZMQ_SUBSCRIBE, subscription_filter.c_str(), subscription_filter.length());

	zmq_socket.connect("epgm://239.0.0.1:5555");
	zmq_socket.connect("tcp://elisto02f.dev.yandex.net:5554");

	// create polling structure
	zmq_pollitem_t poll_items[1];
	poll_items[0].socket = zmq_socket;
	poll_items[0].fd = 0;
	poll_items[0].events = ZMQ_POLLIN;
	poll_items[0].revents = 0;

	std::cout << "waiting for response from endpoint...\n";

	// poll for responce
	while (true) {
		int res = zmq_poll(poll_items, 1, -1);
		if (res == 0) {
			std::cout << "did not get response timely from endpoint\n";
			continue;
		}

		if (res < 0) {
			std::cout << "error code: " << errno << " while polling endpoint\n";
		}

		if ((ZMQ_POLLIN & poll_items[0].revents) != ZMQ_POLLIN) {
			std::cout << "not ZMQ_POLLIN from endpoint\n";
			continue;
		}
		
		zmq::message_t reply;
		bool received_response_ok = true;
		std::string response_string;

		while (zmq_socket.recv(&reply)) {
			response_string = std::string(static_cast<char*>(reply.data()), reply.size());
			
			if (!response_string.empty()) {
				std::cout << "received something!\n";
				std::cout << "\"" << response_string << "\"" << std::endl;
			}
		}
	}
	*/
	/*
	dealer_t			d("../tests/config.json");
	message_path_t		path("server_time", "add_time_func");
	std::string			payload = "message ";

	boost::shared_ptr<response_t> responce;

	try {
		responce = d.send_message(payload.data(), payload.size(), path);

		data_container data;
		while (responce->get(&data)) {
			std::cout << std::string(reinterpret_cast<const char*>(data.data()), 0, data.size()) << std::endl;
		}
	}
	catch (const dealer_error& err) {
		std::cout << "error code: " << err.code() << ", error message: " << err.what() << std::endl;
	}
	catch (const std::exception& ex) {
		std::cout << "error message: " << ex.what() << std::endl;
	}
	catch (...) {
		std::cout << "caught exception, no error message." << std::endl;
	}

	return EXIT_SUCCESS;
	*/
	/*
	dealer_t d("tests/config.json");

	std::vector<message_t> messages;
	d.load_unsent("rimz_app", messages);

	std::cout << "unsent count: " << messages.size() << std::endl;
	
	for (int i = 0; i < messages.size(); ++i) {
		boost::shared_ptr<response_t> resp = d.send_message(messages[i]);
		
		data_container data;
		while (resp->get(&data)) {
			std::cout << std::string(reinterpret_cast<const char*>(data.data()), 0, data.size()) << std::endl;
		}

		d.remove_unsent(messages[i]);
	}

	if (messages.size() > 0) {
		std::cout << "finished with unsent! more...\n";
	}

	message_path_t		path("rimz_app", "rimz_func");
	std::string			payload = "received chunk: ";

	message_policy_t	policy = d.policy_for_service("rimz_app");
	policy.persistent = true;

	sleep(2);

	boost::shared_ptr<response_t> resp = d.send_message(payload.data(), payload.size(), path, policy);
	data_container data;
	while (resp->get(&data)) {
		std::cout << std::string(reinterpret_cast<const char*>(data.data()), 0, data.size()) << std::endl;
	}

	return EXIT_SUCCESS;
	*/

	/*
	dealer_t			d("tests/config.json");
	message_path_t		path("server_time.*", "add_time_func");
	std::string			payload = "server time is";

	for (int j = 0; j < 100; ++j) {
		std::vector<boost::shared_ptr<response_t> > responces_list;
		responces_list = d.send_messages(payload, path);

		data_container data;

		for (size_t i = 0; i < responces_list.size(); ++i) {
			while (responces_list[i]->get(&data)) {
				std::cout << std::string(reinterpret_cast<const char*>(data.data()), 0, data.size()) << std::endl;
			}
		}
	}

	return EXIT_SUCCESS;
	*/

	try {
		options_description desc("Allowed options");
		desc.add_options()
			("help", "Produce help message")
			("dealers,d", value<int>()->default_value(1), "Number of dealers to send messages")
			("threads,t", value<int>()->default_value(1), "Threads per dealer")
			("messages,m", value<int>()->default_value(1), "Messages per dealer")
		;

		variables_map vm;
		store(parse_command_line(argc, argv, desc), vm);
		notify(vm);

		if (vm.count("help")) {
			std::cout << desc << std::endl;
			return EXIT_SUCCESS;
		}
		
		create_client(vm["dealers"].as<int>(), vm["threads"].as<int>(), vm["messages"].as<int>());
		return EXIT_SUCCESS;
	}
	catch (const std::exception& ex) {
		std::cerr << ex.what() << std::endl;
		return EXIT_FAILURE;
	}

	return EXIT_SUCCESS;
}
