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

#include "cocaine/dealer/core/service.hpp"

namespace cocaine {
namespace dealer {

const double service_t::message_harvest_interval = 1.0;
const double service_t::responces_harvest_interval = 1.0;

service_t::service_t(const service_info_t& info,
					 const boost::shared_ptr<context_t>& ctx,
					 bool logging_enabled) :
	dealer_object_t(ctx, logging_enabled),
	m_info(info)
{
	ev::dynamic_loop& loop = context()->event_loop();

	m_message_harvester.reset(new ev::timer(loop));
	m_message_harvester->set<service_t, &service_t::harvest_messages>(this);
	m_message_harvester->start(message_harvest_interval, message_harvest_interval);

	m_responces_harvester.reset(new ev::timer(loop));
	m_responces_harvester->set<service_t, &service_t::harvest_responces>(this);
	m_responces_harvester->start(responces_harvest_interval, responces_harvest_interval);
}

service_t::~service_t() {
	// 2DO send all messages ERROR - dealer shut down
	m_message_harvester->stop();
	m_message_harvester.reset();

	// 2DO send all responces ERROR - dealer shut down
	m_responces_harvester->stop();
	m_responces_harvester.reset();

	// kill handles
	auto it = m_handles.begin();
	for (;it != m_handles.end(); ++it) {
		it->second.reset();
	}

	// detach processed responces
	{
		boost::mutex::scoped_lock lock(m_responces_mutex);
		auto it = m_responses.begin();

		while (it != m_responses.end()) {
			if (it->second.unique()) {
				m_responses.erase(it++);
			}
			else {
				++it;
			}
		}
	}

	log_info("FINISHED SERVICE [%s]", m_info.name.c_str());
}

void
service_t::harvest_responces(ev::timer& timer, int type) {
	boost::mutex::scoped_lock lock(m_responces_mutex);

	// check for unique responses and remove them
	auto it = m_responses.begin();

	while (it != m_responses.end()) {
		if (it->second.unique()) {
			m_responses.erase(it++);
		}
		else {
			++it;
		}
	}
}

service_info_t
service_t::info() const {
	return m_info;
}

boost::shared_ptr<response_t>
service_t::send_message(const shared_message_t& message) {

	boost::shared_ptr<response_t> resp;
	resp.reset(new response_t(message->uuid(), message->path()));

	{
		boost::mutex::scoped_lock lock(m_responces_mutex);
		m_responses[message->uuid().as_string()] = resp;
	}


	boost::mutex::scoped_lock lock(m_handles_mutex);
	bool enqued = enque_to_handle(message);

	if (!enqued) {
		enque_to_unhandled(message);
	}

	return resp;
}

void
service_t::enqueue_responce(boost::shared_ptr<response_chunk_t>& response) {
	assert(response);

	boost::shared_ptr<response_t> response_object;

	{
		boost::mutex::scoped_lock lock(m_responces_mutex);

		// find response object for received chunk
		auto it = m_responses.find(response->uuid.as_string());

		// no response object -> discard chunk
		if (it == m_responses.end()) {
			return;
		}

		// response object has only one ref -> discard chunk
		if (it->second.unique()) {
			return;
		}

		response_object = it->second;
	}

	assert(response_object);
	response_object->add_chunk(response);
}

bool
service_t::enque_to_handle(const shared_message_t& message) {
	//boost::mutex::scoped_lock lock(m_handles_mutex);

	auto it = m_handles.find(message->path().handle_name);
	if (it == m_handles.end()) {
		return false;
	}

	shared_handle_t handle = it->second;
	assert(handle);
	handle->enqueue_message(message);

	if (log_enabled(PLOG_DEBUG)) {
		const static std::string message_str = "enqued msg (%d bytes) with uuid: %s to existing %s (%s)";
		std::string enqued_timestamp_str = message->enqued_timestamp().as_string();

		log_debug(message_str,
				  message->size(),
				  message->uuid().as_human_readable_string().c_str(),
				  message->path().as_string().c_str(),
				  enqued_timestamp_str.c_str());
	}

	return true;
}

void
service_t::enque_to_unhandled(const shared_message_t& message) {
	boost::mutex::scoped_lock lock(m_unhandled_mutex);

	const std::string& handle_name = message->path().handle_name;
	auto it = m_unhandled_messages.find(handle_name);
	
	// check for existing message queue for handle
	if (it == m_unhandled_messages.end()) {
		shared_messages_deque_t queue(new messages_deque_t);
		queue->push_back(message);
		m_unhandled_messages[handle_name] = queue;
	}
	else {
		shared_messages_deque_t queue = it->second;
		assert(queue);
		queue->push_back(message);
	}

	if (log_enabled(PLOG_DEBUG)) {
		const static std::string message_str = "enqued msg (%d bytes) with uuid: %s to unhandled %s (%s)";
		std::string enqued_timestamp_str = message->enqued_timestamp().as_string();

		log_debug(message_str,
				  message->size(),
				  message->uuid().as_human_readable_string().c_str(),
				  message->path().as_string().c_str(),
				  enqued_timestamp_str.c_str());
	}
}

boost::shared_ptr<std::deque<boost::shared_ptr<message_iface> > >
service_t::get_and_remove_unhandled_queue(const std::string& handle_name) {
	boost::mutex::scoped_lock lock(m_unhandled_mutex);

	shared_messages_deque_t queue(new messages_deque_t);

	auto it = m_unhandled_messages.find(handle_name);
	if (it == m_unhandled_messages.end()) {
		return queue;
	}

	queue = it->second;
	m_unhandled_messages.erase(it);

	return queue;
}

void
service_t::append_to_unhandled(const std::string& handle_name,
							  const shared_messages_deque_t& handle_queue)
{
	assert(handle_queue);

	if (handle_queue->empty()) {
		return;
	}

	// in case there are messages, store them		
	log_debug("moving message queue from handle [%s.%s] to service, queue size: %d",
			  m_info.name.c_str(),
			  handle_name.c_str(),
			  handle_queue->size());

	boost::mutex::scoped_lock lock(m_unhandled_mutex);

	// find corresponding unhandled message queue
	auto it = m_unhandled_messages.find(handle_name);

	shared_messages_deque_t queue;
	if (it == m_unhandled_messages.end()) {
		queue.reset(new messages_deque_t);
		m_unhandled_messages[handle_name] = queue;
	}
	else {
		queue = it->second;
	}

	assert(queue);
	queue->insert(queue->end(), handle_queue->begin(), handle_queue->end());

	// clear metadata
	for (auto it = queue->begin(); it != queue->end(); ++it) {
		(*it)->mark_as_sent(false);
		(*it)->set_ack_received(false);
	}

	log_debug("moving message queue done.");
}

void
service_t::get_outstanding_handles(const handles_endpoints_t& handles_endpoints,
								   handles_info_list_t& outstanding_handles)
{
	boost::mutex::scoped_lock lock(m_handles_mutex);

	for (auto it = m_handles.begin(); it != m_handles.end(); ++it) {
		const std::string& handle_name = it->first;

		auto hit = handles_endpoints.find(handle_name);
		if (hit == handles_endpoints.end()) {
			outstanding_handles.push_back(it->second->info());
		}
	}
}

void
service_t::get_new_handles(const handles_endpoints_t& handles_endpoints,
						   handles_info_list_t& new_handles)
{
	boost::mutex::scoped_lock lock(m_handles_mutex);

	auto it = handles_endpoints.begin();
	for (; it != handles_endpoints.end(); ++it) {
		const std::string& handle_name = it->first;

		auto hit = m_handles.find(handle_name);
		if (hit == m_handles.end()) {
			handle_info_t hinfo(handle_name, m_info.app, m_info.name);
			new_handles.push_back(hinfo);
		}
	}
}

void
service_t::create_handle(const handle_info_t& handle_info, const std::set<cocaine_endpoint_t>& endpoints) {
	boost::mutex::scoped_lock lock(m_handles_mutex);

	// create new handle
	shared_handle_t handle(new dealer::handle_t(handle_info, endpoints, context()));
	handle->set_responce_callback(boost::bind(&service_t::enqueue_responce, this, _1));

	// retrieve unhandled queue
	shared_messages_deque_t queue = get_and_remove_unhandled_queue(handle_info.name);

	if (!queue->empty()) {
		handle->assign_message_queue(queue);

		log_debug("assign unhandled message queue to handle %s, queue size: %d",
				  handle_info.as_string().c_str(),
				  queue->size());
	}
	else {
		log_debug("no unhandled message queue for handle %s",
				  handle_info.as_string().c_str());
	}

	// append new handle
	m_handles[handle_info.name] = handle;
}

void
service_t::update_handle(const handle_info_t& handle_info, const std::set<cocaine_endpoint_t>& endpoints) {
	boost::mutex::scoped_lock lock(m_handles_mutex);

	auto it = m_handles.find(handle_info.name);
	if (it == m_handles.end()) {
		log_error("no existing handle %s to update",
				  handle_info.as_string().c_str());

		return;
	}
	
	shared_handle_t handle = it->second;
	assert(handle);
	handle->update_endpoints(endpoints);
}

void
service_t::destroy_handle(const handle_info_t& info) {
	boost::mutex::scoped_lock lock(m_handles_mutex);

	auto it = m_handles.find(info.name);

	if (it == m_handles.end()) {
		log_error("unable to DESTROY HANDLE [%s], handle object missing.", info.name.c_str());
		return;
	}

	shared_handle_t handle = it->second;
	assert(handle);

	// retrieve message cache and terminate all handle activity
	handle->kill();

	boost::shared_ptr<message_cache_t> mcache = handle->messages_cache();
	
	//mcache->log_stats();

	mcache->make_all_messages_new();

	mcache->log_stats();

	const shared_messages_deque_t& handle_queue = mcache->new_messages();

	append_to_unhandled(info.name, handle_queue);

	m_handles.erase(it);
	lock.unlock();
}

void
service_t::harvest_messages(ev::timer& timer, int type) {
	boost::mutex::scoped_lock lock(m_unhandled_mutex);

	auto it = m_unhandled_messages.begin();

	for (; it != m_unhandled_messages.end(); ++it) {
		shared_messages_deque_t queue = it->second;

		// create tmp queue
		shared_messages_deque_t not_expired_queue(new messages_deque_t);
		shared_messages_deque_t expired_queue(new messages_deque_t);
		bool found_expired = false;

		auto qit = queue->begin();
		for (;qit != queue->end(); ++qit) {
			if ((*qit)->is_expired() && (*qit)->is_deadlined()) {
				expired_queue->push_back(*qit);
				found_expired = true;
			}
			else {
				not_expired_queue->push_back(*qit);
			}
		}

		if (!found_expired) {
			continue;
		}

		it->second = not_expired_queue;

		// create error response for deadlined message
		std::string enqued_timestamp_str;
		std::string sent_timestamp_str;
		std::string curr_timestamp_str;

		auto expired_qit = expired_queue->begin();
		for (;expired_qit != expired_queue->end(); ++expired_qit) {
			boost::shared_ptr<response_chunk_t> response(new response_chunk_t);
			response->uuid = (*expired_qit)->uuid();
			response->rpc_code = SERVER_RPC_MESSAGE_ERROR;
			response->error_code = deadline_error;
			response->error_message = "unhandled message expired";
			enqueue_responce(response);

			enqued_timestamp_str = (*expired_qit)->enqued_timestamp().as_string();
			sent_timestamp_str = (*expired_qit)->sent_timestamp().as_string();
			curr_timestamp_str = time_value::get_current_time().as_string();

			if (log_enabled(PLOG_ERROR)) {
				static const std::string log_str = "deadline policy exceeded, for unhandled message %s, (enqued: %s, sent: %s, curr: %s)";

				log_error(log_str,
						  response->uuid.as_human_readable_string().c_str(),
						  enqued_timestamp_str.c_str(),
						  sent_timestamp_str.c_str(),
						  curr_timestamp_str.c_str());
			}
		}
	}
}

} // namespace dealer
} // namespace cocaine
