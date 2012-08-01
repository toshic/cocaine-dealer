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

service_t::service_t(const service_info_t& info,
					 const boost::shared_ptr<context_t>& ctx,
					 bool logging_enabled) :
	dealer_object_t(ctx, logging_enabled),
	m_info(info),
	m_is_dead(false)
{
	// run timed out messages checker
	m_deadlined_messages_refresher.reset(new refresher(boost::bind(&service_t::check_for_deadlined_messages, this),
										 deadline_check_interval));
}

service_t::~service_t() {
	m_is_dead = true;

	// kill handles
	handles_map_t::iterator it = m_handles.begin();
	for (;it != m_handles.end(); ++it) {
		
		log(PLOG_INFO,
		"DESTROY HANDLE [%s.%s.%s]",
		m_info.name.c_str(),
		m_info.app.c_str(),
		it->second->info().name.c_str());

		it->second.reset();
	}

	log(PLOG_INFO, "FINISHED SERVICE [%s]", m_info.name.c_str());
}

bool
service_t::is_dead() {
	return m_is_dead;
}

service_info_t
service_t::info() const {
	return m_info;
}

boost::shared_ptr<response_t>
service_t::send_message(cached_message_prt_t message) {

	boost::shared_ptr<response_t> resp;
	resp.reset(new response_t(message->uuid(), message->path()));

	{
		boost::mutex::scoped_lock lock(m_responces_mutex);
		m_responses[message->uuid()] = resp;
	}


	boost::mutex::scoped_lock lock(m_handles_mutex);
	bool enqued = enque_to_handle(message);

	if (!enqued) {
		enque_to_unhandled(message);
	}

	return resp;
}

void
service_t::enqueue_responce(const cached_response_t& response_chunk) {

	{
		boost::shared_ptr<response_t> response_object;

		{
			boost::mutex::scoped_lock lock(m_responces_mutex);

			std::map<std::string, boost::shared_ptr<response_t> >::iterator it;
			it = m_responses.find(response_chunk.uuid());

			// no response object -> discard chunk
			if (it == m_responses.end()) {
				return;
			}

			// response object has only one ref -> discard chunk
			if (it->second.unique()) {
				m_responses.erase(it);
				return;
			}

			response_object = it->second;
		}

		assert(response_object);

		// create simplified response_t
		chunk_info c_info;
		c_info.uuid = response_chunk.uuid();
		c_info.path = response_chunk.path();
		c_info.code = response_chunk.code();
		c_info.error_msg = response_chunk.error_message();

		response_object->add_chunk(response_chunk.data(), c_info);
	}

	{
		boost::mutex::scoped_lock lock(m_responces_mutex);

		// check for unique responses and remove them
		std::map<std::string, boost::shared_ptr<response_t> >::iterator it = m_responses.begin();
		while (it != m_responses.end()) {
			if (it->second.unique()) {
				m_responses.erase(it++);
			}
			else {
				++it;
			}
		}
	}
}

bool
service_t::enque_to_handle(const cached_message_prt_t& message) {
	//boost::mutex::scoped_lock lock(m_handles_mutex);

	handles_map_t::iterator it = m_handles.find(message->path().handle_name);
	if (it == m_handles.end()) {
		return false;
	}

	handle_ptr_t handle = it->second;
	assert(handle);
	handle->enqueue_message(message);

	if (log_flag_enabled(PLOG_DEBUG)) {
		const static std::string message_str = "enqued msg (%d bytes) with uuid: %s to existing %s (%s)";
		std::string enqued_timestamp_str = message->enqued_timestamp().as_string();

		log(PLOG_DEBUG,
			message_str,
			message->size(),
			message->uuid().c_str(),
			message->path().as_string().c_str(),
			enqued_timestamp_str.c_str());
	}

	return true;
}

void
service_t::enque_to_unhandled(const cached_message_prt_t& message) {
	boost::mutex::scoped_lock lock(m_unhandled_mutex);

	const std::string& handle_name = message->path().handle_name;
	unhandled_messages_map_t::iterator it = m_unhandled_messages.find(handle_name);
	
	// check for existing message queue for handle
	if (it == m_unhandled_messages.end()) {
		messages_deque_ptr_t queue(new cached_messages_deque_t);
		queue->push_back(message);
		m_unhandled_messages[handle_name] = queue;
	}
	else {
		messages_deque_ptr_t queue = it->second;
		assert(queue);
		queue->push_back(message);
	}

	if (log_flag_enabled(PLOG_DEBUG)) {
		const static std::string message_str = "enqued msg (%d bytes) with uuid: %s to unhandled %s (%s)";
		std::string enqued_timestamp_str = message->enqued_timestamp().as_string();

		log(PLOG_DEBUG,
			message_str,
			message->size(),
			message->uuid().c_str(),
			message->path().as_string().c_str(),
			enqued_timestamp_str.c_str());
	}
}

boost::shared_ptr<std::deque<boost::shared_ptr<message_iface> > >
service_t::get_and_remove_unhandled_queue(const std::string& handle_name) {
	boost::mutex::scoped_lock lock(m_unhandled_mutex);

	messages_deque_ptr_t queue(new cached_messages_deque_t);

	unhandled_messages_map_t::iterator it = m_unhandled_messages.find(handle_name);
	if (it == m_unhandled_messages.end()) {
		return queue;
	}

	queue = it->second;
	m_unhandled_messages.erase(it);

	return queue;
}

void
service_t::destroy_handle(const std::string& handle_name) {
	boost::mutex::scoped_lock lock(m_handles_mutex);

	handles_map_t::iterator it = m_handles.find(handle_name);

	if (it == m_handles.end()) {
		log(PLOG_ERROR, "unable to DESTROY HANDLE [%s], handle object missing.", handle_name.c_str());
		return;
	}

	handle_ptr_t handle = it->second;
	assert(handle);

	log(PLOG_WARNING, "DESTROY HANDLE [%s]", handle_name.c_str());

	// retrieve message cache and terminate all handle activity
	handle->kill();

	boost::shared_ptr<message_cache_t> mcache = handle->messages_cache();
	
	log(PLOG_DEBUG, "messages cache - start");
	//mcache->log_stats();

	mcache->make_all_messages_new();

	log(PLOG_DEBUG, "messages cache - end");
	mcache->log_stats();

	const messages_deque_ptr_t& handle_queue = mcache->new_messages();
	
	log(PLOG_DEBUG, "handle_queue size: %d", handle_queue->size());

	append_to_unhandled(handle_name, handle_queue);

	m_handles.erase(it);
	lock.unlock();

	log(PLOG_DEBUG, "DESTROY HANDLE [%s] DONE", handle_name.c_str());
}

void
service_t::append_to_unhandled(const std::string& handle_name,
							  const messages_deque_ptr_t& handle_queue)
{
	assert(handle_queue);

	if (handle_queue->empty()) {
		log(PLOG_DEBUG, "handle_queue->empty()");
		return;
	}

	// in case there are messages, store them		
	log(PLOG_DEBUG, "moving message queue from handle [%s.%s] to service, queue size: %d",
					m_info.name.c_str(),
					handle_name.c_str(),
					handle_queue->size());

	boost::mutex::scoped_lock lock(m_unhandled_mutex);

	// find corresponding unhandled message queue
	unhandled_messages_map_t::iterator it = m_unhandled_messages.find(handle_name);

	messages_deque_ptr_t queue;
	if (it == m_unhandled_messages.end()) {
		queue.reset(new cached_messages_deque_t);
		m_unhandled_messages[handle_name] = queue;
	}
	else {
		queue = it->second;
	}

	assert(queue);
	queue->insert(queue->end(), handle_queue->begin(), handle_queue->end());

	// clear metadata
	for (cached_messages_deque_t::iterator it = queue->begin(); it != queue->end(); ++it) {
		(*it)->mark_as_sent(false);
		(*it)->set_ack_received(false);
	}

	log(PLOG_DEBUG, "moving message queue done.");
}

void
service_t::get_outstanding_handles(const handles_endpoints_t& handles_endpoints,
								   handles_info_list_t& outstanding_handles)
{
	boost::mutex::scoped_lock lock(m_handles_mutex);

	for (handles_map_t::iterator it = m_handles.begin(); it != m_handles.end(); ++it) {
		const std::string& handle_name = it->first;

		handles_endpoints_t::const_iterator hit = handles_endpoints.find(handle_name);
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

	handles_endpoints_t::const_iterator it = handles_endpoints.begin();
	for (; it != handles_endpoints.end(); ++it) {
		const std::string& handle_name = it->first;

		handles_map_t::iterator hit = m_handles.find(handle_name);
		if (hit == m_handles.end()) {
			handle_info_t hinfo(handle_name, m_info.app, m_info.name);
			new_handles.push_back(hinfo);
		}
	}
}

void
service_t::refresh_handles(const handles_endpoints_t& handles_endpoints) {
	handles_info_list_t outstanding_handles;
	handles_info_list_t new_handles;

	get_outstanding_handles(handles_endpoints, outstanding_handles);
	get_new_handles(handles_endpoints, new_handles);

	remove_outstanding_handles(outstanding_handles);
	update_existing_handles(handles_endpoints);
	create_new_handles(new_handles, handles_endpoints);
}

void
service_t::remove_outstanding_handles(const handles_info_list_t& handles_info) {
	if (handles_info.empty()) {
		return;
	}

	for (size_t i = 0; i < handles_info.size(); ++i) {
		destroy_handle(handles_info[i].name);
	}
}

void
service_t::update_existing_handles(const handles_endpoints_t& handles_endpoints) {
	boost::mutex::scoped_lock lock(m_handles_mutex);

	handles_map_t::iterator it = m_handles.begin();
	for (; it != m_handles.end(); ++it) {

		handles_endpoints_t::const_iterator eit = handles_endpoints.find(it->first);

		if (eit != handles_endpoints.end()) {
			handle_ptr_t handle = it->second;
			handle->update_endpoints(eit->second);
		}
	}
}

void
service_t::create_handle(const handle_info_t& handle_info,
						 const handles_endpoints_t& handles_endpoints)
{
	boost::mutex::scoped_lock lock(m_handles_mutex);
	const std::string& handle_name = handle_info.name;

	log(PLOG_INFO,
		"CREATE HANDLE [%s].[%s].[%s]",
		m_info.name.c_str(),
		m_info.app.c_str(),
		handle_name.c_str());

	// create new handle
	handles_endpoints_t::const_iterator it = handles_endpoints.find(handle_name);
	if (it == handles_endpoints.end()) {
		throw internal_error("no endpoints for new handle " + handle_info.as_string());
	}

	handle_ptr_t handle(new dealer::handle_t(handle_info, it->second, context()));
	handle->set_responce_callback(boost::bind(&service_t::enqueue_responce, this, _1));

	// retrieve unhandled queue
	messages_deque_ptr_t queue = get_and_remove_unhandled_queue(handle_name);

	if (!queue->empty()) {
		handle->assign_message_queue(queue);

		log(PLOG_DEBUG,
			"assign unhandled message queue to handle %s, queue size: %d",
			handle_info.as_string().c_str(),
			queue->size());
	}
	else {
		log(PLOG_DEBUG,
			"no unhandled message queue for handle %s",
			handle_info.as_string().c_str());
	}

	// append new handle	
	m_handles[handle_name] = handle;
}

void
service_t::create_new_handles(const handles_info_list_t& handles_info,
							  const handles_endpoints_t& handles_endpoints)
{
	// no handles to create
	if (handles_info.empty()) {
		return;
	}

	// create handles from info and endpoints
	for (size_t i = 0; i < handles_info.size(); ++i) {
		create_handle(handles_info[i], handles_endpoints);
	}
}

void
service_t::check_for_deadlined_messages() {
	boost::mutex::scoped_lock lock(m_unhandled_mutex);

	unhandled_messages_map_t::iterator it = m_unhandled_messages.begin();

	for (; it != m_unhandled_messages.end(); ++it) {
		messages_deque_ptr_t queue = it->second;

		// create tmp queue
		messages_deque_ptr_t not_expired_queue(new cached_messages_deque_t);
		messages_deque_ptr_t expired_queue(new cached_messages_deque_t);
		bool found_expired = false;

		cached_messages_deque_t::iterator qit = queue->begin();
		for (;qit != queue->end(); ++qit) {
			if ((*qit)->is_expired()) {
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

		// create error response_t for deadlined message
		cached_messages_deque_t::iterator expired_qit = expired_queue->begin();

		for (;expired_qit != expired_queue->end(); ++expired_qit) {
			cached_response_t response((*expired_qit)->uuid(),
									   "",
									   (*expired_qit)->path(),
									   deadline_error,
									   "message expired");

			if (log_flag_enabled(PLOG_ERROR)) {
				std::string enqued_timestamp_str = (*expired_qit)->enqued_timestamp().as_string();
				std::string sent_timestamp_str = (*expired_qit)->sent_timestamp().as_string();
				std::string curr_timestamp_str = time_value::get_current_time().as_string();

				std::string log_str = "deadline policy exceeded, for unhandled message %s, (enqued: %s, sent: %s, curr: %s)";

				log(PLOG_ERROR,
					log_str,
					(*expired_qit)->uuid().c_str(),
					enqued_timestamp_str.c_str(),
					sent_timestamp_str.c_str(),
					curr_timestamp_str.c_str());
			}

			enqueue_responce(response);
		}
	}
}

} // namespace dealer
} // namespace cocaine
