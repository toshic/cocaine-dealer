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

#ifndef _COCAINE_DEALER_SOCKET_HPP_INCLUDED_
#define _COCAINE_DEALER_SOCKET_HPP_INCLUDED_

#include <boost/type_traits/remove_const.hpp>
#include <boost/tuple/tuple.hpp>

#include <msgpack.hpp>
#include <zmq.hpp>

#if ZMQ_VERSION < 20200
	#error ZeroMQ version 2.2.0+ required!
#endif

#include "cocaine/dealer/core/birth_control.hpp"
#include "cocaine/dealer/core/context.hpp"
#include "cocaine/dealer/core/inetv4_endpoint.hpp"
#include "cocaine/dealer/utils/uuid.hpp"

namespace cocaine {
namespace dealer {

// Raw message container
template<class T>
struct raw {
	raw(T& value_) : value(value_) {};
	T& value;
};

// Specialize this to disable specific type serialization.
template<class T>
struct serialization_traits;

template<>
struct serialization_traits<std::string> {
	static void pack(zmq::message_t& message, const std::string& value) {
		message.rebuild(value.size());
		memcpy(message.data(), value.data(), value.size());
	}

	static bool unpack(zmq::message_t& message, std::string& value) {
		value.assign(
			static_cast<const char*>(message.data()),
			message.size()
		);

		return true;
	}
};

class socket_t : public boost::noncopyable, public birth_control<socket_t> {
	public:
		socket_t(const boost::shared_ptr<context_t>& context, int type);
		socket_t(const boost::shared_ptr<context_t>& context, int type, const std::string& route);

		void bind(const inetv4_endpoint_t& endpoint);
		void bind(const std::string& endpoint);

		void connect(const inetv4_endpoint_t& endpoint);
		void connect(const std::string& endpoint);
		void disconnect(const inetv4_endpoint_t& endpoint);
		void disconnect(const std::string& endpoint);

		void drop();

		bool send(zmq::message_t& message, int flags = 0) {
			return m_socket.send(message, flags);
		}

		bool send(std::string& message, int flags = 0) {
			zmq::message_t chunk(message.size());
			memcpy((void *)chunk.data(), message.data(), message.size());

			return m_socket.send(chunk, flags);
		}

		bool send_empty(int flags = 0) {
			zmq::message_t chunk(0);
			return m_socket.send(chunk, flags);
		}

		template<class T>
		bool send_packed(const T& value, int flags = 0) {
			msgpack::sbuffer buffer;
			msgpack::pack(buffer, value);
			
			zmq::message_t message(buffer.size());
			memcpy(message.data(), buffer.data(), buffer.size());
			
			return send(message, flags);
		}
		
		template<class T>
		bool send(const raw<T>& object, int flags = 0) {
			zmq::message_t message;
			
			serialization_traits< typename boost::remove_const<T>::type >::pack(message, object.value);
			
			return send(message, flags);
		}

		bool recv(zmq::message_t* message, int flags = 0) {
			return m_socket.recv(message, flags);
		}

		bool recv(std::string& message, int flags = 0) {
			zmq::message_t msg;
			if (!m_socket.recv(&msg, flags)) {
				return false;
			}

			message.clear();
			message.append(reinterpret_cast<char*>(msg.data()), msg.size());

			return true;
		}

		template<class T>
		bool recv_packed(T& result, int flags = 0) {
			zmq::message_t message;
			msgpack::unpacked unpacked;

			if(!recv(&message, flags)) {
				return false;
			}
		   
			try { 
				msgpack::unpack(
					&unpacked,
					static_cast<const char*>(message.data()),
					message.size()
				);
				
				unpacked.get().convert(&result);
			}
			catch(const msgpack::type_error& e) {
				throw std::runtime_error("corrupted object");
			}
			catch(const std::bad_cast& e) {
				throw std::runtime_error("corrupted object - type mismatch");
			}

			return true;
		}
	  
		template<class T>
		bool recv(raw<T>& result, int flags = 0) {
			zmq::message_t message;

			if(!recv(&message, flags)) {
				return false;
			}

			return serialization_traits< typename boost::remove_const<T>::type >::unpack(message, result.value);
		}

		void get_sockopt(int name, void* value, size_t* size) {
			m_socket.getsockopt(name, value, size);
		}

		void set_sockopt(int name, const void* value, size_t size) {
			m_socket.setsockopt(name, value, size);
		}

		void set_linger(int value) {
			set_sockopt(ZMQ_LINGER, &value, sizeof(value));
		}

		void close() {
			m_socket.close();
		}

		void set_identity(const std::string& ident, bool gen_uuid = false) {
			std::string identity = ident;

			if (gen_uuid) {
				wuuid_t sock_uuid;
				sock_uuid.generate();
				identity += sock_uuid.as_human_readable_string();
			}

			set_sockopt(ZMQ_IDENTITY, identity.data(), identity.size());
		}

		void subscribe(const std::string& filter = "") {
			if (m_type != ZMQ_SUB) {
				return;
			}

			set_sockopt(ZMQ_SUBSCRIBE, filter.data(), filter.size());
		}

		bool more() {
			int64_t rcvmore = 0;
			size_t size = sizeof(rcvmore);

			get_sockopt(ZMQ_RCVMORE, &rcvmore, &size);

			return rcvmore != 0;
		}

		std::string identity() {
			char identity[256] = {0};
			size_t size = sizeof(identity);

			get_sockopt(ZMQ_IDENTITY, &identity, &size);

			return identity;
		}

		int fd() {
			int fd = 0;
			size_t size = sizeof(fd);

			get_sockopt(ZMQ_FD, &fd, &size);

			return fd;
		}

		bool pending(unsigned long event = ZMQ_POLLIN) {
			unsigned long events = 0;
			size_t size = sizeof(events);

			get_sockopt(ZMQ_EVENTS, &events, &size);

			return (events & event) == event;
		}

		std::string identity() const {
			return m_identity;
		}

		zmq::socket_t& zmq_socket() {
			return m_socket;
		}

		bool can_connect(const std::string& endpoint) {
			inetv4_endpoint_t v4_endpoint(endpoint);
			return can_connect(v4_endpoint);
		}

		bool can_connect(const inetv4_endpoint_t& endpoint) {
			std::set<inetv4_endpoint_t>::const_iterator it = m_endpoints.find(endpoint);

			if (it == m_endpoints.end()) {
				m_endpoints.insert(endpoint);
				return true;
			}
			
			return false;
		}

	private:
		zmq::socket_t				m_socket;
		std::set<inetv4_endpoint_t>	m_endpoints;
		int m_type;
		std::string m_identity;
};

} // namespace dealer
} // namespace cocaine

#endif