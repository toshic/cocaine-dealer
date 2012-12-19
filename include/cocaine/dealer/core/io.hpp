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

        void bind(const std::string& endpoint);
        void connect(const std::string& endpoint);
        void drop();

        bool send(zmq::message_t& message, int flags = 0) {
            return m_socket.send(message, flags);
        }

        template<class T>
        bool send(const T& value, int flags = 0) {
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

        bool recv(zmq::message_t * message, int flags = 0) {
            return m_socket.recv(message, flags);
        }

        template<class T>
        bool recv(T& result, int flags = 0) {
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

        void getsockopt(int name,
                        void * value,
                        size_t * size)
        {
            m_socket.getsockopt(name, value, size);
        }

        void setsockopt(int name,
                        const void * value,
                        size_t size)
        {
            m_socket.setsockopt(name, value, size);
        }

        bool more() {
            int64_t rcvmore = 0;
            size_t size = sizeof(rcvmore);

            getsockopt(ZMQ_RCVMORE, &rcvmore, &size);

            return rcvmore != 0;
        }

        std::string identity() {
            char identity[256] = {0};
            size_t size = sizeof(identity);

            getsockopt(ZMQ_IDENTITY, &identity, &size);

            return identity;
        }

        int fd() {
            int fd = 0;
            size_t size = sizeof(fd);

            getsockopt(ZMQ_FD, &fd, &size);

            return fd;
        }

        bool pending(unsigned long event = ZMQ_POLLIN) {
            unsigned long events = 0;
            size_t size = sizeof(events);

            getsockopt(ZMQ_EVENTS, &events, &size);

            return (events & event) == event;
        }

        std::string identity() const {
        	return m_identity;
        }

    private:
        zmq::socket_t m_socket;
        std::set<std::string> m_endpoints;
        int m_type;
        std::string m_identity;
};

} // namespace dealer
} // namespace cocaine

#endif