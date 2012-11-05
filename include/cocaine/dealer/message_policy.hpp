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

#ifndef _COCAINE_DEALER_MESSAGE_POLICY_HPP_INCLUDED_
#define _COCAINE_DEALER_MESSAGE_POLICY_HPP_INCLUDED_

#include <string>
#include <sstream>
#include <iomanip>

#include <msgpack.hpp>

#include <cocaine/dealer/types.hpp>
#include <cocaine/dealer/defaults.hpp>
#include <cocaine/dealer/utils/math.hpp>

namespace cocaine {
namespace dealer {

struct message_policy_t {
    message_policy_t() :
        urgent(defaults_t::policy_urgent),
        persistent(defaults_t::policy_persistent),
        timeout(defaults_t::policy_chunk_timeout),
        ack_timeout(defaults_t::policy_ack_timeout),
        deadline(defaults_t::policy_message_deadline),
        max_retries(defaults_t::policy_max_retries) {}

    message_policy_t(bool urgent_,
                     bool persistent_,
                     float timeout_,
                     float ack_timeout_,
                     float deadline_,
                     int max_retries_) :
        urgent(urgent_),
        persistent(persistent_),
        timeout(timeout_),
        ack_timeout(ack_timeout_),
        deadline(deadline_),
        max_retries(max_retries_) {}

    message_policy_t(const message_policy_t& mp) {
        *this = mp;
    }

    message_policy_t& operator = (const message_policy_t& rhs) {
        if (this == &rhs) {
            return *this;
        }

        urgent = rhs.urgent;
        persistent = rhs.persistent;
        timeout = rhs.timeout;
        ack_timeout = rhs.ack_timeout;
        deadline = rhs.deadline;
        max_retries = rhs.max_retries;

        return *this;
    }

    bool operator == (const message_policy_t& rhs) const {
        return (urgent      == rhs.urgent &&
                persistent  == rhs.persistent &&
                math::compare_floats(timeout, rhs.timeout) &&
                math::compare_floats(ack_timeout, rhs.ack_timeout) &&
                math::compare_floats(deadline, rhs.deadline) &&
                max_retries == rhs.max_retries);
    }

    bool operator != (const message_policy_t& rhs) const {
        return !(*this == rhs);
    }

    policy_t server_policy() const {
        return policy_t(urgent, timeout, deadline);
    }

    policy_t server_policy() {
        return policy_t(urgent, timeout, deadline);
    }

    std::string as_string() const {
        std::stringstream sstream;

        sstream << std::boolalpha << std::fixed << std::setprecision(6);
        sstream << "urgent: " << urgent << ", ";
        sstream << "persistent: " << persistent << ", ";
        sstream << "timeout: " << timeout << ", ";
        sstream << "ack_timeout: " << ack_timeout << ", ";
        sstream << "deadline: " << deadline << ", ";
        sstream << "max_retries: " << max_retries;

        return sstream.str();
    }

    bool        urgent;
    bool        persistent;
    double      timeout;
    double      ack_timeout;
    double      deadline;
    int         max_retries;

    MSGPACK_DEFINE(urgent,
                   timeout,
                   ack_timeout,
                   deadline,
                   max_retries);
};

} // namespace dealer
} // namespace cocaine

#endif // _COCAINE_DEALER_MESSAGE_POLICY_HPP_INCLUDED_
