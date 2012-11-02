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

#ifndef _COCAINE_DEALER_UUID_HPP_INCLUDED_
#define _COCAINE_DEALER_UUID_HPP_INCLUDED_

#include <cstring>
#include <uuid/uuid.h>

namespace cocaine {
namespace dealer {

class wuuid_t {
public:
    static const int UUID_SIZE = 16;

    wuuid_t(const wuuid_t& rhs) {
        memcpy(m_uuid, rhs.m_uuid, UUID_SIZE);
        m_str_human_readable_value = rhs.m_str_human_readable_value;
        m_str_value = rhs.m_str_value;
    }

    wuuid_t(const std::string& uuid) {
        memcpy(m_uuid, uuid.data(), UUID_SIZE);
    }

    wuuid_t(uuid_t uuid) {
        memcpy(m_uuid, uuid, UUID_SIZE);
    }

	wuuid_t() {
        memset(m_uuid, 0, sizeof(m_uuid));
    }

    void generate() {
        uuid_generate(m_uuid);
        m_str_value.clear();
        m_str_human_readable_value.clear();
    }

	const std::string& as_string() {
        if (m_str_value.empty()) {
    		m_str_value = std::string(reinterpret_cast<char*>(m_uuid), UUID_SIZE);
        }

		return m_str_value;
	}

    const std::string& as_human_readable_string() {
        if (m_str_human_readable_value.empty()) {
            char buff[37];
            uuid_unparse(m_uuid, buff);
            m_str_human_readable_value = buff;
        }

        return m_str_human_readable_value;
    }

    bool is_empty() {
        static uuid_t empty_uuid = {0};
        if (0 == memcmp(m_uuid, empty_uuid, UUID_SIZE)) {
            return true;
        }

        return false;
    }

    bool operator == (const wuuid_t& rhs) const {
        if (0 == memcmp(m_uuid, rhs.m_uuid, UUID_SIZE)) {
            return true;
        }

        return false;
    }

private:
    uuid_t      m_uuid;
    std::string m_str_human_readable_value;
    std::string m_str_value;
};

} // namespace dealer
} // namespace cocaine

#endif // _COCAINE_DEALER_UUID_HPP_INCLUDED_
