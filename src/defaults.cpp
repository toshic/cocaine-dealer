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

#include "cocaine/dealer/defaults.hpp"

namespace cocaine {
namespace dealer {

const std::string defaults_t::eblob_path		= "/tmp/pmq_eblob";
const float defaults_t::policy_ack_timeout		= 0.05; // seconds
const float defaults_t::policy_chunk_timeout	= 0.0;  // seconds
const float defaults_t::policy_message_deadline	= 0.0;  // seconds
const float defaults_t::endpoint_timeout        = 2.0;  // seconds

} // namespace dealer
} // namespace cocaine
