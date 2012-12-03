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

#ifndef _COCAINE_DEALER_DEFAULTS_HPP_INCLUDED_
#define _COCAINE_DEALER_DEFAULTS_HPP_INCLUDED_

#include <cocaine/dealer/utils/smart_logger.hpp>

namespace cocaine {
namespace dealer {

enum e_autodiscovery_type {
	AT_UNDEFINED = 1,
	AT_MULTICAST,
	AT_HTTP,
	AT_FILE
};

enum e_message_cache_type {
	RAM_ONLY = 1,
	PERSISTENT
};

struct defaults_t {
	// common
	static const int				protocol_version	= 1;
	static const unsigned short		control_port		= 5000;
	static const float				endpoint_timeout;
	static const size_t 			max_message_size	= 2147483648; // 2 gb (bytes)

	// logger
	static const enum e_logger_type	logger_type = STDOUT_LOGGER;
	static const unsigned int	logger_flags	= PLOG_NONE;

	// policy defaults
	static const bool	policy_urgent		= false;
	static const bool	policy_persistent	= false;
	static const int	policy_max_retries	= 0;
	static const float	policy_ack_timeout;
	static const float 	policy_chunk_timeout;
	static const float	policy_message_deadline;

	// persistance
	static const enum e_message_cache_type message_cache_type = RAM_ONLY;

	static const std::string	eblob_path;
	static const size_t			eblob_blob_size		= 2147483648; // 2 gb
	static const int			eblob_sync_interval	= 2;
	static const int			eblob_thread_pool_size	= 16;
	static const int			eblob_defrag_timeout	= 9999999;

	static const unsigned short	statistics_port			= 3333;
	static const int		statistics_protocol_version	= 1;
};

} // namespace dealer
} // namespace cocaine

#endif // _COCAINE_DEALER_DEFAULTS_HPP_INCLUDED_
