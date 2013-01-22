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

#ifndef _COCAINE_DEALER_EBLOB_STORAGE2_HPP_INCLUDED_
#define _COCAINE_DEALER_EBLOB_STORAGE2_HPP_INCLUDED_

#include <string>
#include <map>
#include <stdexcept>

#include <boost/shared_ptr.hpp>
#include <boost/utility.hpp>
#include <boost/lexical_cast.hpp>

#include <eblob/eblob.hpp>

#include "cocaine/dealer/utils/smart_logger.hpp"
#include "cocaine/dealer/utils/error.hpp"
#include "cocaine/dealer/storage/eblob.hpp"
#include "cocaine/dealer/core/dealer_object.hpp"

namespace cocaine {
namespace dealer {

class eblob_storage2_t : private boost::noncopyable, public dealer_object_t {
public:
	eblob_storage2_t(const boost::shared_ptr<context_t>& ctx, bool logging_enabled) :
		dealer_object_t(ctx, logging_enabled)
	{
		// get storage settings
		boost::shared_ptr<configuration_t> config = ctx->config();

		// init storage
	}

	~eblob_storage2_t() {}

	void write(const std::string& namespace,
			   const std::string& key,
			   void* data,
			   size_t size)
	{

	}

	std::string read(const std::string& namespace,
					 const std::string& key)
	{

	}

	void remove_all(const std::string& namespace)
	{

	}

	void remove(const std::string& namespace,
				const std::string& key)
	{

	}

	unsigned long long items_count(const std::string& namespace = "")
	{

	}

private:
	/*
	boost::shared_ptr<eblob_t> get_eblob(const std::string& namespace) {
		std::map<std::string, boost::shared_ptr<eblob_t> >::const_iterator it = m_eblobs.find(nm);

		// no such eblob_t was opened
		if (it == m_eblobs.end()) {
			std::string error_msg = "no eblob_t storage object with path: " + m_path + nm;
			error_msg += " at " + std::string(BOOST_CURRENT_FUNCTION);
			throw internal_error(error_msg);
		}

		return it->second;
	}
	*/
	std::map<std::string, boost::shared_ptr<eblob_t> > m_eblobs;
};

} // namespace dealer
} // namespace cocaine

#endif // _COCAINE_DEALER_EBLOB_STORAGE2_HPP_INCLUDED_
