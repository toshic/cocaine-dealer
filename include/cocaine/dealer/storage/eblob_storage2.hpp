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

#include "cocaine/dealer/storage/eblob2.hpp"
#include "cocaine/dealer/core/dealer_object.hpp"

namespace cocaine {
namespace dealer {

class eblob_storage2_t : private boost::noncopyable, public dealer_object_t {
public:
	eblob_storage2_t(const boost::shared_ptr<context_t>& ctx, bool logging_enabled) :
		dealer_object_t(ctx, logging_enabled) {}

	~eblob_storage2_t() {}

	typedef boost::shared_ptr<eblob2_t> shared_eblob_t;

	void write(const std::string& ns,
			   const std::string& key,
			   void* data,
			   size_t size)
	{
		shared_eblob_t eblob = get_eblob(ns);
		eblob->write(key, data, size);
	}

	std::string read(const std::string& ns,
					 const std::string& key)
	{
		shared_eblob_t eblob = get_eblob(ns);
		return eblob->read(key);
	}

	void remove(const std::string& ns,
				const std::string& key)
	{
		shared_eblob_t eblob = get_eblob(ns);
		return eblob->remove(key);
	}

	unsigned long long items_count(const std::string& ns = "")
	{
		shared_eblob_t eblob = get_eblob(ns);
		return eblob->items_count();
	}

private:
	shared_eblob_t get_eblob(const std::string& ns) {
		std::string eblob_name = ns;

		if (ns.empty()) {
			eblob_name = "default_eblob";
		}

		std::map<std::string, shared_eblob_t>::const_iterator it = m_eblobs.find(ns);

		if (it == m_eblobs.end()) {
			shared_eblob_t eb(new eblob2_t(eblob_name, context(), true));
			m_eblobs.insert(std::make_pair(eblob_name, eb));
			return eb;
		}

		return it->second;
	}

	std::map<std::string, shared_eblob_t> m_eblobs;
};

} // namespace dealer
} // namespace cocaine

#endif // _COCAINE_DEALER_EBLOB_STORAGE2_HPP_INCLUDED_
