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

#ifndef _COCAINE_DEALER_STORAGE_HPP_INCLUDED_
#define _COCAINE_DEALER_STORAGE_HPP_INCLUDED_

#include <string>

#include <boost/utility.hpp>
#include <boost/shared_ptr.hpp>

#include "cocaine/dealer/core/context.hpp"

namespace cocaine {
namespace dealer {

template <typename T>
class storage : private boost::noncopyable {
public:
	storage_t(const boost::shared_ptr<context_t>& ctx,
			  bool logging_enabled = false) : m_storage(ctx, logging_enabled) {}

	~storage_t() {}

	// write
	void write(const std::string& key,
			   const std::string& value)
	{
		m_storage.write("", key, value.data(), value.size());
	}

	void write(const std::string& namespace,
			   const std::string& key,
			   const std::string& value)
	{
		m_storage.write(namespace, key, value.data(), value.size());
	}

	void write(const std::string& key,
			   void* data,
			   size_t size)
	{
		m_storage.write("", key, data, size);
	}

	void write(const std::string& namespace,
			   const std::string& key,
			   void* data,
			   size_t size)
	{
		m_storage.write(namespace, key, data, size);
	}

	// read
	std::string read(const std::string& key)
	{
		return m_storage.read("", key);
	}

	std::string read(const std::string& namespace,
					 const std::string& key)
	{
		return m_storage.read(namespace, key);
	}

	// remove
	void remove_all(const std::string& namespace = "")
	{
		m_storage.remove_all(namespace);
	}

	void remove(const std::string& namespace,
				const std::string& key)
	{
		m_storage.remove(namespace, key);
	}

	void remove(const std::string& key)
	{
		m_storage.remove("", key);
	}

	// info
	unsigned long long items_count(const std::string& namespace = "");

private:
	T m_storage;
};

} // namespace dealer
} // namespace cocaine

#endif // _COCAINE_DEALER_STORAGE_HPP_INCLUDED_
