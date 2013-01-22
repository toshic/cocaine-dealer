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

#include "cocaine/dealer/storage/eblob2.hpp"

namespace cocaine {
namespace dealer {

eblob2_t::eblob2_t() {
}

eblob2_t::eblob2_t(const std::string& name,
				   const boost::shared_ptr<context_t>& ctx,
				   bool logging_enabled) :
	m_alive_items_count(0),
	dealer_object_t(ctx, logging_enabled)
{
	m_path = config()->eblob_path() + name;

	// create eblob logger
	m_eblob_logger.reset(new ioremap::eblob::eblob_logger("/dev/stdout", 0));

	// create config
    eblob_config cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.file			= const_cast<char*>(m_path.c_str());
    cfg.log				= m_eblob_logger->log();
    cfg.sync			= config()->eblob_sync_interval();
    cfg.blob_size		= config()->eblob_blob_size();
    cfg.defrag_timeout	= config()->eblob_defrag_timeout();
    cfg.iterate_threads	= config()->eblob_thread_pool_size();

    // create eblob
    m_storage.reset(new ioremap::eblob::eblob(&cfg));

	log("eblob at path: %s created.", m_path.c_str());
}

eblob2_t::~eblob2_t() {
	log("eblob at path: %s closed.", m_path.c_str());
}

void
eblob2_t::write(const std::string& key,
				const std::string& value,
				int column)
{
	if (!m_storage.get()) {
		std::string error_msg = "empty eblob storage object at " + std::string(BOOST_CURRENT_FUNCTION);
		error_msg += " key: " + key + " column: " + boost::lexical_cast<std::string>(column);
		throw internal_error(error_msg);
	}

	if (column < 0) {
		std::string error_msg = "bad column index at " + std::string(BOOST_CURRENT_FUNCTION);
		error_msg += " key: " + key + " column: " + boost::lexical_cast<std::string>(column);
		throw internal_error(error_msg);
	}

	// 2DO: truncate written value
	m_storage->write_hashed(key, value, 0, BLOB_DISK_CTL_OVERWRITE, column);
}

void
eblob2_t::write(const std::string& key,
				void* data,
				size_t size,
				int column)
{
	if (!m_storage.get()) {
		std::string error_msg = "empty eblob storage object at " + std::string(BOOST_CURRENT_FUNCTION);
		error_msg += " key: " + key + " column: " + boost::lexical_cast<std::string>(column);
		throw internal_error(error_msg);
	}

	if (column < 0) {
		std::string error_msg = "bad column index at " + std::string(BOOST_CURRENT_FUNCTION);
		error_msg += " key: " + key + " column: " + boost::lexical_cast<std::string>(column);
		throw internal_error(error_msg);
	}

	// 2DO: truncate written value
	std::string value(reinterpret_cast<char*>(data), reinterpret_cast<char*>(data) + size);
	m_storage->write_hashed(key, value, 0, BLOB_DISK_CTL_OVERWRITE, column);
}

std::string
eblob2_t::read(const std::string& key, int column) {
	if (!m_storage.get()) {
		std::string error_msg = "empty eblob storage object at " + std::string(BOOST_CURRENT_FUNCTION);
		error_msg += " key: " + key + " column: " + boost::lexical_cast<std::string>(column);
		throw internal_error(error_msg);
	}

	return m_storage->read_hashed(key, 0, 0, column);
}

void
eblob2_t::remove_all(const std::string &key) {
	if (!m_storage.get()) {
		std::string error_msg = "empty eblob storage object at " + std::string(BOOST_CURRENT_FUNCTION);
		error_msg += " key: " + key;
		throw internal_error(error_msg);
	}

	eblob_key ekey;
	m_storage->key(key, ekey);
	m_storage->remove_all(ekey);
}

void
eblob2_t::remove(const std::string& key, int column) {
	if (!m_storage.get()) {
		std::string error_msg = "empty eblob storage object at " + std::string(BOOST_CURRENT_FUNCTION);
		error_msg += " key: " + key + " column: " + boost::lexical_cast<std::string>(column);
		throw internal_error(error_msg);
	}

	m_storage->remove_hashed(key, column);
}

unsigned long long
eblob2_t::items_count() {
	if (!m_storage.get()) {
		std::string error_msg = "empty eblob storage object at " + std::string(BOOST_CURRENT_FUNCTION);
		throw internal_error(error_msg);
	}

	return m_storage->elements();
}

unsigned long long
eblob2_t::alive_items_count() {
	m_alive_items_count = 0;

	if (!m_storage) {
		std::string error_msg = "empty eblob storage object at " + std::string(BOOST_CURRENT_FUNCTION);
		throw internal_error(error_msg);
	}

	m_iteration_callback = boost::bind(&eblob2_t::counting_iteration_callback, this, _1, _2, _3, _4);

	eblob_iterate_control ctl;
    memset(&ctl, 0, sizeof(ctl));

    ctl.priv = this;
    ctl.iterator_cb.iterator = &eblob2_t::iteration_callback;
    ctl.thread_num = config()->eblob_thread_pool_size();

    m_storage->iterate(ctl);

    return m_alive_items_count;
}

void
eblob2_t::iterate(iteration_callback_t callback) {
	if (!callback) {
		return;
	}

	m_iteration_callback = callback;

	eblob_iterate_control ctl;
    memset(&ctl, 0, sizeof(ctl));

    ctl.priv = this;
    ctl.iterator_cb.iterator = &eblob2_t::iteration_callback;
    ctl.thread_num = config()->eblob_thread_pool_size();

    m_storage->iterate(ctl);
}

int
eblob2_t::iteration_callback(eblob_disk_control* dc,
							 eblob_ram_control* rc,
							 void* data,
							 void* priv,
							 __attribute__ ((unused)) void* thread_priv)
{
	eblob2_t* eb = reinterpret_cast<eblob2_t*>(priv);

	// 2DO MAKE PERSISTENCE SINGLE_COLUMN
	eb->iteration_callback_instance((char*)dc->key.id, data, rc->size, 0);

	return 0;
}

void
eblob2_t::iteration_callback_instance(const std::string& key,
									 void* data,
									 uint64_t size,
									 int column)
{
	if (m_iteration_callback) {
		m_iteration_callback(key, data, size, column);
	}
}

void
eblob2_t::counting_iteration_callback(const std::string& key,
									 void* data,
									 uint64_t size,
									 int column)
{
	++m_alive_items_count;
}

} // namespace dealer
} // namespace cocaine
