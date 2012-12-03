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

#ifndef _COCAINE_DEALER_COCAINE_ENDPOINT_HPP_INCLUDED_
#define _COCAINE_DEALER_COCAINE_ENDPOINT_HPP_INCLUDED_

#include <string>

#include <boost/lexical_cast.hpp>

#include "cocaine/dealer/utils/progress_timer.hpp"

namespace cocaine {
namespace dealer {

 // predeclaration
struct cocaine_endpoint_t {
public:
	cocaine_endpoint_t() {}

	cocaine_endpoint_t(const std::string& endpoint_, const std::string& route_, int weight_ = 0) :
		endpoint(endpoint_),
		route(route_),
		weight(weight_) {}

	~cocaine_endpoint_t() {}

	cocaine_endpoint_t(const cocaine_endpoint_t& rhs) :
		endpoint(rhs.endpoint),
		route(rhs.route),
		weight(rhs.weight),
		announce_timer(rhs.announce_timer) {}

	cocaine_endpoint_t& operator = (const cocaine_endpoint_t& rhs) {
		if (this != &rhs) {
			endpoint = rhs.endpoint;
			route = rhs.route;
			weight = rhs.weight;
			announce_timer = rhs.announce_timer;
		}

		return *this;
	}

	bool operator == (const cocaine_endpoint_t& rhs) const {
		return (endpoint == rhs.endpoint &&
				route == rhs.route);
	}

	bool operator != (const cocaine_endpoint_t& rhs) const {
		return (!(*this == rhs));
	}

	bool operator < (const cocaine_endpoint_t& rhs) const {
		return ((endpoint + route) < (rhs.endpoint + rhs.route));
	}

	std::string as_string() const {
		std::string str;
		str += "endpoint: " + endpoint + ", ";
		str += "route: " + route + ", ";
		str += "weight: " + boost::lexical_cast<std::string>(weight) + ", ";
		str += "announce: " + announce_timer.started_at().as_string();

		return str;
	}

	std::string		endpoint;
	std::string		route;
	int				weight;
	progress_timer	announce_timer;
};

} // namespace dealer
} // namespace cocaine

#endif // _COCAINE_DEALER_COCAINE_ENDPOINT_HPP_INCLUDED_
