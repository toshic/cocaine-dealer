#!/usr/bin/env python
# encoding: utf-8
#
#    Copyright (c) 2011-2012 Andrey Sibiryov <me@kobology.ru>
#    Copyright (c) 2011-2012 Other contributors as noted in the AUTHORS file.
#
#    This file is part of Cocaine.
#
#    Cocaine is free software; you can redistribute it and/or modify
#    it under the terms of the GNU Lesser General Public License as published by
#    the Free Software Foundation; either version 3 of the License, or
#    (at your option) any later version.
#
#    Cocaine is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
#    GNU Lesser General Public License for more details.
#
#    You should have received a copy of the GNU Lesser General Public License
#    along with this program. If not, see <http://www.gnu.org/licenses/>. 
#

try:
    from setuptools import setup, Extension
except ImportError:
    from distutils.core import setup, Extension

setup(
    name = "cocaine",
    version = "0.10.1",
    description = "Python binding for Cocaine Dealer",
    long_description = "A python binding for libcocaine-dealer library, allow to send messages to Cocaine from python",
    url = "https://github.com/cocaine/cocaine-dealer",
    author = "Andrey Sibiryov",
    author_email = "me@kobology.ru",
    license = "BSD 2-Clause",
    platforms = ["Linux", "BSD", "MacOS"],
    packages = ["cocaine"],
    package_dir = {"cocaine": "bindings/python/cocaine"},
    ext_modules = [Extension("cocaine._client",
                             ["bindings/python/src/module.cpp", "bindings/python/src/client.cpp", "bindings/python/src/response.cpp"],
                             include_dirs = ["bindings/python/include"],
                             libraries = ["cocaine-dealer"])],
    requires = ["msgpack"]
)
