#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2013-2020 Pytroll developers

# Author(s):

#   Martin Raspaud <martin.raspaud@smhi.se>
#   Panu Lahtinen <pnuu+git@iki.fi>

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""Setup for trollflow2."""

from setuptools import setup, find_packages
import sys

try:
    # HACK: https://github.com/pypa/setuptools_scm/issues/190#issuecomment-351181286
    # Stop setuptools_scm from including all repository files
    import setuptools_scm.integration
    setuptools_scm.integration.find_files = lambda _: []
except ImportError:
    pass


install_requires = ['pyyaml', 'dpath', 'trollsift']

if "test" not in sys.argv:
    install_requires += ['posttroll', 'satpy>=0.32.0', 'pyorbital']

NAME = 'trollflow2'
README = open('README.md', 'r').read()

setup(name=NAME,
      description='Pytroll workflow execution framework',
      long_description=README,
      author='Martin Raspaud',
      author_email='martin.raspaud@smhi.se',
      classifiers=["Development Status :: 4 - Beta",
                   "Intended Audience :: Science/Research",
                   "License :: OSI Approved :: GNU General Public License v3 " +
                   "or later (GPLv3+)",
                   "Operating System :: OS Independent",
                   "Programming Language :: Python",
                   "Topic :: Scientific/Engineering"],
      url="https://github.com/pytroll/trollflow2",
      packages=find_packages(),
      scripts=['bin/satpy_launcher.py', ],
      data_files=[],
      zip_safe=False,
      install_requires=install_requires,
      tests_require=['mock'],
      python_requires='>=3.4',
      test_suite='trollflow2.tests.suite',
      use_scm_version=True
      )
