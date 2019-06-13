#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2019 Pytroll developers
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>

import sys
import logging
import unittest

from trollflow2.tests import (test_trollflow2, test_launcher, test_dict_tools)


def suite():
    """The global test suite.
    """
    logging.basicConfig(level=logging.DEBUG)

    mysuite = unittest.TestSuite()
    mysuite.addTests(test_trollflow2.suite())
    mysuite.addTests(test_launcher.suite())
    mysuite.addTests(test_dict_tools.suite())

    return mysuite


def load_tests(loader, tests, pattern):
    return suite()
