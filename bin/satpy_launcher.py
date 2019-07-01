#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2019 Pytroll developers
#
# Author(s):
#
#   Martin Raspaud <martin.raspaud@smhi.se>
#   Panu Lahtinen <pnuu+git@iki.fi>
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
"""The satpy launcher."""

from logging import getLogger
import argparse

from trollflow2.launcher import run
from satpy.utils import debug_on
debug_on()

LOG = getLogger(__name__)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description='Launch trollflow2 processing with Satpy listening on the specified Posttroll topic(s)')
    parser.add_argument("topic", nargs='*',
                        help="Topic to listen to",
                        type=str)
    parser.add_argument("product_list",
                        help="The yaml file with the product list",
                        type=str)
    parser.add_argument("--test_message", '-m',
                        help="File path with the message used for testing offline",
                        type=str, required=False)

    args = parser.parse_args()
    prod_list = args.product_list
    test_message = args.test_message
    topics = args.topic

    run(prod_list, topics=topics, test_message=test_message)
