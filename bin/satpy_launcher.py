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

import sys
from logging import getLogger

from trollflow2.launcher import run
from satpy.utils import debug_on
debug_on()

LOG = getLogger(__name__)


def main():
    # Product list is always the last argument
    prod_list = sys.argv[-1]
    if len(sys.argv) > 2:
        # Collect all the topics, which can be either comma or space separated
        topics = []
        for arg in sys.argv[1:-1]:
            topics += arg.split(',')
    else:
        topics = None

    run(prod_list, topics=topics)


if __name__ == "__main__":
    main()
