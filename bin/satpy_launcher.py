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

import argparse
import logging

from trollflow2.logging import logging_on
from trollflow2.launcher import Runner
from multiprocessing import Manager


def parse_args():
    """Parse commandline arguments."""
    parser = argparse.ArgumentParser(
        description='Launch trollflow2 processing with Satpy listening on the specified Posttroll topic(s)')
    parser.add_argument("topic", nargs='*',
                        help="Topic to listen to",
                        type=str)
    parser.add_argument("product_list",
                        help="The yaml file with the product list",
                        type=str)
    parser.add_argument("-m", "--test_message",
                        help="File path with the message used for testing offline. This implies threaded running.",
                        type=str, required=False)
    parser.add_argument("-t", "--threaded",
                        help="Run the product generation in threads instead of processes.",
                        action='store_true')

    parser.add_argument("-c", "--log-config",
                        help="Log config file (yaml) to use",
                        type=str, required=False)
    parser.add_argument('-n', "--nameserver", required=False, type=str,
                        help="Nameserver to connect to", default='localhost')
    parser.add_argument('-a', "--addresses", required=False, type=str,
                        help=("Add direct TCP port connection.  Can be used several times: "
                              "'-a tcp://127.0.0.1:12345 -a tcp://123.456.789.0:9013'"),
                        action="append")

    return parser.parse_args()


def main():
    """Launch trollflow2."""
    args = vars(parse_args())

    log_config = args.pop("log_config", None)
    if log_config is not None:
        with open(log_config) as fd:
            import yaml
            log_config = yaml.safe_load(fd.read())

    logger = logging.getLogger("satpy_launcher")

    log_queue = Manager().Queue()

    with logging_on(log_queue, log_config):
        logger.warning("Launching Satpy-based runner.")
        product_list = args.pop("product_list")
        test_message = args.pop("test_message")
        threaded = args.pop("threaded")
        connection_parameters = args

        runner = Runner(product_list, log_queue, connection_parameters, test_message, threaded)
        runner.run()


if __name__ == "__main__":
    main()
