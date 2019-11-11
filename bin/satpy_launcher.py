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

from trollflow2.launcher import run
import logging


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
                        help="File path with the message used for testing offline",
                        type=str, required=False)
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
    args = parse_args()
    prod_list = args.product_list
    test_message = args.test_message
    topics = args.topic
    nameserver = args.nameserver
    addresses = args.addresses

    if args.log_config is not None:
        with open(args.log_config) as fd:
            import yaml
            log_dict = yaml.load(fd.read())
            logging.config.dictConfig(log_dict)
    else:
        from satpy.utils import debug_on
        debug_on()

    run(prod_list, topics=topics, test_message=test_message,
        nameserver=nameserver, addresses=addresses)


if __name__ == "__main__":
    main()
