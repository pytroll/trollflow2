#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2019 Pytroll developers

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
# along with this program.  If not, see <http://www.gnu.org/licenses/>


from logging import getLogger
from posttroll.listener import ListenerContainer
from six.moves.queue import Empty as queue_empty
import sys
from multiprocessing import Process
import yaml
import time
from trollflow2 import gen_dict_extract
from collections import OrderedDict
from six.moves.urllib.parse import urlparse

"""The order of basic things is:
- Create the scene
- Generate the composites
- Resample
- Save to file
"""


LOG = getLogger(__name__)

def run(topics, prod_list):

    listener = ListenerContainer(topics=topics)

    while True:
        try:
            msg = listener.output_queue.get(True, 5)
        except KeyboardInterrupt:
            listener.stop()
            return
        except queue_empty:
            continue

        proc = Process(target=process, args=(msg, prod_list))
        proc.start()
        proc.join()
        time.sleep(5)


def message_to_job(msg, product_list):
    job = OrderedDict()
    # TODO: check the uri is accessible from the current host.
    job['input_filenames'] = [urlparse(uri).path for uri in gen_dict_extract(msg.data, 'uri')]
    job['product_list'] = product_list
    job['input_mda'] = msg.data.copy()

    return job


def process(msg, prod_list):
    with open(prod_list) as fd:
        config = yaml.load(fd.read())
    job = message_to_job(msg, config)
    for wrk in config['workers']:
        cwrk = wrk.copy()
        cwrk.pop('fun')(job, **cwrk)


def main():
    from satpy.utils import debug_on
    debug_on()
    topics = sys.argv[1].split(',')
    prod_list = sys.argv[2]
    run(topics, prod_list)


if __name__ == "__main__":
    main()
