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

from logging import getLogger
try:
    from posttroll.listener import ListenerContainer
except ImportError:
    ListenerContainer = None
from six.moves.queue import Empty as queue_empty
from multiprocessing import Process
import yaml
try:
    from yaml import UnsafeLoader
except ImportError:
    from yaml import Loader as UnsafeLoader
import time
from trollflow2 import gen_dict_extract, plist_iter, AbortProcessing
from collections import OrderedDict
import copy
from six.moves.urllib.parse import urlparse

"""The order of basic things is:
- Create the scene
- Generate the composites
- Resample
- Save to file
"""

LOG = getLogger("launcher")
DEFAULT_PRIORITY = 999


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


def get_area_priorities(product_list):
    """Get processing priorities and names for areas."""
    priorities = {}
    plist = product_list['product_list']
    for area in plist.keys():
        prio = plist[area].get('priority', DEFAULT_PRIORITY)
        if prio is None:
            prio = DEFAULT_PRIORITY
        if prio not in priorities:
            priorities[prio] = [area]
        else:
            priorities[prio].append(area)

    return priorities


def message_to_jobs(msg, product_list):
    """Convert a posttroll message *msg* to a list of jobs given a *product_list*."""
    formats = product_list['common'].get('formats', None)
    for product, pconfig in plist_iter(product_list['product_list'], level='product'):
        if 'formats' not in pconfig and formats is not None:
            pconfig['formats'] = formats.copy()
    jobs = OrderedDict()
    priorities = get_area_priorities(product_list)
    # TODO: check the uri is accessible from the current host.
    input_filenames = [urlparse(uri).path for uri in gen_dict_extract(msg.data, 'uri')]
    for prio, areas in priorities.items():
        jobs[prio] = OrderedDict()
        jobs[prio]['input_filenames'] = input_filenames.copy()
        jobs[prio]['input_mda'] = msg.data.copy()
        jobs[prio]['product_list'] = {}
        for section in product_list:
            if section == 'product_list':
                if section not in jobs[prio]['product_list']:
                    jobs[prio]['product_list'][section] = OrderedDict()
                for area in areas:
                    jobs[prio]['product_list'][section][area] = product_list[section][area]
            else:
                jobs[prio]['product_list'][section] = product_list[section]

    return jobs


def expand(yml):
    """Expand a yaml config so that aliases are copied.

    PFE http://disq.us/p/1tdbxgx
    """
    if isinstance(yml, dict):
        for key, value in yml.items():
            if isinstance(value, dict):
                expand(value)
                yml[key] = copy.deepcopy(yml[key])
    return yml


def process(msg, prod_list):
    try:
        with open(prod_list) as fd:
            config = yaml.load(fd.read(), Loader=UnsafeLoader)
        config = expand(config)
        jobs = message_to_jobs(msg, config)
        for prio in sorted(jobs.keys()):
            job = jobs[prio]
            job['processing_priority'] = prio
            for wrk in config['workers']:
                cwrk = wrk.copy()
                cwrk.pop('fun')(job, **cwrk)
    except AbortProcessing as err:
        LOG.info(str(err))
    except Exception:
        LOG.exception("Process crashed")
