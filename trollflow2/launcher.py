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
"""The launcher module.

This delegate the actual running of the plugins to a subprocess to avoid any
memory buildup.
"""


import ast
import copy
import gc
import os
import re
import signal
import traceback
from collections import OrderedDict
from contextlib import suppress
from datetime import datetime
from logging import getLogger
from queue import Empty
from urllib.parse import urlparse

import yaml
from trollflow2.dict_tools import gen_dict_extract, plist_iter
from trollflow2.logging import setup_queued_logging
from trollflow2.plugins import AbortProcessing

try:
    from posttroll.listener import ListenerContainer
except ImportError:
    ListenerContainer = None

try:
    from yaml import UnsafeLoader, BaseLoader
except ImportError:
    from yaml import Loader as UnsafeLoader
    from yaml import BaseLoader


LOG = getLogger(__name__)
DEFAULT_PRIORITY = 999


def tuple_constructor(loader, node):
    """Construct a tuple."""
    def parse_tup_el(el):
        return ast.literal_eval(el.strip())
    value = loader.construct_scalar(node)
    tup_elements = value[1:-1].split(',')
    if tup_elements[-1] == '':
        tup_elements.pop(-1)
    tup = tuple((parse_tup_el(el) for el in tup_elements))
    return tup


tuple_regex = r'\( *([\w.]+|"[\w\s.]*") *(, *([\w.]+|"[\w\s.]*") *)*((, *([\w.]+|"[\w\s.]*") *)|(, *))\)'
yaml.add_constructor(u'!tuple', tuple_constructor, UnsafeLoader)
yaml.add_implicit_resolver(u'!tuple', re.compile(tuple_regex), None, UnsafeLoader)


def get_test_message(test_message_file):
    """Read file and retrieve the test message."""
    msg = None
    if test_message_file:
        with open(test_message_file) as fpt:
            msg = fpt.readline().strip('\n')

    return msg


def check_results(produced_files, start_time, exitcode):
    """Make sure the composites have been saved."""
    end_time = datetime.now()
    error_detected = False
    try:
        qsize = produced_files.qsize()
    except NotImplementedError:  # MacOS
        qsize = None
    while True:
        try:
            saved_file = produced_files.get(block=False)
            try:
                if os.path.getsize(saved_file) == 0:
                    LOG.error("Empty file detected: %s", saved_file)
                    error_detected = True
            except FileNotFoundError:
                LOG.error("Missing file: %s", saved_file)
                error_detected = True
        except Empty:
            break
    if exitcode != 0:
        error_detected = True
        if exitcode < 0:
            LOG.error('Process killed with signal %d', -exitcode)
        else:
            LOG.critical('Process crashed with exit code %d', exitcode)
    if not error_detected:
        elapsed = end_time - start_time
        if qsize is not None:
            LOG.info(f'All {qsize:d} files produced nominally in '
                     f"{elapsed!s}", extra={"time": elapsed})
        else:
            LOG.info(f'All files produced nominally in '
                     f"{elapsed!s}", extra={"time": elapsed})


def generate_messages(connection_parameters):
    """Generate messages using a ListenerContainer."""
    listener = _create_listener_from_connection_parameters(connection_parameters)
    while True:
        try:
            yield listener.output_queue.get(True, 5)
        except KeyboardInterrupt:
            listener.stop()
            return
        except Empty:
            continue


def _create_listener_from_connection_parameters(connection_parameters):
    """Create listener from connection parameters."""
    topics = connection_parameters['topic']
    nameserver = connection_parameters.get('nameserver', 'localhost')
    addresses = connection_parameters.get('addresses')
    listener = ListenerContainer(
        addresses=addresses,
        nameserver=nameserver,
        topics=topics)
    return listener


class Runner:
    """Class that handles all the administration around running on a product list."""

    def __init__(self, product_list, log_queue, connection_parameters=None, test_message=None, threaded=False):
        """Set up the runner."""
        self.product_list = product_list
        self.log_queue = log_queue
        self.connection_parameters = connection_parameters
        self.test_message = get_test_message(test_message)
        self.threaded = threaded

    def run(self):
        """Spawn one or multiple subprocesses or threads to run the jobs from the product list."""
        messages = self._get_message_iterator()

        if self.threaded:
            self._run_threaded(messages)
        else:
            self._run_subprocess(messages)

    def _get_message_iterator(self):
        """Get the messages to work on."""
        if self.test_message:
            from posttroll.message import Message
            messages = [Message(rawstr=self.test_message)]
            self.threaded = True
        else:
            self._fill_in_connection_parameters()
            messages = generate_messages(self.connection_parameters)
        return messages

    def _fill_in_connection_parameters(self):
        """Fill in the connection parameters for the message listener."""
        with open(self.product_list) as fid:
            config = yaml.load(fid.read(), Loader=BaseLoader)
        if self.connection_parameters is None:
            self.connection_parameters = dict()
        if not self.connection_parameters.get('topic'):
            self.connection_parameters['topic'] = config['product_list'].pop('subscribe_topics', None)

    def _run_threaded(self, messages):
        """Run in a thread."""
        LOG.info("Launching trollflow2 with threads")
        from threading import Thread
        self._run_product_list_on_messages(messages, process, Thread)

    def _run_subprocess(self, messages):
        """Run in a subprocess, with queued logging."""
        LOG.info("Launching trollflow2 with subprocesses")
        from multiprocessing import get_context
        ctx = get_context("spawn")
        self._run_product_list_on_messages(messages, queue_logged_process, ctx.Process)

    def _run_product_list_on_messages(self, messages, target_fun, process_class):
        """Run the product list on the messages."""
        for msg in messages:
            produced_files_queue = self.log_queue._manager.Queue()
            kwargs = dict(produced_files=produced_files_queue, prod_list=self.product_list)
            if not self.threaded:
                kwargs["log_queue"] = self.log_queue
            proc = process_class(target=target_fun, args=(msg,), kwargs=kwargs)
            start_time = datetime.now()
            proc.start()
            proc.join()
            try:
                exitcode = proc.exitcode
            except AttributeError:
                exitcode = 0
            check_results(produced_files_queue, start_time, exitcode)


def get_area_priorities(product_list):
    """Get processing priorities and names for areas."""
    priorities = {}
    plist = product_list['product_list']['areas']
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
    formats = product_list['product_list'].get('formats', None)
    for _product, pconfig in plist_iter(product_list['product_list'], level='product'):
        if 'formats' not in pconfig and formats is not None:
            pconfig['formats'] = copy.deepcopy(formats)
    jobs = OrderedDict()
    priorities = get_area_priorities(product_list)
    # TODO: check the uri is accessible from the current host.
    input_filenames = _extract_filenames(msg)
    for prio, areas in priorities.items():
        jobs[prio] = OrderedDict()
        jobs[prio]['input_filenames'] = input_filenames.copy()
        jobs[prio]['input_mda'] = msg.data.copy()
        jobs[prio]['product_list'] = {}
        for section in product_list:
            if section == 'product_list':
                if section not in jobs[prio]['product_list']:
                    jobs[prio]['product_list'][section] = OrderedDict(product_list[section].copy())
                    del jobs[prio]['product_list'][section]['areas']
                    jobs[prio]['product_list'][section]['areas'] = OrderedDict()
                for area in areas:
                    jobs[prio]['product_list'][section]['areas'][area] = product_list[section]['areas'][area]
            else:
                jobs[prio]['product_list'][section] = product_list[section]
    return jobs


def _extract_filenames(msg):
    """Extract the filenames from *msg*.

    If the message contains a `filesystem` item, use fsspec to decode it.
    """
    filenames = [urlparse(uri).path for uri in gen_dict_extract(msg.data, 'uri')]
    filenames = _create_fs_file_instances(filenames, msg)
    return filenames


def _create_fs_file_instances(filenames, msg):
    """Create FSFile instances when filesystem is provided."""
    filesystems = list(gen_dict_extract(msg.data, 'filesystem'))
    if filesystems:
        from satpy.readers import FSFile
        from fsspec.spec import AbstractFileSystem
        import json
        filenames = [FSFile(filename, AbstractFileSystem.from_json(json.dumps(filesystem)))
                     for filename, filesystem in zip(filenames, filesystems)]
    return filenames


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


def get_dask_client(config):
    """Create Dask client if configured."""
    client = None

    try:
        client_class = config["dask_distributed"]["class"]
        settings = config["dask_distributed"].get("settings", {})
        client = client_class(**settings)
        try:
            if not client.ncores():
                LOG.warning("No workers available, reverting to default scheduler")
                client.close()
                client = None
        except AttributeError:
            client = None
    except OSError:
        LOG.error("Scheduler not found, reverting to default scheduler")
    except KeyError:
        LOG.debug("Distributed processing not configured, "
                  "using default scheduler")
    else:
        LOG.debug(f"Using dask distributed client {client!s}")

    return client


def queue_logged_process(msg, prod_list, produced_files, log_queue):
    """Run `process` with a queued log."""
    setup_queued_logging(log_queue)
    with suppress(ValueError):
        signal.signal(signal.SIGUSR1, print_traces)
        LOG.debug("Use SIGUSR1 on pid {} to check the current tracebacks of this subprocess.".format(os.getpid()))
    process(msg, prod_list, produced_files)


def print_traces(signum, frame):
    """Print traces for debugging."""
    import sys
    import traceback
    print(f"Got signal {signum} in {frame}, dumping traces.", file=sys.stderr)

    for thread, current_frame in sys._current_frames().items():
        print('Thread 0x%x' % thread, file=sys.stderr)
        traceback.print_stack(current_frame, file=sys.stderr)
        print(file=sys.stderr)


def process(msg, prod_list, produced_files):
    """Process a message."""
    try:
        with open(prod_list) as fid:
            config = yaml.load(fid.read(), Loader=UnsafeLoader)
    except (IOError, yaml.YAMLError):
        # Either open() or yaml.load() failed
        LOG.exception("Process crashed, check YAML file.")
        raise

    # Get distributed client
    client = get_dask_client(config)

    try:
        config = expand(config)
        jobs = message_to_jobs(msg, config)
        for prio in sorted(jobs.keys()):
            job = jobs[prio]
            job['processing_priority'] = prio
            job['produced_files'] = produced_files
            try:
                for wrk in config['workers']:
                    cwrk = wrk.copy()
                    if "timeout" in cwrk:

                        def _timeout_handler(signum, frame, wrk=wrk):
                            raise TimeoutError(
                                f"Timeout for {wrk['fun']!s} expired "
                                f"after {wrk['timeout']:.1f} seconds, "
                                "giving up")
                        signal.signal(signal.SIGALRM, _timeout_handler)
                        # using setitimer because it accepts floats,
                        # unlike signal.alarm
                        signal.setitimer(signal.ITIMER_REAL,
                                         cwrk.pop("timeout"))
                    cwrk.pop('fun')(job, **cwrk)
                    if "timeout" in cwrk:
                        signal.alarm(0)  # cancel the alarm
            except AbortProcessing as err:
                LOG.warning(str(err))
    except Exception:
        LOG.exception("Process crashed")
        if "crash_handlers" in config:
            trace = traceback.format_exc()
            for hand in config['crash_handlers']['handlers']:
                hand['fun'](config['crash_handlers']['config'], trace)
        raise
    finally:
        # Remove config and run garbage collection so all remaining
        # references e.g. to FilePublisher should be removed
        LOG.debug('Cleaning up')
        for wrk in config.get("workers", []):
            try:
                wrk['fun'].stop()
            except AttributeError:
                continue
        del config
        try:
            client.close()
        except AttributeError:
            pass
        gc.collect()


def sendmail(config, trace):
    """Send email about crashes using `sendmail`."""
    from email.mime.text import MIMEText
    from subprocess import Popen, PIPE

    email_settings = config['sendmail']
    msg = MIMEText(email_settings["header"] + "\n\n" + "\n\n" + trace)
    msg["From"] = email_settings["from"]
    msg["To"] = email_settings["to"]
    msg["Subject"] = email_settings["subject"]
    sendmail = email_settings.get("sendmail", "/usr/bin/sendmail")

    pid = Popen([sendmail, "-t", "-oi"], stdin=PIPE)
    pid.communicate(msg.as_bytes())
    pid.terminate()
