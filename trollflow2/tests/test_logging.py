#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2021 Pytroll developers
#
# Author(s):
#
#   Martin Raspaud <martin.raspaud@smhi.se>
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
"""Tests for the logging utilities."""

import logging
import sys
import time
from multiprocessing import Manager
from unittest import mock

import pytest
from trollflow2.logging import logging_on, setup_queued_logging

log_queue = Manager().Queue(-1)  # no limit on size


def test_queued_logging_has_a_listener():
    """Test that the queued logging has a listener."""
    with mock.patch("trollflow2.logging.QueueListener", autospec=True) as q_listener:
        with logging_on(log_queue):
            assert q_listener.called
            assert q_listener.return_value.start.called
        assert q_listener.return_value.stop.called


def test_queued_logging_stops_listener_on_exception():
    """Test that queued logging stops the listener even if an exception occurs."""
    with mock.patch("trollflow2.logging.QueueListener", autospec=True) as q_listener:
        with pytest.raises(Exception):
            with logging_on(log_queue):
                raise Exception("Oh no!")
        assert q_listener.return_value.stop.called


LOG_CONFIG = {'version': 1,
              'formatters': {'simple': {'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s'}},
              'handlers': {'file': {'class': 'logging.handlers.BufferingHandler',
                                    'capacity': 1,
                                    'formatter': 'simple'}},
              'root': {'level': 'INFO', 'handlers': ['file']}}


def test_log_config_is_used_when_provided():
    """Test that the log config is used when provided."""
    config = LOG_CONFIG

    log = logging.getLogger()
    with mock.patch("logging.handlers.BufferingHandler.emit", autospec=True) as emit:
        with logging_on(log_queue, config=config):
            assert not emit.called
            log.warning("uh oh...")
            # we wait for the log record to go through the queue listener in
            # its own thread
            time.sleep(.01)
            assert emit.called


def test_logging_works(caplog):
    """Test that the logs get out there."""
    log = logging.getLogger("something")
    with logging_on(log_queue):
        logging.getLogger().addHandler(caplog.handler)
        log.warning("oh no :(")
        assert "oh no :(" in caplog.text


def fun(q, log_message):
    """Fake a function to run."""
    log = logging.getLogger('for fun')
    setup_queued_logging(q)
    log.debug(log_message)


def run_subprocess(log_message, queue):
    """Run a subprocess."""
    from multiprocessing import get_context
    ctx = get_context('spawn')
    proc = ctx.Process(target=fun, args=(queue, log_message))
    proc.start()
    proc.join()


@pytest.mark.skipif(sys.platform != "linux",
                    reason="Logging from a subprocess seems to work only on Linux")
def test_logging_works_in_subprocess(caplog):
    """Test that the logs get out there, even from a subprocess."""
    log_message = 'yeah, we are in a subprocess now'
    with logging_on(log_queue):
        logging.getLogger().addHandler(caplog.handler)

        run_subprocess(log_message, log_queue)
        assert log_message in caplog.text
