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
import time
from logging.handlers import QueueHandler, BufferingHandler
from unittest import mock

import pytest

from trollflow2.logging import logging_on


def teardown_function():
    """Clean up the handlers after execution."""
    root = logging.getLogger()
    while root.hasHandlers():
        root.removeHandler(root.handlers[0])


def test_logging_adds_one_queue_handlers(caplog):
    """Test that logging adds a queue handler."""
    log = logging.getLogger()
    with logging_on():
        log.warning('bla')
        assert len(log.handlers) == 1
        assert isinstance(log.handlers[0], QueueHandler)
    assert "bla" in caplog.text


def test_logging_is_queued_by_default():
    """Test that logging is queued by default."""
    log = logging.getLogger()
    num_queued_handlers_before = count_queue_handlers(log)
    with logging_on():
        num_queued_handlers = count_queue_handlers(log)
        assert num_queued_handlers > num_queued_handlers_before


def count_queue_handlers(log):
    """Count the number of queue handlers in the log handlers."""
    num_queued_handlers = 0
    for handler in log.handlers:
        if isinstance(handler, logging.handlers.QueueHandler):
            num_queued_handlers += 1
    return num_queued_handlers


def test_queued_logging_has_a_listener():
    """Test that the queued logging has a listener."""
    with mock.patch("trollflow2.logging.QueueListener", autospec=True) as q_listener:
        with logging_on():
            assert q_listener.called
            assert q_listener.return_value.start.called
        assert q_listener.return_value.stop.called


def test_queued_logging_stops_listener_on_exception():
    """Test that queued logging stops the listener even if an exception occurs."""
    with mock.patch("trollflow2.logging.QueueListener", autospec=True) as q_listener:
        with pytest.raises(Exception):
            with logging_on():
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
        with logging_on(config=config):
            assert not emit.called
            log.warning("uh oh...")
            # we wait for the log record to go through the queue listener in
            # its own thread
            time.sleep(.01)
            assert emit.called


def test_log_config_still_uses_queuehandler():
    """Test that the log config still uses the queuehandler."""
    config = LOG_CONFIG

    log = logging.getLogger()
    with logging_on(config=config):
        for handler in log.handlers:
            assert not isinstance(handler, BufferingHandler)
