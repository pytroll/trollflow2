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
        with pytest.raises(Exception, match='Oh no!'):
            with logging_on(log_queue):
                raise Exception("Oh no!")
        assert q_listener.return_value.stop.called


def test_queued_logging_process_default_config(caplog):
    """Test default config for queued logging started in a process."""
    _run_in_process()

    assert "root debug" in caplog.text
    assert "logger_1 debug" in caplog.text
    assert "logger_2 debug" in caplog.text


def _run_in_process(log_config=None):
    from multiprocessing import Manager, get_context

    from trollflow2.logging import logging_on

    log_queue = Manager().Queue()

    with logging_on(log_queue, config=log_config):
        kwargs = {'log_config': log_config}
        ctx = get_context("spawn")
        proc = ctx.Process(target=_queue_logged_process, args=(log_queue,), kwargs=kwargs)
        proc.start()
        proc.join()


def test_queued_logging_process_custom_config(caplog):
    """Test default config for queued logging started in a process."""
    log_config = {
        'version': 1,
        'handlers': {
            'console': {
                'class': 'logging.StreamHandler',
            },
        },
        'loggers': {
            'root': {
                'level': 'WARNING',
                'handlers': ['console'],
            },
            'logger_1': {
                'level': 'DEBUG',
                'handlers': ['console'],
            },
            'logger_2': {
                'level': 'INFO',
                'handlers': ['console'],
            },
        },
    }

    _run_in_process(log_config=log_config)

    assert "root debug" not in caplog.text
    assert "root info" not in caplog.text
    assert "logger_1 debug" in caplog.text
    assert "logger_2 debug" not in caplog.text
    assert "logger_2 info" in caplog.text


def _queue_logged_process(log_queue, log_config=None):
    from logging import getLogger

    from trollflow2.logging import setup_queued_logging

    setup_queued_logging(log_queue, log_config)

    root = getLogger()
    logger_1 = getLogger('logger_1')
    logger_2 = getLogger('logger_2')

    root.debug("root debug")
    root.info("root info")
    root.warning("root warning")

    logger_1.debug("logger_1 debug")
    logger_1.info("logger_1 info")
    logger_1.warning("logger_1 warning")

    logger_2.debug("logger_2 debug")
    logger_2.info("logger_2 info")
    logger_2.warning("logger_2 warning")


LOG_CONFIG = {'version': 1,
              'formatters': {'simple': {'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s'}},
              'handlers': {'file': {'class': 'logging.handlers.BufferingHandler',
                                    'capacity': 1,
                                    'formatter': 'simple'}},
              'root': {'level': 'INFO', 'handlers': ['file']}}


def test_log_config_is_used_when_provided():
    """Test that the log config is used when provided."""
    config = LOG_CONFIG

    logger = logging.getLogger()
    with mock.patch("logging.handlers.BufferingHandler.emit", autospec=True) as emit:
        with logging_on(log_queue, config=config):
            assert not emit.called
            logger.warning("uh oh...")
            # we wait for the log record to go through the queue listener in
            # its own thread
            time.sleep(.01)
            assert emit.called


def test_logging_works(caplog):
    """Test that the logs get out there."""
    logger = logging.getLogger("something")
    with logging_on(log_queue):
        logging.getLogger().addHandler(caplog.handler)
        logger.warning("oh no :(")
        assert "oh no :(" in caplog.text


def fun(q, log_message):
    """Fake a function to run."""
    logger = logging.getLogger('for fun')
    setup_queued_logging(q)
    logger.debug(log_message)


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
