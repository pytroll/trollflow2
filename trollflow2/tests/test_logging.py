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
from unittest import mock

import pytest

from trollflow2.logging import (create_logged_process, logging_on,
                                queued_logging)


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
        with pytest.raises(Exception, match='Oh no!'):
            with logging_on():
                raise Exception("Oh no!")
        assert q_listener.return_value.stop.called


def test_queued_logging_process_default_config(caplog):
    """Test default config for queued logging started in a process."""
    with logging_on():
        run_subprocess(["logger_1", "logger_2"])
    assert not duplicate_lines(caplog.text)
    assert "root debug" in caplog.text
    assert "logger_1 debug" in caplog.text
    assert "logger_2 debug" in caplog.text


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
            '': {
                'level': 'WARNING',
                'handlers': ['console'],
            },
            'logger_1': {
                'level': 'DEBUG',
            },
            'logger_2': {
                'level': 'INFO',
            },
        },
    }

    with logging_on(log_config):
        run_subprocess(["logger_1", "logger_2"])

    assert "root debug" not in caplog.text
    assert "root info" not in caplog.text
    assert "logger_1 debug" in caplog.text
    assert "logger_2 debug" not in caplog.text
    assert "logger_2 info" in caplog.text


BUFFERING_LOG_CONFIG = {'version': 1,
                        'formatters': {'simple': {'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s'}},
                        'handlers': {'buffer': {'class': 'logging.handlers.BufferingHandler',
                                                'capacity': 1,
                                                'formatter': 'simple'}},
                        'root': {'level': 'INFO', 'handlers': ['buffer']}}


def test_log_config_is_used_when_provided():
    """Test that the log config is used when provided."""
    config = BUFFERING_LOG_CONFIG

    logger = logging.getLogger()
    with mock.patch("logging.handlers.BufferingHandler.emit", autospec=True) as emit:
        with logging_on(config):
            assert not emit.called
            logger.warning("uh oh...")
            # we wait for the log record to go through the queue listener in
            # its own thread
            time.sleep(.01)
            assert emit.called


def test_logging_works(caplog):
    """Test that the logs get out there."""
    logger = logging.getLogger("something")
    message = "oh no :("
    with logging_on():
        logger.warning(message)
    assert message in caplog.text


def run_subprocess(loggers):
    """Run a subprocess."""
    proc = create_logged_process(target=fun, args=(loggers,))
    proc.start()
    proc.join()


@queued_logging
def fun(loggers):
    """Fake a function to run."""
    root_logger = logging.getLogger()
    root_logger.debug("root debug")
    root_logger.info("root info")
    root_logger.warning("root warning")

    for log_name in loggers:
        logger = logging.getLogger(log_name)
        logger.debug(f"{log_name} debug")
        logger.info(f"{log_name} info")
        logger.warning(f"{log_name} warning")


def test_logging_works_in_subprocess_not_double(tmp_path):
    """Test that the logs get to a file, even from a subprocess, without duplicate lines."""
    logfile = tmp_path / "mylog"
    LOG_CONFIG_TO_FILE = {'version': 1,
                          'formatters': {'simple': {'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s'}},
                          'handlers': {'file': {'class': 'logging.FileHandler',
                                                'filename': logfile,
                                                'formatter': 'simple'}},
                          "loggers":
                              {'': {'level': 'WARNING', 'handlers': ['file']},
                               'foo1': {'level': 'DEBUG'},
                               'foo2': {'level': 'INFO'},
                               }
                          }

    with logging_on(LOG_CONFIG_TO_FILE):
        run_subprocess(["foo1", "foo2"])
    with open(logfile) as fd:
        file_contents = fd.read()

    assert not duplicate_lines(file_contents)
    assert "root debug" not in file_contents
    assert "foo1 debug" in file_contents
    assert "foo2 debug" not in file_contents
    assert "root info" not in file_contents
    assert "foo1 info" in file_contents
    assert "foo2 info" in file_contents
    assert "root warning" in file_contents
    assert "foo1 warning" in file_contents
    assert "foo2 warning" in file_contents


def duplicate_lines(contents):
    """Make sure there are no duplicate lines."""
    lines = contents.strip().split("\n")
    return len(lines) != len(set(lines))
