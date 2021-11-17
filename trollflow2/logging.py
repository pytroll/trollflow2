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
"""Logging utilities."""

import logging
import logging.config
from contextlib import contextmanager
from logging import getLogger, DEBUG
from logging.handlers import QueueListener, QueueHandler

DEFAULT_LOG_CONFIG = {'version': 1,
                      'disable_existing_loggers': False,
                      'formatters': {'pytroll': {'format': '[%(levelname)s: %(asctime)s : %(name)s] %(message)s',
                                                 'datefmt': '%Y-%m-%d %H:%M:%S'}},
                      'handlers': {'console': {'class': 'logging.StreamHandler',
                                               'formatter': 'pytroll'}},
                      'root': {'level': 'DEBUG', 'handlers': ['console']}}


@contextmanager
def logging_on(log_queue, config=None):
    """Activate queued logging.

    This context activates logging through the use of logging's QueueHandler and
    QueueListener.
    Whether the default config parameters are used or a custom configuration is
    passed, the log handlers are passed to a QueueListener instance, such that
    the subprocesses of trollflow2 can use a QueueHandler to communicate logs.
    """
    root = logging.getLogger()
    # Lift out the existing handlers (we need to keep these for pytest's caplog)
    handlers = root.handlers.copy()

    if config is None:
        config = DEFAULT_LOG_CONFIG
    logging.config.dictConfig(config)

    # set up and run listener
    listener = QueueListener(log_queue, *(root.handlers + handlers))
    listener.start()
    try:
        yield
    finally:
        listener.stop()


def setup_queued_logging(log_queue):
    """Set up queued logging."""
    root_logger = getLogger()
    root_logger.addHandler(QueueHandler(log_queue))
    root_logger.setLevel(DEBUG)
