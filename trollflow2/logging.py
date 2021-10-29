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

from contextlib import contextmanager
import logging
import logging.config
from logging.handlers import QueueHandler, QueueListener
from multiprocessing import Queue

DEFAULT_LOG_CONFIG = {'version': 1,
                      'disable_existing_loggers': False,
                      'formatters': {'pytroll': {'format': '[%(levelname)s: %(asctime)s : %(name)s] %(message)s',
                                                 'datefmt': '%Y-%m-%d %H:%M:%S'}},
                      'handlers': {'console': {'class': 'logging.StreamHandler',
                                               'formatter': 'pytroll'}},
                      'root': {'level': 'DEBUG', 'handlers': ['console']}}


@contextmanager
def logging_on(config=None):
    """Activate queued logging.

    This context activates logging through the use of logging's QueueHandler and
    QueueListener.
    Whether the default config parameters are used or a custom configuration is
    passed, the regular handlers are removed from the root handler after
    configuration and passed instead to the QueueListener instance, such that
    the only handler exposed to the subprocesses or threads of trollflow2 is a
    QueueHandler.
    """
    root = logging.getLogger()

    if config is None:
        config = DEFAULT_LOG_CONFIG
    logging.config.dictConfig(config)

    # Lift out the existing handlers
    handlers = root.handlers.copy()
    while root.hasHandlers():
        root.removeHandler(root.handlers[0])

    # set up queuehandler
    que = Queue(-1)  # no limit on size
    queue_handler = QueueHandler(que)

    root.addHandler(queue_handler)

    # set up and run listener
    listener = QueueListener(que, *handlers)
    listener.start()
    try:
        yield
    finally:
        listener.stop()
