#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2019 Pytroll developers
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
"""Utilities for testing trollflow2."""

from unittest import mock

try:
    # Numpy doesn't like being removed from sys.modules by the patcher, so
    # import it first
    import numpy  # noqa
except ImportError:
    pass


def find_missing_modules():
    """Find the missing modules.

    PFE: http://www.voidspace.org.uk/python/mock/examples.html#mocking-imports-with-patch-dict
    """
    import_mock = mock.MagicMock()
    modules = dict()
    module_patcher = None
    while True:
        try:
            import trollflow2.plugins  # noqa
        except ImportError as err:
            print("Patching {}".format(err.name))
            if module_patcher is not None:
                module_patcher.stop()
            modules[err.name] = getattr(import_mock, err.name)
            module_patcher = mock.patch.dict("sys.modules", modules)
            module_patcher.start()
        else:
            import sys
            del sys.modules["trollflow2.plugins"]
            break
    if module_patcher is not None:
        module_patcher.stop()
    return module_patcher


module_patcher = find_missing_modules()


class TestCase:
    """Patch the missing imports."""

    def setup_method(self):
        """Set up the test case."""
        if module_patcher is not None:
            module_patcher.start()

    def teardown_method(self):
        if module_patcher is not None:
            module_patcher.stop()


def create_filenames_and_topics(job):
    """Create the filenames and topics for *job*."""
    import os.path

    from trollsift import compose

    from trollflow2.dict_tools import plist_iter

    topic_pattern = job["product_list"]["product_list"]["publish_topic"]
    topics = []

    for fmat, fmat_config in plist_iter(job["product_list"]["product_list"],
                                        job["input_mda"].copy()):
        fname_pattern = fmat["fname_pattern"]
        filename = compose(os.path.join(fmat["output_dir"],
                                        fname_pattern), fmat)
        fmat.pop("format", None)
        fmat_config["filename"] = filename
        topics.append(compose(topic_pattern, fmat))

    return topics
