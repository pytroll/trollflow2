import sys
import logging
import unittest

from trollflow2.tests import (test_trollflow2, test_launcher)


def suite():
    """The global test suite.
    """
    logging.basicConfig(level=logging.DEBUG)

    mysuite = unittest.TestSuite()
    mysuite.addTests(test_trollflow2.suite())
    mysuite.addTests(test_launcher.suite())

    return mysuite


def load_tests(loader, tests, pattern):
    return suite()
