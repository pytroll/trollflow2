import sys
import logging

from trollflow2.tests import test_trollflow2

if sys.version_info < (2, 7):
    import unittest2 as unittest
else:
    import unittest


def suite():
    """The global test suite.
    """
    logging.basicConfig(level=logging.DEBUG)

    mysuite = unittest.TestSuite()
    mysuite.addTests(test_trollflow2.suite())

    return mysuite


def load_tests(loader, tests, pattern):
    return suite()
