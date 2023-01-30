"""Module with S3 related utilities."""

import logging

from s3fs import S3FileSystem

LOG = logging.getLogger("__name__")


def check_s3_file(remote_file):
    """Check that file saved in S3 is not empty."""
    s3 = S3FileSystem()
    if s3.stat(remote_file)['size'] == 0:
        LOG.error("Empty file detected: %s", remote_file)
        return True
    return False
