"""Module with S3 related utilities."""

import logging
import os

from s3fs import S3FileSystem

LOG = logging.getLogger("__name__")


def check_s3_file(saved_file, remote_filesystem):
    """Check that file saved in S3 is not empty."""
    s3 = S3FileSystem()
    remote_file = os.path.join(remote_filesystem, os.path.basename(saved_file))
    if s3.stat(remote_file)['size'] == 0:
        LOG.error("Empty file detected: %s", remote_file)
        return True
    return False
