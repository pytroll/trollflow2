"""Trollflow2 command line interface."""

import argparse
import json
import logging

import yaml

from trollflow2.launcher import logging_on, process_files

logger = logging.getLogger(__name__)


def parse_args(args=None):
    """Parse commandline arguments."""
    parser = argparse.ArgumentParser(
        description='Launch trollflow2 processing with Satpy on the provides files then quit.')
    parser.add_argument("files", nargs='*',
                        help="Data files to run on",
                        type=str)
    parser.add_argument("-p", "--product-list",
                        help="The yaml file with the product list",
                        type=str,
                        required=True)
    parser.add_argument("-m", "--metadata",
                        help="Metadata (json) to pass on",
                        type=str, required=False, default="{}")
    parser.add_argument("-c", "--log-config",
                        help="Log config file (yaml) to use",
                        type=str, required=False, default=None)
    return parser.parse_args(args)


def cli(args=None):
    """Command line interface."""
    args = parse_args(args)

    log_config = _read_log_config(args)

    with logging_on(log_config):
        logger.info("Starting Satpy.")
        produced_files = []
        process_files(args.files, json.loads(args.metadata), args.product_list, produced_files)


def _read_log_config(args):
    """Read the config."""
    log_config = args.log_config
    if log_config is not None:
        with open(log_config) as fd:
            log_config = yaml.safe_load(fd.read())
    return log_config
