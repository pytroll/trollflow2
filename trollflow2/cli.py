"""Trollflow2 command line interface."""

import argparse
import json
import logging
from datetime import datetime
from queue import Queue

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
        produced_files = Queue()
        process_files(args.files, json.loads(args.metadata, object_hook=datetime_decoder),
                      args.product_list, produced_files)


def _read_log_config(args):
    """Read the config."""
    log_config = args.log_config
    if log_config is not None:
        with open(log_config) as fd:
            log_config = yaml.safe_load(fd.read())
    return log_config


def datetime_decoder(dct):
    """Decode datetimes to python objects."""
    if isinstance(dct, list):
        pairs = enumerate(dct)
    elif isinstance(dct, dict):
        pairs = dct.items()
    result = []
    for key, val in pairs:
        if isinstance(val, str):
            try:
                val = datetime.fromisoformat(val)
            except ValueError:
                pass
        elif isinstance(val, (dict, list)):
            val = datetime_decoder(val)
        result.append((key, val))
    if isinstance(dct, list):
        return [x[1] for x in result]
    elif isinstance(dct, dict):
        return dict(result)
