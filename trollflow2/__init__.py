#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2019 Pytroll developers

# Author(s):

#   Martin Raspaud <martin.raspaud@smhi.se>
#   Panu Lahtinen <pnuu+git@iki.fi>

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>

# Workaround for unittests so that satpy and posttroll installations
# are not necessary
try:
    from satpy import Scene
    from satpy.writers import compute_writer_results
    from satpy.resample import get_area_def
    from posttroll.message import Message
    from posttroll.publisher import NoisyPublisher
except ImportError:
    Scene = None
    compute_writer_results = None
    get_area_def = None
    Message = None
    NoisyPublisher = None

try:
    from trollsched.satpass import Pass
except ImportError:
    Pass = None
from logging import getLogger
#from multiprocessing import Process
from collections import OrderedDict
from trollsift import compose
import dpath
import os

LOG = getLogger("trollflow2_plugins")


class AbortProcessing(Exception):
    pass


def create_scene(job, reader=None):
    LOG.info('Generating scene')
    job['scene'] = Scene(filenames=job['input_filenames'], reader=reader)


def load_composites(job):
    composites = set(dpath.util.values(job['product_list'], '/product_list/*/products/*/productname'))
    LOG.info('Loading %s', str(composites))
    scn = job['scene']
    scn.load(composites)
    job['scene'] = scn


def resample(job, radius_of_influence=None):
    job['resampled_scenes'] = {}
    scn = job['scene']
    product_list = job['product_list']
    for area in product_list['product_list']:
        LOG.info('Resampling to %s', str(area))
        job['resampled_scenes'][area] = scn.resample(area, radius_of_influence=radius_of_influence)


def save_datasets(job):
    scns = job['resampled_scenes']
    objs = []
    base_config = job['input_mda'].copy()
    base_config.update(job['product_list']['common'])
    base_config.pop('dataset', None)
    for fmat, fmat_config in plist_iter(job['product_list']['product_list'], base_config):
        fname_pattern = fmat['fname_pattern']
        outdir = fmat['output_dir']
        filename = compose(os.path.join(outdir, fname_pattern), fmat)
        fmat.pop('format', None)
        objs.append(scns[fmat['areaname']].save_dataset(fmat['productname'], filename=filename, compute=False, **fmat))
        fmat_config['filename'] = filename
    compute_writer_results(objs)


class FilePublisher(object):
    # todo add support for custom port and nameserver
    def __new__(cls):
        self = super().__new__(cls)
        LOG.debug('Starting publisher')
        self.pub = NoisyPublisher('l2processor')
        self.pub.start()
        return self

    def __call__(self, job):
        # create message
        # send message
        mda = job['input_mda'].copy()
        mda.pop('dataset', None)
        mda.pop('collection', None)
        topic = job['product_list']['common']['publish_topic']
        for area, config in job['product_list']['product_list'].items():
            for prod, pconfig in config['products'].items():
                for fmat in pconfig['formats']:
                    file_mda = mda.copy()
                    file_mda['uri'] = fmat['filename']
                    file_mda['uid'] = os.path.basename(fmat['filename'])
                    msg = Message(topic, 'file', file_mda)
                    LOG.debug('Publishing %s', str(msg))
                    self.pub.send(str(msg))
        self.pub.stop()


def covers(job):
    """Check area coverage. Remove areas with too low coverage from the
    worklist.
    """
    if Pass is None:
        LOG.error("Trollsched import failed, coverage calculation not possible")
        LOG.info("Keeping all areas")
        return

    product_list = job['product_list'].copy()
    scn_mda = job['input_mda'].copy()
    scn_mda.update(job['scene'].attrs)

    platform_name = scn_mda['platform_name']
    start_time = scn_mda['start_time']
    end_time = scn_mda['end_time']
    sensor = scn_mda['sensor']

    areas = list(product_list['product_list'].keys())
    for area in areas:
        area_path = "/product_list/%s" % area
        min_coverage = get_config_value(product_list,
                                        area_path,
                                        "min_coverage")
        if not min_coverage:
            LOG.debug("Minimum area coverage not given or set to zero "
                      "for area %s", area)
            continue

        cov = get_scene_coverage(platform_name, start_time, end_time,
                                 sensor, area)

        if cov < min_coverage:
            LOG.info(
                "Area coverage %.2f %% below threshold %.2f %%",
                cov, min_coverage)
            LOG.info("Removing area %s from the worklist", area)
            dpath.util.delete(product_list, area_path)

    job['product_list'] = product_list


def get_scene_coverage(platform_name, start_time, end_time, sensor, area_id):
    """Get scene area coverage in percentages"""
    overpass = Pass(platform_name, start_time, end_time, instrument=sensor)
    area_def = get_area_def(area_id)

    return 100 * overpass.area_coverage(area_def)


def plist_iter(product_list, base_mda=None, level=None):
    if base_mda is None:
        base_mda = {}
    else:
        base_mda = base_mda.copy()
    for area, area_config in product_list.items():
        aconfig = base_mda.copy()
        aconfig.update(area_config)
        aconfig.pop('products', None)
        if level == 'area':
            yield aconfig, area_config
            continue
        for prod, prod_config in area_config['products'].items():
            pconfig = aconfig.copy()
            pconfig.update(prod_config)
            pconfig.pop('formats', None)
            if level == 'product':
                yield pconfig, prod_config
                continue
            for idx, fmat_config in enumerate(prod_config['formats']):
                fconfig = pconfig.copy()
                fconfig.update(fmat_config)
                yield fconfig, fmat_config


def gen_dict_extract(var, key):
    if hasattr(var, 'items'):
        for k, v in var.items():
            if k == key:
                yield v
            if hasattr(v, 'items'):
                for result in gen_dict_extract(v, key):
                    yield result
            elif isinstance(v, list):
                for d in v:
                    for result in gen_dict_extract(d, key):
                        yield result


def get_config_value(config, path, key):
    """Get the most local config value for key *key* starting from the
    dictionary path *path*. If nothing is found, path "/common/" is
    also checked, and if still nothing is found, return None.
    """
    path_parts = path.split('/')
    # Loop starting from the current path, and continue upwards
    # towards the root until something is found
    num = len(path_parts)
    for i in range(num, 1, -1):
        pwd = "/".join(path_parts[:i] + [key])
        vals = dpath.util.values(config, pwd)
        if len(vals) > 0:
            return vals[0]

    vals = dpath.util.values(config, "/common/" + key)
    if len(vals) > 0:
        return vals[0]

    return None


from ._version import get_versions
__version__ = get_versions()['version']
del get_versions
