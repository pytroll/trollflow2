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


from satpy import Scene
from satpy.writers import compute_writer_results
from satpy.resample import get_area_def
try:
    from trollsched.satpass import Pass
except ImportError:
    Pass = None
from logging import getLogger
#from multiprocessing import Process
from posttroll.message import Message
from posttroll.publisher import NoisyPublisher
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
    for area, config in product_list['product_list'].items():
        composites = dpath.util.values(config, '/products/*/productname')
        LOG.info('Resampling %s to %s', str(composites), str(area))
        job['resampled_scenes'][area] = scn.resample(area, composites, radius_of_influence=radius_of_influence)


def save_datasets(job):
    scns = job['resampled_scenes']
    objs = []
    for area, config in job['product_list']['product_list'].items():
        for prod, pconfig in config['products'].items():
            base_config = job['input_mda'].copy()
            base_config.update(job['product_list']['common'])
            aconfig = config.copy()
            aconfig.pop('products', None)
            base_config.update(aconfig)
            base_config.update(pconfig)
            outdir = base_config['output_dir']
            fname_pattern = base_config['fname_pattern']
            for fmat in base_config['formats']:
                base_config.update(fmat)
                filename = compose(os.path.join(outdir, fname_pattern), base_config)
                cfmat = fmat.copy()
                cfmat.pop('format', None)
                import ipdb; ipdb.set_trace()
                objs.append(scns[area].save_dataset(pconfig['productname'], filename=filename, compute=False, **cfmat))
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
        self.pub.stop()


        min_coverage = get_config_value(product_list,
                                        "/product_list/%s/" % area,
                                        "min_coverage")
        if not covers(area, scn_mda, min_coverage=min_coverage):
            continue

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
    scn_mda.update(job['scene']scn.attrs)

    platform_name = scn_mda['platform_name']
    start_time = scn_mda['start_time']
    end_time = scn_mda['end_time']
    sensor = scn_mda['sensor']

    areas = list(product_list.keys())
    for area in areas:
        area_path = "/product_list/%s/" % area
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
