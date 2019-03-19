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
    from pyorbital.astronomy import sun_zenith_angle
except ImportError:
    Scene = None
    compute_writer_results = None
    get_area_def = None
    Message = None
    NoisyPublisher = None
    sun_zenith_angle = None

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


def create_scene(job):
    defaults = {'reader': None,
                'reader_kwargs': None,
                'ppp_config_dir': None}
    product_list = job['product_list']
    conf = _get_plugin_conf(product_list, '/common', defaults)
    LOG.info('Generating scene')
    try:
        job['scene'] = Scene(filenames=job['input_filenames'], **conf)
    except ValueError as err:
        raise AbortProcessing("Failed creating scene: %s" % str(err))


def load_composites(job):
    """Load composites given in the job's product_list."""
    composites = set().union(*(set(d.keys())
                               for d in dpath.util.values(job['product_list'], '/product_list/*/products')))
    LOG.info('Loading %s', str(composites))
    scn = job['scene']
    resolution = job['product_list']['common'].get('resolution', None)
    generate = job['product_list']['common'].get('delay_composites', True) is False
    scn.load(composites, resolution=resolution, generate=generate)
    job['scene'] = scn


def resample(job):
    """Resample the scene to some areas."""
    defaults = {"radius_of_influence": None,
                "resampler": "nearest",
                "reduce_data": True,
                "cache_dir": None,
                "mask_area": False,
                "epsilon": 0.0}
    product_list = job['product_list']
    conf = _get_plugin_conf(product_list, '/common', defaults)
    job['resampled_scenes'] = {}
    scn = job['scene']
    for area in product_list['product_list']:
        area_conf = _get_plugin_conf(product_list, '/product_list/' + str(area),
                                     conf)
        LOG.info('Resampling to %s', str(area))
        if area is None:
            minarea = get_config_value(product_list,
                                       '/product_list/' + str(area),
                                       'use_min_area')
            maxarea = get_config_value(product_list,
                                       '/product_list/' + str(area),
                                       'use_max_area')
            if minarea is True:
                job['resampled_scenes'][area] = scn.resample(scn.min_area(),
                                                             **area_conf)
            elif maxarea is True:
                job['resampled_scenes'][area] = scn.resample(scn.max_area(),
                                                             **area_conf)
            else:
                job['resampled_scenes'][area] = scn
        else:
            job['resampled_scenes'][area] = scn.resample(area, **area_conf)


def save_datasets(job):
    """Save the datasets (and trigger the computation)."""
    scns = job['resampled_scenes']
    objs = []
    base_config = job['input_mda'].copy()
    base_config.update(job['product_list']['common'])
    base_config.pop('dataset', None)

    for fmat, fmat_config in plist_iter(job['product_list']['product_list'], base_config):
        fname_pattern = fmat['fname_pattern']
        filename = compose(os.path.join(fmat['output_dir'], fname_pattern), fmat)
        fmat.pop('format', None)
        try:
            objs.append(scns[fmat['area']].save_dataset(fmat['product'], filename=filename, compute=False, **fmat))
        except KeyError as err:
            LOG.info('Skipping %s: %s', fmat['productname'], str(err))
        else:
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
        mda = job['input_mda'].copy()
        mda.pop('dataset', None)
        mda.pop('collection', None)
        topic = job['product_list']['common']['publish_topic']
        for fmat, fmat_config in plist_iter(job['product_list']['product_list']):
            file_mda = mda.copy()
            try:
                file_mda['uri'] = fmat['filename']
            except KeyError:
                continue
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

    scn_mda = job['scene'].attrs.copy()
    scn_mda.update(job['input_mda'])

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


def check_platform(job):
    """Check if the platform is valid.  If not, discard the scene."""
    mda = job['input_mda']
    product_list = job['product_list']
    conf = get_config_value(product_list, '/common', 'processed_platforms')
    if conf is None:
        return
    platform = mda['platform_name']
    if platform not in conf:
        raise AbortProcessing(
            "'%s' not in list of allowed platforms" % platform)


def metadata_alias(job):
    """Replace input metadata values with aliases"""
    mda_out = job['input_mda'].copy()
    product_list = job['product_list']
    aliases = get_config_value(product_list, '/common', 'metadata_aliases')
    if aliases is None:
        return
    for key in aliases:
        if key in mda_out:
            mda_out[key] = aliases[key].get(mda_out[key], mda_out[key])
    job['input_mda'] = mda_out.copy()


def sza_check(job):
    """Remove products which are not valid for the current Sun zenith angle."""
    scn = job['scene']
    start_time = scn.attrs['start_time']
    product_list = job['product_list']
    areas = list(product_list['product_list'].keys())
    for area in areas:
        products = list(product_list['product_list'][area]['products'].keys())
        for product in products:
            prod_path = "/product_list/%s/products/%s" % (area, product)
            lon = get_config_value(product_list, prod_path, "sunzen_check_lon")
            lat = get_config_value(product_list, prod_path, "sunzen_check_lat")
            if lon is None or lat is None:
                LOG.debug("No 'sunzen_check_lon' or 'sunzen_check_lat' configured, "
                          "can\'t check Sun elevation for %s / %s",
                          area, product)
                continue

            sunzen = sun_zenith_angle(start_time, lon, lat)
            LOG.debug("Sun zenith angle is %.2f degrees", sunzen)
            # Check nighttime limit
            limit = get_config_value(product_list, prod_path,
                                     "sunzen_minimum_angle")
            if limit is not None:
                if sunzen < limit:
                    LOG.info("Sun zenith angle to small for nighttime "
                             "product '%s', product removed.", product)
                    dpath.util.delete(product_list, prod_path)
                continue

            # Check daytime limit
            limit = get_config_value(product_list, prod_path,
                                     "sunzen_maximum_angle")
            if limit is not None:
                if sunzen > limit:
                    LOG.info("Sun zenith angle to large for daytime "
                             "product '%s', product removed.", product)
                    dpath.util.delete(product_list, prod_path)
                continue

        if len(product_list['product_list'][area]['products']) == 0:
            LOG.info("Removing empty area: %s", area)
            dpath.util.delete(product_list, '/product_list/%s' % area)


def plist_iter(product_list, base_mda=None, level=None):
    if base_mda is None:
        base_mda = {}
    else:
        base_mda = base_mda.copy()
    for area, area_config in product_list.items():
        aconfig = base_mda.copy()
        aconfig.update(area_config)
        aconfig.pop('products', None)
        aconfig['area'] = area
        if level == 'area':
            yield aconfig, area_config
            continue
        for prod, prod_config in area_config['products'].items():
            pconfig = aconfig.copy()
            pconfig.update(prod_config)
            pconfig['product'] = prod
            if level == 'product':
                yield pconfig, prod_config
                continue
            for idx, file_config in enumerate(pconfig.get('formats', [{'format': 'tif', 'writer': 'geotiff'}])):
                fconfig = pconfig.copy()
                fconfig.pop('formats', None)
                fconfig.update(file_config)
                yield fconfig, file_config


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


def get_config_value(config, path, key, default=None):
    """Get the most local config value for key *key* starting from the
    dictionary path *path*. If nothing is found, path "/common/" is
    also checked, and if still nothing is found, return *default*.
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

    return default


def _get_plugin_conf(product_list, path, defaults):
    conf = {}
    for key in defaults:
        conf[key] = get_config_value(product_list, path, key,
                                     default=defaults.get(key))
    return conf


from ._version import get_versions
__version__ = get_versions()['version']
del get_versions
