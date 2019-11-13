#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2019 Pytroll developers

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

"""Trollflow2 plugins."""

import os
from contextlib import contextmanager
from logging import getLogger
from tempfile import NamedTemporaryFile
from urllib.parse import urlunsplit

import dpath
import rasterio
from posttroll.message import Message
from posttroll.publisher import NoisyPublisher
from pyorbital.astronomy import sun_zenith_angle
from pyresample.boundary import AreaDefBoundary
from rasterio.enums import Resampling
from satpy import Scene
from satpy.dataset import DatasetID
from satpy.resample import get_area_def
from satpy.writers import compute_writer_results
from trollflow2.dict_tools import get_config_value, plist_iter
from trollsift import compose

# Allow trollsched to be missing
try:
    from trollsched.satpass import Pass
    from trollsched.spherical import get_twilight_poly
except ImportError:
    Pass = None
    get_twilight_poly = None


LOG = getLogger(__name__)


class AbortProcessing(Exception):
    """Exception when processing has to be aborted."""

    pass


def create_scene(job):
    """Create a satpy scene."""
    defaults = {'reader': None,
                'reader_kwargs': None,
                'ppp_config_dir': None}
    product_list = job['product_list']
    conf = _get_plugin_conf(product_list, '/product_list', defaults)

    LOG.info('Generating scene')
    try:
        job['scene'] = Scene(filenames=job['input_filenames'], **conf)
    except ValueError as err:
        raise AbortProcessing("Failed creating scene: %s" % str(err))


def load_composites(job):
    """Load composites given in the job's product_list."""
    # composites = set().union(*(set(d.keys())
    #                            for d in dpath.util.values(job['product_list'], '/product_list/areas/*/products')))
    composites_by_res = {}
    for flat_prod_cfg, _prod_cfg in plist_iter(job['product_list']['product_list'], level='product'):
        res = flat_prod_cfg.get('resolution', None)
        if isinstance(flat_prod_cfg['product'], (tuple, list, set)):
            composites_by_res.setdefault(res, set()).update(flat_prod_cfg['product'])
        else:
            composites_by_res.setdefault(res, set()).add(flat_prod_cfg['product'])
    scn = job['scene']
    generate = job['product_list']['product_list'].get('delay_composites', True) is False
    for resolution, composites in composites_by_res.items():
        LOG.info('Loading %s at resolution %s', str(composites), str(resolution))
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
    conf = _get_plugin_conf(product_list, '/product_list', defaults)
    job['resampled_scenes'] = {}
    scn = job['scene']
    for area in product_list['product_list']['areas']:
        area_conf = _get_plugin_conf(product_list, '/product_list/areas/' + str(area),
                                     conf)
        LOG.info('Resampling to %s', str(area))
        if area is None:
            minarea = get_config_value(product_list,
                                       '/product_list/areas/' + str(area),
                                       'use_min_area')
            maxarea = get_config_value(product_list,
                                       '/product_list/areas/' + str(area),
                                       'use_max_area')
            if minarea is True:
                job['resampled_scenes'][area] = scn.resample(scn.min_area(),
                                                             **area_conf)
            elif maxarea is True:
                job['resampled_scenes'][area] = scn.resample(scn.max_area(),
                                                             **area_conf)
            else:
                # The composites need to be created for the saving to work
                if not set(scn.datasets.keys()).issuperset(scn.wishlist):
                    LOG.debug("Generating composites for 'null' area (satellite projection).")
                    scn.load(scn.wishlist, generate=True)
                job['resampled_scenes'][area] = scn
        else:
            LOG.debug("area: %s, area_conf: %s", area, str(area_conf))
            job['resampled_scenes'][area] = scn.resample(area, **area_conf)


# Datasets saving


def _prepare_filename_and_directory(fmat):
    """Compose the directory and filename (returned in that order) from *fmat*."""
    # filename composing
    fname_pattern = fmat['fname_pattern']
    directory = compose(fmat.get('output_dir', ''), fmat)
    filename = os.path.join(directory, compose(fname_pattern, fmat))

    # directory creation
    if directory and not os.path.exists(directory):
        os.makedirs(directory)

    return directory, filename


def _get_temp_filename(directory, reserved):
    """Get a unique temporary filename, avoiding names in *reserved*."""
    file_object = NamedTemporaryFile(prefix=('tmp' + str(os.getpid())), dir=directory)
    while file_object.name in reserved:
        # make sure we don't get an existing filename
        file_object.close()
        file_object = NamedTemporaryFile(prefix=('tmp' + str(os.getpid())), dir=directory)
    tmp_filename = file_object.name
    file_object.close()
    return tmp_filename


@contextmanager
def prepared_filename(fmat, renames):
    """Replace the filename with a temp filename and fill in `renames` if necessary."""
    directory, orig_filename = _prepare_filename_and_directory(fmat)

    # tmp filenaming
    use_tmp_file = fmat.get('use_tmp_file', False)

    if use_tmp_file:
        filename = _get_temp_filename(directory, renames.keys())
        yield filename
        renames[filename] = orig_filename
    else:
        yield orig_filename


def save_dataset(scns, fmat, fmat_config, renames):
    """Save one dataset to file, not doing the actual computation."""
    obj = None
    try:
        with prepared_filename(fmat, renames) as filename:
            res = fmat.get('resolution', None)
            kwargs = fmat_config.copy()
            kwargs.pop('fname_pattern', None)
            kwargs.pop('dispatch', None)
            if isinstance(fmat['product'], (tuple, list, set)):
                kwargs.pop('format')
                dsids = []
                for prod in fmat['product']:
                    dsids.append(DatasetID(name=prod, resolution=res, modifiers=None))
                obj = scns[fmat['area']].save_datasets(datasets=dsids,
                                                       filename=filename,
                                                       compute=False, **kwargs)
            else:
                dsid = DatasetID(name=fmat['product'], resolution=res, modifiers=None)
                obj = scns[fmat['area']].save_dataset(dsid,
                                                      filename=filename,
                                                      compute=False, **kwargs)
    except KeyError as err:
        LOG.info('Skipping %s: %s', fmat['product'], str(err))
    else:
        fmat_config['filename'] = renames.get(filename, filename)
    return obj


@contextmanager
def renamed_files():
    """Context renaming files."""
    renames = {}

    yield renames

    for tmp_name, actual_name in renames.items():
        os.rename(tmp_name, actual_name)


def save_datasets(job):
    """Save the datasets (and trigger the computation).

    If the `use_tmp_file` option is provided in the product list and is set to
    True, the file will be first saved to a temporary name before being renamed.
    This is useful when other processes are waiting for the file to be present
    to start their work, but would crash on incomplete files.

    """
    scns = job['resampled_scenes']
    objs = []
    base_config = job['input_mda'].copy()
    base_config.pop('dataset', None)

    with renamed_files() as renames:
        for fmat, fmat_config in plist_iter(job['product_list']['product_list'], base_config):
            obj = save_dataset(scns, fmat, fmat_config, renames)
            if obj is not None:
                objs.append(obj)
                job['produced_files'].put(fmat_config['filename'])

        compute_writer_results(objs)


class FilePublisher(object):
    """Publisher for generated files."""

    def __init__(self, port=0, nameservers=None):
        """Create new instance."""
        self.pub = None
        self.port = port
        self.nameservers = nameservers
        self.__setstate__({'port': port, 'nameservers': nameservers})

    def __setstate__(self, kwargs):
        """Set things running even when loading from YAML."""
        LOG.debug('Starting publisher')
        self.port = kwargs.get('port', 0)
        self.nameservers = kwargs.get('nameservers', None)
        self.pub = NoisyPublisher('l2processor', port=self.port,
                                  nameservers=self.nameservers)
        self.pub.start()

    @staticmethod
    def create_message(fmat, mda):
        """Create a message topic and mda."""
        topic_pattern = fmat["publish_topic"]
        file_mda = mda.copy()
        file_mda.update(fmat.get('extra_metadata', {}))

        file_mda['uri'] = os.path.abspath(fmat['filename'])

        file_mda['uid'] = os.path.basename(fmat['filename'])
        file_mda['product'] = fmat['product']
        file_mda['area'] = fmat['area']
        for key in ['productname', 'areaname', 'format']:
            try:
                file_mda[key] = fmat[key]
            except KeyError:
                pass
        for extra_info in ['area_coverage_percent', 'area_sunlight_coverage_percent']:
            try:
                file_mda[extra_info] = fmat[extra_info]
            except KeyError:
                pass

        topic = compose(topic_pattern, fmat)
        return topic, file_mda

    @staticmethod
    def create_dispatch_uri(ditem, fmat):
        """Create a uri from dispatch info."""
        path = compose(ditem['path'], fmat)
        netloc = ditem.get('hostname', '')

        return urlunsplit((ditem.get('scheme', ''), netloc, path, '', ''))

    def send_dispatch_messages(self, fmat, fmat_config, topic, file_mda):
        """Send dispatch messages corresponding to a file."""
        for dispatch_item in fmat_config.get('dispatch', []):
            mda = {
                'file_mda': file_mda,
                'source': fmat_config['filename'],
                'target': self.create_dispatch_uri(dispatch_item, fmat)
                }
            msg = Message(topic, 'dispatch', mda)
            LOG.debug('Sending dispatch order: %s', str(msg))
            self.pub.send(str(msg))

    def __call__(self, job):
        """Call the publisher."""
        mda = job['input_mda'].copy()
        mda.pop('dataset', None)
        mda.pop('collection', None)
        for fmat, fmat_config in plist_iter(job['product_list']['product_list'], mda):
            try:
                topic, file_mda = self.create_message(fmat, mda)
            except KeyError:
                LOG.debug('Could not create a message for %s.', str(fmat))
                continue
            msg = Message(topic, 'file', file_mda)
            LOG.debug('Publishing %s', str(msg))
            self.pub.send(str(msg))
            self.send_dispatch_messages(fmat, fmat_config, topic, file_mda)

    def stop(self):
        """Stop the publisher."""
        if self.pub:
            self.pub.stop()

    def __del__(self):
        """Stop the publisher when last reference is deleted."""
        self.stop()


def covers(job):
    """Check area coverage.

    Remove areas with too low coverage from the worklist.
    """
    if Pass is None:
        LOG.error("Trollsched import failed, coverage calculation not possible")
        LOG.info("Keeping all areas")
        return

    col_area = job['product_list']['product_list'].get('coverage_by_collection_area', False)
    if col_area and 'collection_area_id' in job['input_mda']:
        if job['input_mda']['collection_area_id'] not in job['product_list']['product_list']['areas']:
            raise AbortProcessing(
                "Area collection ID '%s' does not match "
                "production area(s) %s" % (
                    job['input_mda']['collection_area_id'],
                    str(list(job['product_list']['product_list']['areas']))))

    product_list = job['product_list'].copy()

    scn_mda = job['scene'].attrs.copy()
    scn_mda.update(job['input_mda'])

    platform_name = scn_mda['platform_name']
    start_time = scn_mda['start_time']
    end_time = scn_mda['end_time']
    sensor = scn_mda['sensor']
    if isinstance(sensor, (list, tuple, set)):
        sensor = list(sensor)[0]
        LOG.warning("Possibly many sensors given, taking only one for "
                    "coverage calculations: %s", sensor)

    areas = list(product_list['product_list']['areas'].keys())
    for area in areas:
        area_path = "/product_list/areas/%s" % area
        min_coverage = get_config_value(product_list,
                                        area_path,
                                        "min_coverage")
        if not min_coverage:
            LOG.debug("Minimum area coverage not given or set to zero "
                      "for area %s", area)
            continue

        cov = get_scene_coverage(platform_name, start_time, end_time,
                                 sensor, area)
        product_list['product_list']['areas'][area]['area_coverage_percent'] = cov
        if cov < min_coverage:
            LOG.info(
                "Area coverage %.2f %% below threshold %.2f %%",
                cov, min_coverage)
            LOG.info("Removing area %s from the worklist", area)
            dpath.util.delete(product_list, area_path)

        else:
            LOG.debug("Area coverage %.2f %% above threshold %.2f %% - Carry on",
                      cov, min_coverage)

    job['product_list'] = product_list


def get_scene_coverage(platform_name, start_time, end_time, sensor, area_id):
    """Get scene area coverage in percentages."""
    overpass = Pass(platform_name, start_time, end_time, instrument=sensor)
    area_def = get_area_def(area_id)

    return 100 * overpass.area_coverage(area_def)


def check_platform(job):
    """Check if the platform is valid.  If not, discard the scene."""
    mda = job['input_mda']
    product_list = job['product_list']
    conf = get_config_value(product_list, '/product_list', 'processed_platforms')
    if conf is None:
        return
    platform = mda['platform_name']
    if platform not in conf:
        raise AbortProcessing(
            "'%s' not in list of allowed platforms" % platform)


def metadata_alias(job):
    """Replace input metadata values with aliases."""
    mda_out = job['input_mda'].copy()
    product_list = job['product_list']
    aliases = get_config_value(product_list, '/product_list', 'metadata_aliases')
    if aliases is None:
        return
    for key in aliases:
        if key in mda_out:
            val = mda_out[key]
            if isinstance(val, (list, tuple, set)):
                typ = type(val)
                new_vals = typ([aliases[key].get(itm, itm) for itm in val])
                mda_out[key] = new_vals
            else:
                mda_out[key] = aliases[key].get(mda_out[key], mda_out[key])
    job['input_mda'] = mda_out.copy()


def sza_check(job):
    """Remove products which are not valid for the current Sun zenith angle."""
    scn = job['scene']
    start_time = scn.attrs['start_time']
    product_list = job['product_list']
    areas = list(product_list['product_list']['areas'].keys())
    for area in areas:
        products = list(product_list['product_list']['areas'][area]['products'].keys())
        for product in products:
            prod_path = "/product_list/areas/%s/products/%s" % (area, product)
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

        if len(product_list['product_list']['areas'][area]['products']) == 0:
            LOG.info("Removing empty area: %s", area)
            dpath.util.delete(product_list, '/product_list/areas/%s' % area)


def check_sunlight_coverage(job):
    """Remove products with too low daytime coverage.

    This plugins looks for a parameter called `min_sunlight_coverage` in the
    product list, expressed in % (so between 0 and 100). If the sunlit fraction
    is less than configured, the affected products will be discarded.
    """
    if get_twilight_poly is None:
        LOG.error("Trollsched import failed, sunlight coverage calculation not possible")
        LOG.info("Keeping all products")
        return

    scn_mda = job['scene'].attrs.copy()
    scn_mda.update(job['input_mda'])
    platform_name = scn_mda['platform_name']
    start_time = scn_mda['start_time']
    end_time = scn_mda['end_time']
    sensor = scn_mda['sensor']

    if isinstance(sensor, (list, tuple, set)):
        sensor = list(sensor)[0]
        LOG.warning("Possibly many sensors given, taking only one for "
                    "coverage calculations: %s", sensor)

    product_list = job['product_list']
    areas = list(product_list['product_list']['areas'].keys())

    for area in areas:
        products = list(product_list['product_list']['areas'][area]['products'].keys())
        for product in products:
            try:
                if isinstance(product, tuple):
                    prod = job['resampled_scenes'][area][product[0]]
                else:
                    prod = job['resampled_scenes'][area][product]
            except KeyError:
                LOG.warning("No dataset %s for this scene and area %s", product, area)
                continue
            else:
                area_def = prod.attrs['area']
            prod_path = "/product_list/areas/%s/products/%s" % (area, product)
            config = get_config_value(product_list, prod_path, "sunlight_coverage")
            if config is None:
                continue
            min_day = config.get('min')
            max_day = config.get('max')
            use_pass = config.get('check_pass', False)
            if use_pass:
                overpass = Pass(platform_name, start_time, end_time, instrument=sensor)
            else:
                overpass = None
            if min_day is None and max_day is None:
                continue
            coverage = _get_sunlight_coverage(area_def, start_time, overpass)
            product_list['product_list']['areas'][area]['area_sunlight_coverage_percent'] = coverage * 100
            if min_day is not None and coverage < (min_day / 100.0):
                LOG.info("Not enough sunlight coverage in "
                         "product '%s', removed.", product)
                dpath.util.delete(product_list, prod_path)
            if max_day is not None and coverage > (max_day / 100.0):
                LOG.info("Too much sunlight coverage in "
                         "product '%s', removed.", product)
                dpath.util.delete(product_list, prod_path)


def _get_sunlight_coverage(area_def, start_time, overpass=None):
    """Get the sunlight coverage of *area_def* at *start_time* as a value between 0 and 1."""
    adp = AreaDefBoundary(area_def, frequency=100).contour_poly
    poly = get_twilight_poly(start_time)
    if overpass is not None:
        ovp = overpass.boundary.contour_poly
        cut_area_poly = adp.intersection(ovp)
    else:
        cut_area_poly = adp

    if cut_area_poly is None:
        if not adp._is_inside(ovp):
            return 0.0
        else:
            # Should already have been taken care of in pyresample.spherical.intersection
            cut_area_poly = adp

    daylight = cut_area_poly.intersection(poly)
    if daylight is None:
        if sun_zenith_angle(start_time, *area_def.get_lonlat(0, 0)) < 90:
            return 1.0
        else:
            return 0.0
    else:
        daylight_area = daylight.area()
        total_area = adp.area()
        return daylight_area / total_area


def add_overviews(job):
    """Add overviews to images already written to disk."""
    # Get the formats, including filenames and overview settings
    for _flat_fmat, fmt in plist_iter(job['product_list']['product_list']):
        if "overviews" in fmt and 'filename' in fmt:
            fname = fmt['filename']
            overviews = fmt['overviews']
            try:
                with rasterio.open(fname, 'r+') as dst:
                    dst.build_overviews(overviews, Resampling.average)
                    dst.update_tags(ns='rio_overview',
                                    resampling='average')
                LOG.info("Added overviews to %s", fname)
            except rasterio.RasterioIOError:
                pass


def _get_plugin_conf(product_list, path, defaults):
    conf = {}
    for key in defaults:
        conf[key] = get_config_value(product_list, path, key,
                                     default=defaults.get(key))
    return conf
