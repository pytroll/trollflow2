# Copyright (c) 2019 Pytroll developers
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>
#
# Workaround for unittests so that satpy and posttroll installations
# are not necessary

"""Trollflow2 plugins."""

import collections.abc
import copy
import datetime as dt
import os
import pathlib
from contextlib import contextmanager, suppress, nullcontext
from logging import getLogger
from tempfile import NamedTemporaryFile
from urllib.parse import urlsplit, urlunsplit

with suppress(ImportError):
    import hdf5plugin  # noqa

import dask
import dask.array as da
import dpath.util
import rasterio
from dask.delayed import Delayed
from posttroll.message import Message
from posttroll.publisher import create_publisher_from_dict_config
from pyorbital.astronomy import sun_zenith_angle
from pyresample.area_config import AreaNotFound
from pyresample.boundary import AreaDefBoundary, Boundary
from pyresample.geometry import get_geostationary_bounding_box
from rasterio.enums import Resampling
from satpy import Scene
from satpy.resample import get_area_def
from satpy.version import version as satpy_version
from satpy.writers import compute_writer_results, group_results_by_output_file
from trollsift import compose

from trollflow2.dict_tools import get_config_value, plist_iter

try:
    from satpy.dataset import DataQuery
    DEFAULT = '*'
except ImportError:  # satpy <= 0.22.0
    from satpy.dataset import DatasetID as DataQuery
    DEFAULT = None

# Allow trollsched to be missing
try:
    from trollsched.satpass import Pass
    from trollsched.spherical import get_twilight_poly
except ImportError:
    Pass = None
    get_twilight_poly = None


logger = getLogger(__name__)


class AbortProcessing(Exception):
    """Exception when processing has to be aborted."""


def create_scene(job):
    """Create a satpy scene."""
    defaults = {'reader': None,
                'reader_kwargs': None}
    if satpy_version <= "0.25.1":
        defaults['ppp_config_dir'] = None
    product_list = job['product_list']
    conf = _get_plugin_conf(product_list, '/product_list', defaults)

    logger.info('Creating scene')
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
        res = flat_prod_cfg.get('resolution', DEFAULT)
        if isinstance(flat_prod_cfg['product'], (tuple, list, set)):
            composites_by_res.setdefault(res, set()).update(flat_prod_cfg['product'])
        else:
            composites_by_res.setdefault(res, set()).add(flat_prod_cfg['product'])

    logger.info(f"Loading {len(composites_by_res)} composites.")

    scn = job['scene']
    generate = job['product_list']['product_list'].get('delay_composites', True) is False
    extra_args = job["product_list"]["product_list"].get("scene_load_kwargs", {})
    for resolution, composites in composites_by_res.items():
        logger.debug('Loading %s at resolution %s', str(composites), str(resolution))
        scn.load(composites, resolution=resolution, generate=generate, **extra_args)
    job['scene'] = scn


def aggregate(job):
    """Aggregate the chosen composites."""
    if 'aggregate' not in job['product_list']['product_list']:
        return
    logger.debug("Aggregating composites.")
    kwargs = job['product_list']['product_list']['aggregate']
    job['scene'] = job['scene'].aggregate(**kwargs)


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
        logger.info('Resampling to %s', str(area))
        if area == 'None':
            coarsest = (get_config_value(product_list,
                                         '/product_list/areas/' + str(area),
                                         'use_coarsest_area') or
                        get_config_value(product_list,
                                         '/product_list/areas/' + str(area),
                                         'use_min_area'))
            finest = (get_config_value(product_list,
                                       '/product_list/areas/' + str(area),
                                       'use_finest_area') or
                      get_config_value(product_list,
                                       '/product_list/areas/' + str(area),
                                       'use_max_area'))
            native = conf.get('resampler') == 'native'
            if coarsest is True:
                job['resampled_scenes'][area] = scn.resample(scn.coarsest_area(),
                                                             **area_conf)
            elif finest is True:
                job['resampled_scenes'][area] = scn.resample(scn.finest_area(),
                                                             **area_conf)
            elif native:
                job['resampled_scenes'][area] = scn.resample(resampler='native')
            else:
                # The composites need to be created for the saving to work
                if not set(scn.keys()).issuperset(scn.wishlist):
                    logger.debug("Generating composites for 'null' area (satellite projection).")
                    scn.load(scn.wishlist, generate=True)
                job['resampled_scenes'][area] = scn
        else:
            logger.debug("area: %s, area_conf: %s", area, str(area_conf))
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
        target_scheme = urlsplit(directory).scheme
        if target_scheme in ('', 'file'):
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
    staging_zone = fmat.get("staging_zone", False)

    if staging_zone or use_tmp_file:
        if staging_zone:
            directory = staging_zone
            of = orig_filename
        if use_tmp_file:
            filename = _get_temp_filename(directory, renames.keys())
        else:
            filename = os.path.join(directory, os.path.basename(of))
        yield filename
        renames[filename] = orig_filename
    else:
        yield orig_filename


def _format_decoration_text(deco, fmat):
    """Format decoration text if it contains a key that is included in fmat."""
    if "text" in deco and "txt" in deco["text"]:
        try:
            deco["text"]["txt"] = deco["text"]["txt"].format(**fmat)
        except KeyError:
            logger.warning('Could not format: %s.', str(deco["text"]["txt"]))
    return deco


def format_decoration(fmat, fmat_config):
    """Format decoration text using template given in fmt_config with key-value pairs in fmat."""
    fmat_config_local = copy.deepcopy(fmat_config)
    if "decorate" in fmat_config:
        for deco in fmat_config_local["decorate"]["decorate"]:
            deco = _format_decoration_text(deco, fmat)
    return fmat_config_local


def save_dataset(scns, fmat, fmat_config, renames, compute=False):
    """Save one dataset to file.

    If `compute=False` the saving is delayed and done lazily.
    """
    obj = None
    try:
        with prepared_filename(fmat, renames) as filename:
            res = fmat.get('resolution', DEFAULT)
            kwargs = fmat_config.copy()
            kwargs = format_decoration(fmat, fmat_config)
            # these keyword arguments are used by the trollflow2 plugin but not
            # by satpy writers
            for name in {"fname_pattern", "dispatch", "output_dir",
                         "use_tmp_file", "staging_zone"}:
                kwargs.pop(name, None)
            if isinstance(fmat['product'], (tuple, list, set)):
                kwargs.pop('format')
                dsids = []
                for prod in fmat['product']:
                    dsids.append(_create_data_query(prod, res))
                obj = scns[fmat['area']].save_datasets(datasets=dsids,
                                                       filename=filename,
                                                       compute=compute, **kwargs)
            else:
                dsid = _create_data_query(fmat['product'], res)
                obj = scns[fmat['area']].save_dataset(dsid,
                                                      filename=filename,
                                                      compute=compute, **kwargs)
    except KeyError as err:
        logger.warning('Skipping %s: %s', fmat['product'], str(err))
    else:
        fmat_config['filename'] = renames.get(filename, filename)
    return obj


def _create_data_query(product, res):
    return DataQuery(name=product, resolution=res, modifiers=DEFAULT)


@contextmanager
def renamed_files():
    """Context renaming files."""
    renames = {}

    yield renames

    for tmp_name, actual_name in renames.items():
        target_scheme = urlsplit(actual_name).scheme
        if target_scheme in ('', 'file'):
            os.rename(tmp_name, actual_name)


def save_datasets(job):
    """Save the datasets (and trigger the computation).

    If the ``use_tmp_file`` option is provided in the product list and
    is set to True, the file will be first saved to a temporary name
    before being renamed.  This is useful when other processes are
    waiting for the file to be present to start their work, but would
    crash on incomplete files.

    If the ``staging_zone`` option is provided in the product list,
    then the file will be created in this directory first, using either a
    temporary filename (if ``use_tmp_file`` is true) or the final filename
    (if ``use_tmp_file`` is false).  This is useful for writers which
    write the filename to the headers, such as the Satpy ninjotiff and
    ninjogeotiff writers.  The ``staging_zone`` directory must be on
    the same filesystem as ``output_dir``.  When using those writers,
    it is recommended to set ``use_tmp_file`` to `False` when using a
    ``staging_zone`` directory, such that the filename written to the
    headers remains meaningful.

    The product list may contain a ``call_on_done`` parameter.
    This parameter has effect if and only if ``eager_writing`` is False
    (which is the default).  It should contain a list of references
    to callables.  Upon computation time, each callable will be
    called with four arguments: the result of ``save_dataset``,
    targets (if applicable), the full job dictionary, and the
    dictionary describing the format config and output filename
    that was written.  The parameter ``targets`` is set to None
    if using a writer where :meth:`~satpy.Scene.save_datasets`
    does not return this.  The callables must return again the
    ``save_dataset`` return value (possibly altered).  This callback
    could be used, for example, to ship products as soon as they are
    successfully produced.

    Three built-in are provided with Trollflow2: :func:`callback_close`,
    :func:`callback_move` and :func:`callback_log`.

    Other arguments defined in the job list (either directly under
    ``product_list``, or under ``formats``) are passed on to the satpy writer.  The
    arguments ``use_tmp_file``, ``staging_zone``, ``output_dir``,
    ``fname_pattern``, and ``dispatch`` are never passed to the writer.
    """
    scns = job['resampled_scenes']
    objs = []
    base_config = job['input_mda'].copy()
    base_config.pop('dataset', None)
    eager_writing = job['product_list']['product_list'].get("eager_writing", False)
    early_moving = job['product_list']['product_list'].get("early_moving", False)
    call_on_done = job["product_list"]["product_list"].get("call_on_done", None)
    if call_on_done is not None:
        callbacks = [dask.delayed(c) for c in call_on_done]
    else:
        callbacks = None
    if early_moving:
        cm = nullcontext({})
    else:
        cm = renamed_files()
    with cm as renames:
        for fmat, fmat_config in plist_iter(job['product_list']['product_list'], base_config):
            late_saver = save_dataset(scns, fmat, fmat_config, renames, compute=eager_writing)
            late_saver = _apply_callbacks(late_saver, callbacks, job, fmat_config)
            if late_saver is not None:
                objs.append(late_saver)
                job['produced_files'].put(fmat_config['filename'])
        if not eager_writing:
            compute_writer_results(objs)


def _apply_callbacks(late_saver, callbacks, *args):
    """Apply callbacks if there are any.

    If we are using callbacks via the ``call_on_done`` parameter, wrap
    ``late_saver`` with those iteratively.  If not, return ``late_saver`` as is.
    Here, ``late_saver`` is whatever :meth:`satpy.Scene.save_datasets`
    returns.
    """
    if callbacks is None:
        return late_saver
    if isinstance(late_saver, Delayed):
        return _apply_callbacks_to_delayed(late_saver, callbacks, None, *args)
    if isinstance(late_saver, collections.abc.Sequence) and len(late_saver) == 2:
        if isinstance(late_saver[0], collections.abc.Sequence):
            return _apply_callbacks_to_multiple_sources_and_targets(late_saver, callbacks, *args)
        return _apply_callbacks_to_single_source_and_target(late_saver, callbacks, *args)
    raise ValueError(
        "Unrecognised return value type from ``save_datasets``, "
        "don't know how to apply wrappers.")


def _apply_callbacks_to_delayed(delayed, callbacks, *args):
    """Recursively apply the callbacks to the delayed object.

    Args:
        delayed: dask Delayed object to which callbacks are applied
        callbacks: list of dask Delayed objects to apply
        *args: remaining arguments passed to callbacks

    Returns:
        delayed type with callbacks applied
    """
    delayed = callbacks[0](delayed, *args)
    for callback in callbacks[1:]:
        delayed = callback(delayed, *args)
    return delayed


def _apply_callbacks_to_multiple_sources_and_targets(late_saver, callbacks, *args):
    """Apply callbacks to multiple sources/targets pairs.

    Taking source/target pairs such as returned by
    :meth:`satpy.Scene.save_datasets`, split those by file and turn them all in
    delayed types by calling :func:`dask.array.store`, then apply callbacks.

    Args:
        late_saver: tuple of ``(sources, targets)`` such as may be returned
            by :meth:`satpy.Scene.save_datasets`.
        callbacks: list of dask Delayed objects to apply
        *args: remaining arguments passed to callbacks

    Returns:
        list of delayed types
    """
    delayeds = []
    for (src, targ) in group_results_by_output_file(*late_saver):
        delayed = da.store(src, targ, compute=False)
        delayeds.append(_apply_callbacks_to_delayed(delayed, callbacks, targ, *args))
    return delayeds


def _apply_callbacks_to_single_source_and_target(late_saver, callbacks, *args):
    """Apply callbacks to single source/target pairs.

    Taking a single source/target pair such as may be returned by
    :meth:`satpy.Scene.save_datasets`, turn this into a delayed type
    type by calling :func:`dask.array.store`, then apply callbacks.

    Args:
        late_saver: tuple of ``(source, target)`` such as may be returned
            by :meth:`satpy.Scene.save_datasets`.
        callbacks: list of dask Delayed objects to apply
        *args: remaining arguments passed to callbacks

    Returns:
        delayed types
    """
    (src, targ) = late_saver
    delayed = da.store(src, targ, compute=False)
    return _apply_callbacks_to_delayed(delayed, callbacks, [targ], *args)


def product_missing_from_scene(product, scene):
    """Check if product is missing from the scene."""
    if not isinstance(product, (tuple, list)):
        product = (product, )
    if all(prod not in scene for prod in product):
        return True
    return False


class FilePublisher:
    """Publisher for generated files."""

    def __init__(self, port=0, nameservers=""):
        """Create new instance."""
        self.pub = None
        self.port = port
        self.nameservers = nameservers
        self.__setstate__({'port': port, 'nameservers': nameservers})

    def __setstate__(self, kwargs):
        """Set things running even when loading from YAML."""
        logger.debug('Starting publisher')
        self.port = kwargs.get('port', 0)
        self.nameservers = kwargs.get('nameservers', "")
        self._pub_starter = create_publisher_from_dict_config(
            {
                'port': self.port,
                'nameservers': self.nameservers,
                'name': 'l2processor',
            }
        )
        self.pub = self._pub_starter.start()

    @staticmethod
    def create_message(fmat, mda):
        """Create a message topic and mda."""
        from urllib.parse import urlparse

        topic_pattern = fmat["publish_topic"]
        file_mda = mda.copy()
        file_mda.update(fmat.get('extra_metadata', {}))

        if urlparse(fmat['filename']).scheme != '':
            file_mda['uri'] = fmat['filename']
        else:
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
            logger.debug('Sending dispatch order: %s', str(msg))
            self.pub.send(str(msg))

    def __call__(self, job):
        """Call the publisher."""
        mda = job['input_mda'].copy()
        mda.pop('dataset', None)
        mda.pop('collection', None)
        for fmat, fmat_config in plist_iter(job['product_list']['product_list'], mda):
            resampled_scene = job['resampled_scenes'].get(fmat['area'], [])
            if product_missing_from_scene(fmat['product'], resampled_scene):
                logger.debug('Not publishing missing product %s.', str(fmat))
                continue
            try:
                topic, file_mda = self.create_message(fmat, mda)
            except KeyError:
                logger.debug('Could not create a message for %s.', str(fmat))
                continue
            msg = Message(topic, 'file', file_mda)
            logger.info('Publishing %s', str(msg))
            self.pub.send(str(msg))
            self.send_dispatch_messages(fmat, fmat_config, topic, file_mda)

    def stop(self):
        """Stop the publisher."""
        if self.pub:
            self.pub.stop()
            self.pub = None

    def __del__(self):
        """Stop the publisher when last reference is deleted."""
        self.stop()


def covers(job):
    """Check overall area coverage.

    Remove areas with too low coverage from the worklist.
    """
    logger.info("Checking area coverage.")
    if Pass is None:
        logger.error("Trollsched import failed, coverage calculation not possible")
        logger.debug("Keeping all areas")
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

    scn_mda = _get_scene_metadata(job)
    scn_mda.update(job['input_mda'])

    platform_name = scn_mda['platform_name']
    start_time = scn_mda['start_time']
    end_time = scn_mda['end_time']
    sensor = scn_mda['sensor']
    if isinstance(sensor, (list, tuple, set)):
        if len(sensor) > 1:
            logger.warning("Multiple sensors given, taking the first one for "
                           "coverage calculations: %s", sensor)
        sensor = list(sensor)[0]

    areas = list(product_list['product_list']['areas'].keys())
    for area in areas:
        _check_coverage_for_area(
            area, product_list, platform_name, start_time, end_time,
            sensor, job["scene"])

    job['product_list'] = product_list


def _get_scene_metadata(job):
    scn_mda = {"start_time": job['scene'].start_time,
               "end_time": job['scene'].end_time,
               "sensor": job['scene'].sensor_names}
    return scn_mda


def _check_coverage_for_area(
        area, product_list, platform_name, start_time, end_time, sensor, scene):
    """Check area coverage for single area.

    Helper for covers().  Changes product_list in-place.
    """
    area_path = "/product_list/areas/%s" % area
    min_coverage = get_config_value(product_list,
                                    area_path,
                                    "min_coverage")
    if not min_coverage:
        logger.debug("Minimum area coverage not given or set to zero "
                     "for area %s", area)
        return

    _check_overall_coverage_for_area(
        area, product_list, platform_name, start_time, end_time,
        sensor, min_coverage)


def _check_overall_coverage_for_area(
        area, product_list, platform_name, start_time, end_time, sensor,
        min_coverage):
    """Check overall coverage single area.

    Helper for covers().
    """
    area_path = "/product_list/areas/%s" % area
    cov = get_scene_coverage(platform_name, start_time, end_time,
                             sensor, area)
    product_list['product_list']['areas'][area]['area_coverage_percent'] = cov
    if cov < min_coverage:
        logger.info(
            "Area coverage %.2f %% below threshold %.2f %%",
            cov, min_coverage)
        logger.info("Removing area %s from the worklist", area)
        dpath.util.delete(product_list, area_path)

    else:
        logger.debug(f"Area coverage {cov:.2f}% above threshold "
                     f"{min_coverage:.2f}% - Carry on with {area:s}")


def get_scene_coverage(platform_name, start_time, end_time, sensor, area_id):
    """Get scene area coverage in percentages."""
    overpass = Pass(platform_name, start_time, end_time, instrument=sensor)
    area_def = get_area_def(area_id)

    try:
        return 100 * overpass.area_coverage(area_def)
    except AttributeError:
        return 100


def check_metadata(job):
    """Check the message metadata.

    If the metadata does not match the configured values, the scene
    will be discarded.

    """
    logger.info("Checking metadata.")
    mda = job['input_mda']
    product_list = job['product_list']
    conf = get_config_value(product_list, '/product_list', 'check_metadata')
    if conf is None:
        return
    for key, val in conf.items():
        if key not in mda:
            logger.warning("Metadata item '%s' not in the input message.",
                           key)
            continue
        if key == 'start_time':
            time_diff = dt.datetime.utcnow() - mda[key]
            if time_diff > abs(dt.timedelta(minutes=val)):
                age = "older" if val < 0 else "newer"
                raise AbortProcessing(
                    f"Data are {age} than the defined threshold. Skipping processing."
                )
        elif mda[key] not in val:
            raise AbortProcessing("Metadata '%s' item '%s' not in '%s'" %
                                  (key, mda[key], str(val)))


def metadata_alias(job):
    """Replace input metadata values with aliases."""
    mda_out = job['input_mda'].copy()
    product_list = job['product_list']
    aliases = get_config_value(product_list, '/product_list', 'metadata_aliases')
    if aliases is None:
        return

    logger.info("Adjusting metadata using configured aliases.")
    for key in aliases:
        if key in mda_out:
            val = mda_out[key]
            if isinstance(val, (list, tuple, set)):
                typ = type(val)
                new_vals = typ([aliases[key].get(itm, itm) for itm in val])
            else:
                new_vals = aliases[key].get(mda_out[key], mda_out[key])
            logger.debug(f"Replacing '{key}: {str(val)}' with '{str(new_vals)}'")
            mda_out[key] = new_vals
    job['input_mda'] = mda_out.copy()


def sza_check(job):
    """Remove products which are not valid for the current Sun zenith angle."""
    logger.info("Check Sun zenith angle.")
    scn_mda = _get_scene_metadata(job)
    scn_mda.update(job['input_mda'])
    start_time = scn_mda['start_time']
    product_list = job['product_list']
    areas = list(product_list['product_list']['areas'].keys())
    for area in areas:
        products = list(product_list['product_list']['areas'][area]['products'].keys())
        for product in products:
            prod_path = "/product_list/areas/%s/products/%s" % (area, product)
            lon = get_config_value(product_list, prod_path, "sunzen_check_lon")
            lat = get_config_value(product_list, prod_path, "sunzen_check_lat")
            if lon is None or lat is None:
                logger.debug("No 'sunzen_check_lon' or 'sunzen_check_lat' configured, "
                             "can\'t check Sun elevation for %s / %s",
                             area, product)
                continue

            sunzen = sun_zenith_angle(start_time, lon, lat)
            logger.debug("Sun zenith angle is %.2f degrees", sunzen)
            # Check nighttime limit
            limit = get_config_value(product_list, prod_path,
                                     "sunzen_minimum_angle")
            if limit is not None:
                if sunzen < limit:
                    logger.info("Sun zenith angle too small for nighttime "
                                "product '%s', product removed.", product)
                    dpath.util.delete(product_list, prod_path)
                continue

            # Check daytime limit
            limit = get_config_value(product_list, prod_path,
                                     "sunzen_maximum_angle")
            if limit is not None:
                if sunzen > limit:
                    logger.info("Sun zenith angle too large for daytime "
                                "product '%s', product removed.", product)
                    dpath.util.delete(product_list, prod_path)
                continue

        if len(product_list['product_list']['areas'][area]['products']) == 0:
            logger.info("Removing empty area: %s", area)
            dpath.util.delete(product_list, '/product_list/areas/%s' % area)


def check_sunlight_coverage(job):
    """Remove products with too low/high sunlight coverage.

    This plugins looks for a dictionary called `sunlight_coverage` in
    the product list, with members `min` and/or `max` that define the
    minimum and/or maximum allowed sunlight coverage within the scene.
    The limits are expressed in % (so between 0 and 100).  If the
    sunlit fraction is outside the set limits, the affected products
    will be discarded.  It is also possible to define `check_pass:
    True` in this dictionary to check the sunlit fraction within the
    overpass of an polar-orbiting satellite.

    """
    logger.info("Checking sunlight coverage.")

    if get_twilight_poly is None:
        logger.error("Trollsched import failed, sunlight coverage calculation not possible")
        logger.info("Keeping all products")
        return

    scn_mda = _get_scene_metadata(job)
    scn_mda.update(job['input_mda'])
    platform_name = scn_mda['platform_name']
    start_time = scn_mda['start_time']
    end_time = scn_mda['end_time']
    sensor = scn_mda['sensor']

    if isinstance(sensor, (list, tuple, set)):
        sensor = list(sensor)
        if len(sensor) > 1:
            logger.warning("Multiple sensors given, taking only one for "
                           "coverage calculations: %s", sensor[0])
        sensor = sensor[0]

    product_list = job['product_list']
    areas = list(product_list['product_list']['areas'].keys())

    for area in areas:
        products = list(product_list['product_list']['areas'][area]['products'].keys())
        try:
            area_def = get_area_def(area)
        except AreaNotFound:
            area_def = None
        coverage = {True: None, False: None}
        overpass = None
        for product in products:
            prod_path = "/product_list/areas/%s/products/%s" % (area, product)
            config = get_config_value(product_list, prod_path, "sunlight_coverage")
            if config is None:
                continue
            min_day = config.get('min')
            max_day = config.get('max')
            check_pass = config.get('check_pass', False)

            if min_day is None and max_day is None:
                logger.debug("Sunlight coverage not configured for %s / %s",
                             product, area)
                continue

            if area_def is None:
                area_def = _get_product_area_def(job, area, product)
                if area_def is None:
                    continue

            if check_pass and overpass is None:
                overpass = Pass(platform_name, start_time, end_time, instrument=sensor)

            if coverage[check_pass] is None:
                coverage[check_pass] = _get_sunlight_coverage(area_def,
                                                              start_time,
                                                              overpass)
            area_conf = product_list['product_list']['areas'][area]
            area_conf['area_sunlight_coverage_percent'] = coverage[check_pass] * 100
            if min_day is not None and coverage[check_pass] < (min_day / 100.0):
                logger.info("Not enough sunlight coverage for "
                            f"product '{product!s}', removed. Needs at least "
                            f"{min_day:.1f}%, got {coverage[check_pass]:.1%}.")
                dpath.util.delete(product_list, prod_path)
            if max_day is not None and coverage[check_pass] > (max_day / 100.0):
                logger.info("Too much sunlight coverage for "
                            f"product '{product!s}', removed. Needs at most "
                            f"{max_day:.1f}%, got {coverage[check_pass]:.1%}.")
                dpath.util.delete(product_list, prod_path)


def _get_sunlight_coverage(area_def, start_time, overpass=None):
    """Get the sunlight coverage of *area_def* at *start_time* as a value between 0 and 1."""
    if area_def.proj_dict.get('proj') == 'geos':
        adp = Boundary(
            *get_geostationary_bounding_box(area_def,
                                            nb_points=100)).contour_poly
    else:
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
        total_area = cut_area_poly.area()
        return daylight_area / total_area


def _get_product_area_def(job, area, product):
    """Get area definition for a product."""
    try:
        if 'resampled_scenes' in job:
            scn = job['resampled_scenes'][area]
        else:
            scn = job['scene']

        if isinstance(product, tuple):
            prod = scn[product[0]]
        else:
            prod = scn[product]
    except KeyError:
        try:
            prod = scn[list(scn.keys())[0]]
        except IndexError:
            logger.warning("No dataset %s for this scene and area %s",
                           product, area)
            return None

    return prod.attrs['area']


def add_overviews(job):
    """Add overviews to images already written to disk."""
    logger.info("Adding image overviews.")

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
                logger.debug("Added overviews to %s", fname)
            except rasterio.RasterioIOError:
                pass


def _get_plugin_conf(product_list, path, defaults):
    conf = {}
    for key in defaults:
        conf[key] = get_config_value(product_list, path, key,
                                     default=defaults.get(key))
    return conf


def check_valid_data_fraction(job):
    """Remove products that have too much invalid data.

    Remove any products where the fraction valid_data/expected_valid_data is
    less than a configured threshold in %.  Expected valid data is calculated
    by the scene coverage for each resampled scene.  This plugin was designed
    for use with AVHRR, which may alternate between channels 3A and 3B.
    Since this is different between resampled scenes, this plugin must be
    applied after scene resampling.

    This will trigger a calculation for the data to be checked.

    In theory, this selection should be possible based on metadata, which
    should contain information about channels 3A and 3B.  Unfortunately,
    experience has shown these metadata are not always reliable.

    To be configured with the ``rel_valid`` key indicating validity in %.
    For example:

        product_list:
          areas:
            fribbulus_xax:
              red:
                min_valid_data_fraction: 40

        workers:
          - fun: !!python/name:trollflow2.plugins.create_scene
          - fun: !!python/name:trollflow2.plugins.load_composites
          - fun: !!python/name:trollflow2.plugins.resample
          - fun: !!python/name:trollflow2.plugins.check_valid_data_fraction
          - fun: !!python/name:trollflow2.plugins.save_datasets

    """
    logger.info("Checking valid data fraction.")

    exp_cov = {}
    # As stated, this will trigger a computation.  To prevent computing
    # multiple times, we should persist everything that needs to be persisted,
    # all together.
    _persist_what_we_must(job)
    for (area_name, area_props) in job["product_list"]["product_list"]["areas"].items():
        to_remove = set()
        for (prod_name, prod_props) in area_props["products"].items():
            if "min_valid_data_fraction" in prod_props:
                if not _product_meets_min_valid_data_fraction(
                        prod_name, prod_props, area_name, area_props, job,
                        exp_cov):
                    to_remove.add(prod_name)
        for rem in to_remove:
            logger.debug(f"Removing {rem} due to low coverage.")
            del area_props["products"][rem]
        logger.info(f"Removed {len(to_remove)} products from area {area_name} due to low coverage.")


def _persist_what_we_must(job):
    """Persist anything that has a min_valid_data_fraction key.

    The `check_valid_data_fraction` plugin needs to calculate the products, but those should
    be calculated all at once.  This function looks for all products that have
    a `"min_valid_data_fraction"` in the product properties, persists (calculates) them all
    at once and replaces the corresponding datasets with their persisted
    versions.
    """
    to_persist = []
    for (area_name, area_props) in job["product_list"]["product_list"]["areas"].items():
        scn = job["resampled_scenes"][area_name]
        for (prod_name, prod_props) in area_props["products"].items():
            if "min_valid_data_fraction" in prod_props and prod_name in scn:
                to_persist.append((scn, prod_name, scn[prod_name]))
    logger.debug("Persisting early due to content checks")
    persisted = dask.persist(*[p[2] for p in to_persist])
    for ((sc, prod_name, _old), new) in zip(to_persist, persisted):
        sc[prod_name] = new


def _product_meets_min_valid_data_fraction(
        prod_name, prod_props, area_name, area_props, job, exp_cov):
    """Check if product meets min_valid_data_fraction.

    Helper for `check_valid_data_fraction`, check if ``product`` meets the
    ``min_valid_data_fraction`` as defined in ``prod_props``.

    Returns True if product can remain or is absent.  Returns False if product
    has to be removed.
    """
    logger.debug(f"Checking validity for {area_name:s}/{prod_name:s}")
    if prod_name not in job["resampled_scenes"][area_name]:
        logger.debug(f"product {prod_name!s} not found, already removed or loading failed?")
        return True
    prod = job["resampled_scenes"][area_name][prod_name]
    platform_name = prod.attrs["platform_name"]
    start_time = prod.attrs["start_time"]
    end_time = prod.attrs["end_time"]
    sensor = prod.attrs["sensor"]
    if area_name not in exp_cov:
        # get_scene_coverage uses %, convert to fraction
        exp_cov[area_name] = get_scene_coverage(
            platform_name, start_time, end_time, sensor, area_name)/100
    exp_valid = exp_cov[area_name]
    if exp_valid == 0:
        logger.debug(f"product {prod_name!s} no expected coverage at all, removing")
        return False
    valid = job["resampled_scenes"][area_name][prod_name].notnull()
    actual_valid = float(valid.sum()/valid.size)
    rel_valid = float(actual_valid / exp_valid)
    logger.debug(f"Expected maximum validity: {exp_valid:%}")
    logger.debug(f"Actual validity (coverage): {actual_valid:%}")
    logger.debug(f"Relative validity: {rel_valid:%}")
    min_frac = prod_props["min_valid_data_fraction"]/100
    if not 0 <= rel_valid < 1.05:
        logger.warning(f"Found {rel_valid:%} valid data, impossible... "
                       "inaccurate coverage estimate suspected!")
        return True
    if rel_valid < min_frac:
        logger.debug(f"Found {rel_valid:%}<{min_frac:%} valid data, removing "
                     f"{prod_name:s} for area {area_name:s} from the worklist")
        return False
    logger.debug(f"Found {rel_valid:%}>{min_frac:%}, keeping "
                 f"{prod_name:s} for area {area_name:s} in the worklist")
    return True


def callback_log(obj, targs, job, fmat_config):
    """Log written files as callback for save_datasets call_on_done.

    Callback function that can be used with the :func:`save_datasets`
    ``call_on_done`` functionality.  Will log a message with loglevel INFO to
    report that the filename was written successfully along with its size.

    If using :func:`callback_move` in combination with
    :func:`callback_log`, you must call :func:`callback_log` AFTER
    :func:`callback_move`, because the logger looks for the final
    destination of the file, not the temporary one.
    """
    filename = fmat_config["filename"]
    size = os.path.getsize(filename)
    logger.info(f"Wrote {filename:s} successfully, total {size:d} bytes.")
    return obj


def callback_move(obj, targs, job, fmat_config):
    """Move files as a callback by save_datasets call_on_done.

    Callback function that can be used with the :func:`save_datasets`
    ``call_on_done`` functionality.  Moves the file to the directory indicated
    with ``output_dir`` in the configuration.  This directory will be
    created if needed.

    This callback must be used with ``staging_zone`` and ``early_moving`` MUST
    be set in the configuration.  If used in combination with
    :func:`callback_log`, you must call :func:`callback_log` AFTER
    :func:`callback_move`, because the logger looks for the final destination
    of the file, not the temporary one.
    """
    destfile = pathlib.Path(fmat_config["filename"])
    srcdir = pathlib.Path(job["product_list"]["product_list"]["staging_zone"])
    srcfile = srcdir / destfile.name
    logger.debug(f"Moving {srcfile!s} to {destfile!s}")
    srcfile.rename(destfile)
    return obj


def callback_close(obj, targs, job, fmat_config):
    """Close files as a callback where needed.

    When using callbacks with writers that return a ``(src, target)`` pair for
    ``da.store``, satpy doesn't close the file until after computation is
    completed.  That means there may be data that have been computed, but not
    yet written to disk.  This is normally the case for the geotiff writer.
    For callbacks that depend on the files to be complete, the file should be
    closed first.  This callback should be prepended in this case.

    If passed a ``dask.Delayed`` object, this callback does nothing.  If passed
    a ``(src, targ)`` pair, it closes the target.
    """
    if targs:
        for targ in targs:
            targ.close()
    return obj
