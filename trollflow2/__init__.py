from satpy import Scene
from satpy.writers import compute_writer_results
from satpy.resample import get_area_def
try:
    from trollsched.satpass import Pass
except ImportError:
    Pass = None
from logging import getLogger
from posttroll.listener import ListenerContainer
from six.moves.queue import Empty as queue_empty
from six.moves.urllib.parse import urlparse
#from multiprocessing import Process
from posttroll.message import Message
from collections import OrderedDict
import dpath

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
    for area, config in job['product_list']['product_list'].items():
        composites = dpath.util.values(config, '/products/*/productname')
        LOG.info('Resampling %s to %s', str(composites), str(area))
        job['resampled_scenes'][area] = scn.resample(area, composites, radius_of_influence=radius_of_influence)


def save_datasets(job):
    scns = job['resampled_scenes']
    objs = []
    for area, config in job['product_list']['product_list'].items():
        for prod, pconfig in config['products'].items():
            for fmat in pconfig['formats']:
                cfmat = fmat.copy()
                cfmat.pop('format', None)
                objs.append(scns[area].save_dataset(pconfig['productname'], compute=False, **cfmat))
    compute_writer_results(objs)


class FilePublisher(object):
    def __init__(self):
        # initialize publisher
        pass

    def __call__(self, job):
        # create message
        # send message
        pass


def coverage(job):
    """Check area coverage"""
    if Pass is None:
        LOG.error("Trollsched import failed, coverage calculation not possible")
        return
    min_coverage = config.get('min_coverage')
    if not min_coverage:
        LOG.debug("Minimum area coverage not given or set to zero")
        return
    area = config['area']
    platform_name = scn_mda['platform_name']
    start_time = scn_mda['start_time']
    end_time = scn_mda['end_time']
    sensor = scn_mda['sensor']
    cov = get_scene_coverage()
    if cov < min_coverage:
        raise AbortProcessing(
            "Area coverage %.2f %% below threshold %.2f %" % (cov,
                                                              min_coverage))


def get_scene_coverage(platform_name, start_time, end_time, sensor, area_id):
    """Get scene area coverage in percentages"""
    overpass = Pass(platform_name, start_time, end_time, instrument=sensor)
    area_def = get_area_def(area_id)

    return 100 * overpass.area_coverage(area_def)
