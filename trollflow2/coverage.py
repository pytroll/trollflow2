import logging

from trollflow2 import AbortProcessing
from satpy.resample import get_area_def
try:
    from trollsched.satpass import Pass
except ImportError:
    Pass = None

LOG = logging.get_logger("coverage")


def coverage(config, scn_mda):
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
