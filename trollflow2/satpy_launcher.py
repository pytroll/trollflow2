#!/usr/bin/env python

from satpy import Scene
from satpy.utils import debug_on
from satpy.writers import compute_writer_results
from logging import getLogger
from posttroll.listener import ListenerContainer
from six.moves.queue import Empty as queue_empty
from six.moves.urllib.parse import urlparse
import sys
#from multiprocessing import Process
from threading import Thread as Process  # to get ipdb to work
import yaml
import time
from trollmoves.utils import gen_dict_extract
from posttroll.message import Message
from collections import OrderedDict
import dpath

"""The order of basic things is:
- Create the scene
- Generate the composites
- Resample
- Save to file
"""


LOG = getLogger(__name__)

def main2():

    # start listener

    # convert message and product list to an order.

    # how does an order look like ?

    # set of files to process, set of composites and areas to generate, set of files to write

    #

    pass

def run(topics, prod_list):

    #listener = ListenerContainer(topics=topics)

    while True:
        try:
            #msg = listener.output_queue.get(True, 5)
            msg = '''pytroll://AAPP-HRPT/1B file safusr.u@lxserv1887.smhi.se 2019-02-14T12:18:20.248209 v1.01 application/json {"sensor": "avhrr/3", "uid": "hrpt_metop01_20190214_1206_33256.l1b", "format": "AAPP-HRPT", "variant": "DR", "start_time": "2019-02-14T12:06:15", "orbit_number": 33256, "uri": "file:///san1/polar_in/direct_readout/hrpt/lvl1/hrpt_metop01_20190214_1206_33256.l1b", "platform_name": "Metop-B", "end_time": "2019-02-14T12:18:09", "type": "Binary", "data_processing_level": "1B", "origin": "172.29.1.74:9098"}'''
            msg = '''pytroll://collection/CF/2/CT/ dataset safusr.u@lxserv1887.smhi.se 2019-02-15T12:03:09.779329 v1.01 application/json {"orig_platform_name": "metopb", "orbit_number": 33270, "start_time": "2019-02-15T11:45:35.900000", "stfrac": 9, "end_time": "2019-02-15T11:58:11.500000", "etfrac": 5, "status": "OK", "format": "CF", "data_processing_level": "2", "orbit": 33270, "module": "ppsMakePhysiography", "platform_name": "Metop-B", "pps_version": "v2018", "file_was_already_processed":false, "dataset": [{"uri": "/data/proj/safutv/polar_out/pps2018/direct_readout/S_NWC_CMA_metopb_33270_20190215T1145359Z_20190215T1158115Z.nc", "uid": "S_NWC_CMA_metopb_33270_20190215T1145359Z_20190215T1158115Z.nc"}, {"uri": "/data/proj/safutv/polar_out/pps2018/direct_readout/S_NWC_CTTH_metopb_33270_20190215T1145359Z_20190215T1158115Z.nc", "uid": "S_NWC_CTTH_metopb_33270_20190215T1145359Z_20190215T1158115Z.nc"}, {"uri": "/data/proj/safutv/polar_out/pps2018/direct_readout/S_NWC_CT_metopb_33270_20190215T1145359Z_20190215T1158115Z.nc", "uid": "S_NWC_CT_metopb_33270_20190215T1145359Z_20190215T1158115Z.nc"}], "sensor": ["avhrr"]}'''
            msg = '''pytroll://collection/CF/2/CT/ dataset safusr.u@lxserv1887.smhi.se 2019-02-17T06:20:30.062917 v1.01 application/json {"orig_platform_name": "noaa15", "orbit_number": 7993, "start_time": "2019-02-17T06:00:11.100000", "stfrac": 1, "end_time": "2019-02-17T06:15:10.400000", "etfrac": 4, "status": "OK", "format": "CF", "data_processing_level": "2", "orbit": 7993, "module": "ppsMakePhysiography", "platform_name": "NOAA-15", "pps_version": "v2018", "file_was_already_processed":false, "dataset": [{"uri": "/data/proj/safutv/polar_out/pps2018/direct_readout/S_NWC_CMA_noaa15_07993_20190217T0600111Z_20190217T0615104Z.nc", "uid": "S_NWC_CMA_noaa15_07993_20190217T0600111Z_20190217T0615104Z.nc"}, {"uri": "/data/proj/safutv/polar_out/pps2018/direct_readout/S_NWC_CTTH_noaa15_07993_20190217T0600111Z_20190217T0615104Z.nc", "uid": "S_NWC_CTTH_noaa15_07993_20190217T0600111Z_20190217T0615104Z.nc"}, {"uri": "/data/proj/safutv/polar_out/pps2018/direct_readout/S_NWC_CT_noaa15_07993_20190217T0600111Z_20190217T0615104Z.nc", "uid": "S_NWC_CT_noaa15_07993_20190217T0600111Z_20190217T0615104Z.nc"}], "sensor": ["avhrr"]}'''
            msg = Message(rawstr=msg)
        except KeyboardInterrupt:
            listener.stop()
            return
        except queue_empty:
            continue

        proc = Process(target=process, args=(msg, prod_list))
        proc.start()
        proc.join()
        time.sleep(5)


def message_to_job(msg, product_list):
    job = OrderedDict()
    job['input_filenames'] = list(gen_dict_extract(msg.data, 'uri'))
    job['product_list'] = product_list
    job['input_mda'] = msg.data.copy()

    return job


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

def process(msg, prod_list):
    with open(prod_list) as fd:
        config = yaml.load(fd.read())
    job = message_to_job(msg, config)
    for wrk in config['workers']:
        cwrk = wrk.copy()
        cwrk.pop('fun')(job, **cwrk)



def main():
    debug_on()
    topics = sys.argv[1].split(',')
    prod_list = sys.argv[2]
    run(topics, prod_list)


if __name__ == "__main__":
    main()
