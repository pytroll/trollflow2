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


from logging import getLogger
from posttroll.listener import ListenerContainer
from six.moves.queue import Empty as queue_empty
import sys
#from multiprocessing import Process
from threading import Thread as Process  # to get ipdb to work
import yaml
import time
from trollmoves.utils import gen_dict_extract
from collections import OrderedDict
from posttroll.message import Message

"""The order of basic things is:
- Create the scene
- Generate the composites
- Resample
- Save to file
"""


LOG = getLogger(__name__)

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


def process(msg, prod_list):
    with open(prod_list) as fd:
        config = yaml.load(fd.read())
    job = message_to_job(msg, config)
    for wrk in config['workers']:
        cwrk = wrk.copy()
        cwrk.pop('fun')(job, **cwrk)


def main():
    from satpy.utils import debug_on
    debug_on()
    topics = sys.argv[1].split(',')
    prod_list = sys.argv[2]
    run(topics, prod_list)


if __name__ == "__main__":
    main()
