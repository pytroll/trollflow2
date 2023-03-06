#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2019 Pytroll developers
#
# Author(s):
#
#   Martin Raspaud <martin.raspaud@smhi.se>
#   Panu Lahtinen <pnuu+git@iki.fi>
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
"""Test S3 related plugins."""

import datetime as dt
from unittest import mock

import pytest  # noqa

from trollflow2.launcher import read_config
from trollflow2.tests.utils import create_filenames_and_topics

yaml_test_s3_uploader_plain = """
product_list:
  output_dir: s3://bucket-name/
  staging_zone: /tmp/
  publish_topic: /topic
  fname_pattern:
    "{start_time:%Y%m%d_%H%M}_{platform_name}_{areaname}_{productname}.{format}"

  areas:
    euro4:
      areaname: euro4
      products:
        airmass:
          productname: airmass
          formats:
          - format: tif
            writer: geotiff
        natural_with_ir:
          productname: natural_with_colorized_ir_clouds
          formats:
          - format: tif
            writer: geotiff

"""

input_mda = {'orig_platform_name': 'noaa15', 'orbit_number': 7993,
             'start_time': dt.datetime(2019, 2, 17, 6, 0, 11, 100000), 'stfrac': 1,
             'end_time': dt.datetime(2019, 2, 17, 6, 15, 10, 400000), 'etfrac': 4, 'status': 'OK',
             'format': 'CF', 'data_processing_level': '2', 'orbit': 7993, 'module': 'ppsMakePhysiography',
             'platform_name': 'NOAA-15', 'pps_version': 'v2018', 'file_was_already_processed': False,
             'dataset': [{'uri': '/home/a001673/data/satellite/test_trollflow2/S_NWC_CMA_noaa15_07993_20190217T0600111Z_20190217T0615104Z.nc',  # noqa
                          'uid': 'S_NWC_CMA_noaa15_07993_20190217T0600111Z_20190217T0615104Z.nc'},
                         {'uri': '/home/a001673/data/satellite/test_trollflow2/S_NWC_CTTH_noaa15_07993_20190217T0600111Z_20190217T0615104Z.nc',  # noqa
                          'uid': 'S_NWC_CTTH_noaa15_07993_20190217T0600111Z_20190217T0615104Z.nc'},
                         {'uri': '/home/a001673/data/satellite/test_trollflow2/S_NWC_CT_noaa15_07993_20190217T0600111Z_20190217T0615104Z.nc',  # noqa
                          'uid': 'S_NWC_CT_noaa15_07993_20190217T0600111Z_20190217T0615104Z.nc'}],
             'sensor': ['avhrr']}


def test_s3_uploader_update_filenames():
    """Ensure that filenames are updated when transfer is made to S3."""
    from yaml import UnsafeLoader

    from trollflow2.dict_tools import plist_iter
    from trollflow2.plugins.s3 import uploader

    product_list = read_config(raw_string=yaml_test_s3_uploader_plain, Loader=UnsafeLoader)
    job = {"product_list": product_list, "input_mda": input_mda.copy()}
    _ = create_filenames_and_topics(job)

    movers_mock = mock.MagicMock()
    trollmoves_mock = mock.MagicMock()
    with mock.patch.dict('sys.modules', {'trollmoves': trollmoves_mock, 'trollmoves.movers': movers_mock}):
        uploader(job)
        for fmt, _ in plist_iter(job['product_list']['product_list']):
            assert fmt['filename'].startswith('s3://bucket-name/')


def test_s3_uploader_move():
    """Test that S3 mover is moving the file."""
    from yaml import UnsafeLoader
    product_list = read_config(raw_string=yaml_test_s3_uploader_plain, Loader=UnsafeLoader)
    job = {"product_list": product_list, "input_mda": input_mda.copy()}
    _ = create_filenames_and_topics(job)

    movers_mock = mock.MagicMock()
    trollmoves_mock = mock.MagicMock()
    with mock.patch.dict('sys.modules', {'trollmoves': trollmoves_mock, 'trollmoves.movers': movers_mock}):
        from trollflow2.plugins.s3 import uploader

        uploader(job)

        assert movers_mock.S3Mover.return_value.move.call_count == 2
