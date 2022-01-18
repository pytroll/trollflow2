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
"""Test the product list tools."""

import unittest
try:
    from unittest import mock
except ImportError:
    import mock  # noqa
import datetime as dt
try:
    # Numpy doesn't like being removed from sys.modules by the patcher, so
    # import it first
    import numpy  # noqa
except ImportError:
    pass

from trollflow2.launcher import read_config


yaml_test1 = """
product_list:
  something: foo
  min_coverage: 5.0
  areas:
      euron1:
        areaname: euron1_in_fname
        min_coverage: 20.0
        products:
          cloud_top_height:
            productname: cloud_top_height_in_fname
            output_dir: /tmp/satdmz/pps/www/latest_2018/
            formats:
              - format: png
                writer: simple_image
              - format: jpg
                writer: simple_image
                fill_value: 0
            fname_pattern: "{platform_name:s}_{start_time:%Y%m%d_%H%M}_{areaname:s}_ctth_static.{format}"

      germ:
        areaname: germ_in_fname
        fname_pattern: "{start_time:%Y%m%d_%H%M}_{areaname:s}_{productname}.{format}"
        products:
          cloudtype:
            productname: cloudtype_in_fname
            output_dir: /tmp/satdmz/pps/www/latest_2018/
            formats:
              - format: png
                writer: simple_image

      omerc_bb:
        areaname: omerc_bb
        output_dir: /tmp
        products:
          ct:
            productname: ct
            formats:
              - format: nc
                writer: cf
          cloud_top_height:
            productname: cloud_top_height
            formats:
              - format: tif
                writer: geotiff

      null:
        areaname: null_in_fname
        fname_pattern: "{start_time:%Y%m%d_%H%M}_{areaname:s}_{productname}.{format}"
        products:
          cloudtype:
            productname: cloudtype_in_fname
            output_dir: /tmp/satdmz/pps/www/latest_2018/
            formats:
              - format: png
                writer: simple_image

"""

yaml_test2 = """
product_list:
  something: foo
  min_coverage: 5.0
  areas:
      euron1:
        areaname: euron1_in_fname
        min_coverage: 20.0
        products:
          cloud_top_height:
            productname: cloud_top_height_in_fname
            output_dir: /tmp/satdmz/pps/www/latest_2018/
            formats:
              - format: png
                writer: simple_image
              - format: jpg
                writer: simple_image
                fill_value: 0
            fname_pattern: "{platform_name:s}_{start_time:%Y%m%d_%H%M}_{areaname:s}_ctth_static.{format}"

      germ:
        areaname: germ_in_fname
        fname_pattern: "{start_time:%Y%m%d_%H%M}_{areaname:s}_{productname}.{format}"
        formats:
          - format: png
            writer: simple_image
        products:
          cloudtype:
            productname: cloudtype_in_fname
            output_dir: /tmp/satdmz/pps/www/latest_2018/

      omerc_bb:
        areaname: omerc_bb
        output_dir: /tmp
        products:
          ct:
            productname: ct
            formats:
              - format: nc
                writer: cf
          cloud_top_height:
            productname: cloud_top_height
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


class TestProdList(unittest.TestCase):
    """Test case for the product list handling."""

    def test_iter(self):
        """Test plist_iter."""
        from trollflow2.dict_tools import plist_iter
        prodlist = read_config(raw_string=yaml_test1)['product_list']
        expected = [{'areaname': 'euron1_in_fname', 'area': 'euron1', 'productname': 'cloud_top_height_in_fname', 'product': 'cloud_top_height',  # noqa
                     'min_coverage': 20.0, 'something': 'foo',
                     'output_dir': '/tmp/satdmz/pps/www/latest_2018/', 'format': 'png', 'writer': 'simple_image',
                     'fname_pattern': '{platform_name:s}_{start_time:%Y%m%d_%H%M}_{areaname:s}_ctth_static.{format}'},
                    {'areaname': 'euron1_in_fname', 'area': 'euron1', 'productname': 'cloud_top_height_in_fname', 'product': 'cloud_top_height', 'fill_value': 0,  # noqa
                     'min_coverage': 20.0, 'something': 'foo',
                     'output_dir': '/tmp/satdmz/pps/www/latest_2018/', 'format': 'jpg', 'writer': 'simple_image',
                     'fname_pattern': '{platform_name:s}_{start_time:%Y%m%d_%H%M}_{areaname:s}_ctth_static.{format}'},
                    {'areaname': 'germ_in_fname', 'area': 'germ', 'productname': 'cloudtype_in_fname', 'product': 'cloudtype',  # noqa
                     'output_dir': '/tmp/satdmz/pps/www/latest_2018/', 'min_coverage': 5.0, 'something': 'foo',
                     'fname_pattern': '{start_time:%Y%m%d_%H%M}_{areaname:s}_{productname}.{format}',
                     'format': 'png', 'writer': 'simple_image'},
                    {'areaname': 'omerc_bb', 'area': 'omerc_bb', 'productname': 'ct', 'product': 'ct', 'min_coverage': 5.0, 'something': 'foo',  # noqa
                     'output_dir': '/tmp', 'format': 'nc', 'writer': 'cf'},
                    {'areaname': 'omerc_bb', 'area': 'omerc_bb', 'productname': 'cloud_top_height', 'product': 'cloud_top_height',  # noqa
                     'output_dir': '/tmp', 'format': 'tif', 'min_coverage': 5.0, 'something': 'foo',
                     'writer': 'geotiff'}]
        for i, exp in zip(plist_iter(prodlist), expected):
            self.assertDictEqual(i[0], exp)

        prodlist = read_config(raw_string=yaml_test2)['product_list']
        for i, exp in zip(plist_iter(prodlist), expected):
            self.assertDictEqual(i[0], exp)


class TestConfigValue(unittest.TestCase):
    """Test case for get_config_value."""

    def setUp(self):
        """Set up the test case."""
        self.prodlist = read_config(raw_string=yaml_test1)
        self.path = "/product_list/areas/germ/products/cloudtype"

    def test_config_value_same_level(self):
        """Test the config value at the same level."""
        from trollflow2.dict_tools import get_config_value
        expected = "/tmp/satdmz/pps/www/latest_2018/"
        res = get_config_value(self.prodlist, self.path, "output_dir")
        self.assertEqual(res, expected)

    def test_config_value_parent_level(self):
        """Test getting a config value from the parent level."""
        from trollflow2.dict_tools import get_config_value
        expected = "{start_time:%Y%m%d_%H%M}_{areaname:s}_{productname}.{format}"
        res = get_config_value(self.prodlist, self.path, "fname_pattern")
        self.assertEqual(res, expected)

    def test_config_value_common(self):
        """Test getting a common config value."""
        from trollflow2.dict_tools import get_config_value
        expected = "foo"
        res = get_config_value(self.prodlist, self.path, "something")
        self.assertEqual(res, expected)

    def test_config_value_missing(self):
        """Test getting a missing config value."""
        from trollflow2.dict_tools import get_config_value
        res = get_config_value(self.prodlist, self.path, "nothing")
        self.assertIsNone(res)

    def test_config_value_missing_own_default(self):
        """Test getting a missing value with default."""
        from trollflow2.dict_tools import get_config_value
        res = get_config_value(self.prodlist, self.path, "nothing",
                               default=42)
        self.assertEqual(res, 42)

    def test_null_area(self):
        from trollflow2.dict_tools import get_config_value
        path = "/product_list/areas/None/products/cloudtype"
        expected = "/tmp/satdmz/pps/www/latest_2018/"
        res = get_config_value(self.prodlist, path, "output_dir")
        self.assertEqual(res, expected)


if __name__ == '__main__':
    unittest.main()
