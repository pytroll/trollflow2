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

import unittest
import yaml
try:
    from unittest import mock
except ImportError:
    import mock


yaml_test1 = """common:
  something: foo
product_list:
  euron1:
    areaname: euron1
    products:
      ctth:
        productname: cloud_top_height
        output_dir: /tmp/satdmz/pps/www/latest_2018/
        formats:
          - format: png
            writer: simple_image
          - format: jpg
            writer: simple_image
            fill_value: 0
        fname_pattern: "{platform_name:s}_{start_time:%Y%m%d_%H%M}_{areaname:s}_ctth.{format}"

  germ:
    areaname: germ
    fname_pattern: "{start_time:%Y%m%d_%H%M}_{areaname:s}_{productname}.{format}"
    products:
      cloudtype:
        productname: cloudtype
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
"""


class TestProdList(unittest.TestCase):

    def test_iter(self):
        from trollflow2 import plist_iter
        prodlist = yaml.load(yaml_test1)['product_list']
        expected = [{'areaname': 'euron1', 'productname': 'cloud_top_height',
                     'output_dir': '/tmp/satdmz/pps/www/latest_2018/', 'format': 'png', 'writer': 'simple_image',
                     'fname_pattern': '{platform_name:s}_{start_time:%Y%m%d_%H%M}_{areaname:s}_ctth.{format}'},
                    {'areaname': 'euron1', 'productname': 'cloud_top_height', 'fill_value': 0,
                     'output_dir': '/tmp/satdmz/pps/www/latest_2018/', 'format': 'jpg', 'writer': 'simple_image',
                     'fname_pattern': '{platform_name:s}_{start_time:%Y%m%d_%H%M}_{areaname:s}_ctth.{format}'},
                    {'areaname': 'germ', 'productname': 'cloudtype', 'output_dir': '/tmp/satdmz/pps/www/latest_2018/',
                     'fname_pattern': '{start_time:%Y%m%d_%H%M}_{areaname:s}_{productname}.{format}',
                     'format': 'png', 'writer': 'simple_image'},
                    {'areaname': 'omerc_bb', 'productname': 'ct', 'output_dir': '/tmp', 'format': 'nc', 'writer': 'cf'},
                    {'areaname': 'omerc_bb', 'productname': 'cloud_top_height', 'output_dir': '/tmp', 'format': 'tif',
                     'writer': 'geotiff'}]
        for i, exp in zip(plist_iter(prodlist), expected):
            self.assertDictEqual(i, exp)


class TestSaveDatasets(unittest.TestCase):
    @mock.patch('trollflow2.compute_writer_results')
    def test_save_datasets(self, cwr_mock):
        pass


if __name__ == '__main__':
    unittest.main()
