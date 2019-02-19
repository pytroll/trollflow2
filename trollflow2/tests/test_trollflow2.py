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
import datetime as dt


yaml_test1 = """common:
  something: foo
  min_coverage: 5.0
product_list:
  euron1:
    areaname: euron1
    min_coverage: 20.0
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
                     'min_coverage': 20.0,
                     'output_dir': '/tmp/satdmz/pps/www/latest_2018/', 'format': 'png', 'writer': 'simple_image',
                     'fname_pattern': '{platform_name:s}_{start_time:%Y%m%d_%H%M}_{areaname:s}_ctth.{format}'},
                    {'areaname': 'euron1', 'productname': 'cloud_top_height', 'fill_value': 0,
                     'min_coverage': 20.0,
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


class TestConfigValue(unittest.TestCase):

    def setUp(self):
        self.prodlist = yaml.load(yaml_test1)
        self.path = "/product_list/germ/products/cloudtype"

    def test_config_value_same_level(self):
        from trollflow2 import get_config_value
        expected = "/tmp/satdmz/pps/www/latest_2018/"
        res = get_config_value(self.prodlist, self.path, "output_dir")
        self.assertEqual(res, expected)

    def test_config_value_parent_level(self):
        from trollflow2 import get_config_value
        expected = "{start_time:%Y%m%d_%H%M}_{areaname:s}_{productname}.{format}"
        res = get_config_value(self.prodlist, self.path, "fname_pattern")
        self.assertEqual(res, expected)

    def test_config_value_common(self):
        from trollflow2 import get_config_value
        expected = "foo"
        res = get_config_value(self.prodlist, self.path, "something")
        self.assertEqual(res, expected)

    def test_config_value_missing(self):
        from trollflow2 import get_config_value
        res = get_config_value(self.prodlist, self.path, "nothing")
        self.assertIsNone(res)


class TestCreateScene(unittest.TestCase):

    @mock.patch("trollflow2.Scene")
    def test_create_scene(self, scene):
        from trollflow2 import create_scene
        scene.return_value = "foo"
        job = {"input_filenames": "bar"}
        create_scene(job)
        self.assertEqual(job["scene"], "foo")
        scene.assert_called_with(filenames='bar', reader=None)
        create_scene(job, reader="baz")
        scene.assert_called_with(filenames='bar', reader='baz')


class TestLoadComposites(unittest.TestCase):

    def setUp(self):
        self.product_list = yaml.load(yaml_test1)

    def test_load_composites(self):
        from trollflow2 import load_composites
        scn = mock.MagicMock()
        job = {"product_list": self.product_list, "scene": scn}
        load_composites(job)
        scn.load.assert_called_with({'ct', 'cloudtype', 'cloud_top_height'})


class TestResample(unittest.TestCase):

    def setUp(self):
        self.product_list = yaml.load(yaml_test1)

    def test_resample(self):
        from trollflow2 import resample
        scn = mock.MagicMock()
        scn.resample.return_value = "foo"
        job = {"scene": scn, "product_list": self.product_list}
        resample(job)
        self.assertTrue(mock.call('omerc_bb',
                                  ['ct', 'cloud_top_height'],
                                  radius_of_influence=None) in
                        scn.resample.mock_calls)
        self.assertTrue(mock.call('germ',
                                 ['cloudtype'],
                                 radius_of_influence=None) in
                        scn.resample.mock_calls)
        self.assertTrue(mock.call('euron1',
                                  ['cloud_top_height'],
                                  radius_of_influence=None) in
                        scn.resample.mock_calls)
        self.assertTrue("resampled_scenes" in job)
        for area in ["omerc_bb", "germ", "euron1"]:
            self.assertTrue(area in job["resampled_scenes"])
            self.assertTrue(job["resampled_scenes"][area] == "foo")


class TestCovers(unittest.TestCase):

    def setUp(self):
        self.product_list = yaml.load(yaml_test1)
        self.input_mda = {"platform_name": "NOAA-15",
                          "sensor": "avhrr-3",
                          "start_time": dt.datetime(2019, 1, 19, 11),
                          "end_time": dt.datetime(2019, 1, 19, 12),
                         }

    @mock.patch('trollflow2.Pass', new=None)
    def test_covers_no_trollsched(self):
        from trollflow2 import covers
        job_orig = {"foo": "bar"}
        job = job_orig.copy()
        covers(job)
        self.assertEqual(job, job_orig)

    @mock.patch('trollflow2.get_scene_coverage')
    @mock.patch('trollflow2.Pass')
    def test_covers(self, ts_pass, get_scene_coverage):
        from trollflow2 import covers
        get_scene_coverage.return_value = 10.0
        scn = mock.MagicMock()
        scn.attrs = {}
        job = {"product_list": self.product_list,
               "input_mda": self.input_mda,
               "scene": scn}
        covers(job)
        # Area "euron1" should be removed
        self.assertFalse("euron1" in job['product_list']['product_list'])
        # Other areas should stay in the list
        self.assertTrue("germ" in job['product_list']['product_list'])
        self.assertTrue("omerc_bb" in job['product_list']['product_list'])


if __name__ == '__main__':
    unittest.main()
