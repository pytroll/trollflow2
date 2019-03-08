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
"""

yaml_test2 = """common:
  something: foo
  min_coverage: 5.0
product_list:
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

yaml_common = """common:
  output_dir: &output_dir
    /tmp/satnfs/polar_out/pps2018/direct_readout/
  publish_topic: /NWC-CF/L3
  use_extern_calib: false
  fname_pattern: &fname
    "{platform_name}_{start_time:%Y%m%d_%H%M}_{areaname}_{productname}.{format}"
  formats: &formats
    - format: tif
      writer: geotiff
    - format: nc
      writer: cf
"""

input_mda = {'orig_platform_name': 'noaa15', 'orbit_number': 7993,
             'start_time': dt.datetime(2019, 2, 17, 6, 0, 11, 100000), 'stfrac': 1,
             'end_time': dt.datetime(2019, 2, 17, 6, 15, 10, 400000), 'etfrac': 4, 'status': 'OK',
             'format': 'CF', 'data_processing_level': '2', 'orbit': 7993, 'module': 'ppsMakePhysiography',
             'platform_name': 'NOAA-15', 'pps_version': 'v2018', 'file_was_already_processed': False,
             'dataset': [{'uri': '/home/a001673/data/satellite/test_trollflow2/S_NWC_CMA_noaa15_07993_20190217T0600111Z_20190217T0615104Z.nc',
                          'uid': 'S_NWC_CMA_noaa15_07993_20190217T0600111Z_20190217T0615104Z.nc'},
                         {'uri': '/home/a001673/data/satellite/test_trollflow2/S_NWC_CTTH_noaa15_07993_20190217T0600111Z_20190217T0615104Z.nc',
                          'uid': 'S_NWC_CTTH_noaa15_07993_20190217T0600111Z_20190217T0615104Z.nc'},
                         {'uri': '/home/a001673/data/satellite/test_trollflow2/S_NWC_CT_noaa15_07993_20190217T0600111Z_20190217T0615104Z.nc',
                          'uid': 'S_NWC_CT_noaa15_07993_20190217T0600111Z_20190217T0615104Z.nc'}],
             'sensor': ['avhrr']}


class TestProdList(unittest.TestCase):

    def test_iter(self):
        from trollflow2 import plist_iter
        prodlist = yaml.load(yaml_test1)['product_list']
        expected = [{'areaname': 'euron1_in_fname', 'area': 'euron1', 'productname': 'cloud_top_height_in_fname', 'product': 'cloud_top_height',
                     'min_coverage': 20.0,
                     'output_dir': '/tmp/satdmz/pps/www/latest_2018/', 'format': 'png', 'writer': 'simple_image',
                     'fname_pattern': '{platform_name:s}_{start_time:%Y%m%d_%H%M}_{areaname:s}_ctth_static.{format}'},
                    {'areaname': 'euron1_in_fname', 'area': 'euron1', 'productname': 'cloud_top_height_in_fname', 'product': 'cloud_top_height', 'fill_value': 0,
                     'min_coverage': 20.0,
                     'output_dir': '/tmp/satdmz/pps/www/latest_2018/', 'format': 'jpg', 'writer': 'simple_image',
                     'fname_pattern': '{platform_name:s}_{start_time:%Y%m%d_%H%M}_{areaname:s}_ctth_static.{format}'},
                    {'areaname': 'germ_in_fname', 'area': 'germ', 'productname': 'cloudtype_in_fname', 'product': 'cloudtype',
                     'output_dir': '/tmp/satdmz/pps/www/latest_2018/',
                     'fname_pattern': '{start_time:%Y%m%d_%H%M}_{areaname:s}_{productname}.{format}',
                     'format': 'png', 'writer': 'simple_image'},
                    {'areaname': 'omerc_bb', 'area': 'omerc_bb', 'productname': 'ct', 'product': 'ct',
                     'output_dir': '/tmp', 'format': 'nc', 'writer': 'cf'},
                    {'areaname': 'omerc_bb', 'area': 'omerc_bb', 'productname': 'cloud_top_height', 'product': 'cloud_top_height',
                     'output_dir': '/tmp', 'format': 'tif',
                     'writer': 'geotiff'}]
        for i, exp in zip(plist_iter(prodlist), expected):
            self.assertDictEqual(i[0], exp)
        prodlist = yaml.load(yaml_test2)['product_list']
        for i, exp in zip(plist_iter(prodlist), expected):
            self.assertDictEqual(i[0], exp)

class TestSaveDatasets(unittest.TestCase):
    @mock.patch('trollflow2.compute_writer_results')
    def test_save_datasets(self, cwr_mock):
        self.maxDiff = None
        from trollflow2 import save_datasets
        job = {}
        job['input_mda'] = input_mda
        job['product_list'] = {
            'product_list': yaml.load(yaml_test1)['product_list'],
            'common': yaml.load(yaml_common)['common'],
        }
        job['resampled_scenes'] = {}
        for area in job['product_list']['product_list']:
            job['resampled_scenes'][area] = mock.Mock()
        save_datasets(job)
        dexpected = {'euron1': {'areaname': 'euron1_in_fname',
                                'min_coverage': 20.0,
                                'products': {'cloud_top_height': {'fname_pattern': '{platform_name:s}_{start_time:%Y%m%d_%H%M}_{areaname:s}_ctth_static.{format}',
                                                                  'formats': [{'filename': '/tmp/satdmz/pps/www/latest_2018/NOAA-15_20190217_0600_euron1_in_fname_ctth_static.png',
                                                                               'format': 'png',
                                                                               'writer': 'simple_image'},
                                                                              {'filename': '/tmp/satdmz/pps/www/latest_2018/NOAA-15_20190217_0600_euron1_in_fname_ctth_static.jpg',
                                                                               'fill_value': 0,
                                                                               'format': 'jpg',
                                                                               'writer': 'simple_image'}],
                                                                  'output_dir': '/tmp/satdmz/pps/www/latest_2018/',
                                                                  'productname': 'cloud_top_height_in_fname'}}},
                     'germ': {'areaname': 'germ_in_fname',
                              'fname_pattern': '{start_time:%Y%m%d_%H%M}_{areaname:s}_{productname}.{format}',
                              'products': {'cloudtype': {'formats': [{'filename': '/tmp/satdmz/pps/www/latest_2018/20190217_0600_germ_in_fname_cloudtype_in_fname.png',
                                                                      'format': 'png',
                                                                      'writer': 'simple_image'}],
                                                         'output_dir': '/tmp/satdmz/pps/www/latest_2018/',
                                                         'productname': 'cloudtype_in_fname'}}},
                     'omerc_bb': {'areaname': 'omerc_bb',
                                  'output_dir': '/tmp',
                                  'products': {'cloud_top_height': {'formats': [{'filename': '/tmp/NOAA-15_20190217_0600_omerc_bb_cloud_top_height.tif',
                                                                                 'format': 'tif',
                                                                                 'writer': 'geotiff'}],
                                                                    'productname': 'cloud_top_height'},
                                               'ct': {'formats': [{'filename': '/tmp/NOAA-15_20190217_0600_omerc_bb_ct.nc',
                                                                   'format': 'nc',
                                                                   'writer': 'cf'}],
                                                      'productname': 'ct'}}}}
        self.assertDictEqual(job['product_list']['product_list'], dexpected)


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

    def test_config_value_missing_own_default(self):
        from trollflow2 import get_config_value
        res = get_config_value(self.prodlist, self.path, "nothing",
                               default=42)
        self.assertEqual(res, 42)


class TestCreateScene(unittest.TestCase):

    @mock.patch("trollflow2.Scene")
    def test_create_scene(self, scene):
        from trollflow2 import create_scene
        scene.return_value = "foo"
        job = {"input_filenames": "bar", "product_list": {}}
        create_scene(job)
        self.assertEqual(job["scene"], "foo")
        scene.assert_called_with(filenames='bar', reader=None,
                                 reader_kwargs=None, ppp_config_dir=None)
        job = {"input_filenames": "bar",
               "product_list": {"common": {"reader": "baz"}}}
        create_scene(job)
        scene.assert_called_with(filenames='bar', reader='baz',
                                 reader_kwargs=None, ppp_config_dir=None)


class TestLoadComposites(unittest.TestCase):

    def setUp(self):
        self.product_list = yaml.load(yaml_test1)

    def test_load_composites(self):
        from trollflow2 import load_composites
        scn = mock.MagicMock()
        job = {"product_list": self.product_list, "scene": scn}
        load_composites(job)
        scn.load.assert_called_with({'ct', 'cloudtype', 'cloud_top_height'}, resolution=None, generate=False)

    def test_load_composites_with_config(self):
        from trollflow2 import load_composites
        scn = mock.MagicMock()
        self.product_list['common']['resolution'] = 1000
        self.product_list['common']['delay_composites'] = False
        job = {"product_list": self.product_list, "scene": scn}
        load_composites(job)
        scn.load.assert_called_with({'ct', 'cloudtype', 'cloud_top_height'}, resolution=1000, generate=True)


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
                                  radius_of_influence=None,
                                  resampler="nearest",
                                  reduce_data=True,
                                  cache_dir=None,
                                  mask_area=False,
                                  epsilon=0.0) in
                        scn.resample.mock_calls)
        self.assertTrue(mock.call('germ',
                                  radius_of_influence=None,
                                  resampler="nearest",
                                  reduce_data=True,
                                  cache_dir=None,
                                  mask_area=False,
                                  epsilon=0.0) in
                        scn.resample.mock_calls)
        self.assertTrue(mock.call('euron1',
                                  radius_of_influence=None,
                                  resampler="nearest",
                                  reduce_data=True,
                                  cache_dir=None,
                                  mask_area=False,
                                  epsilon=0.0) in
                        scn.resample.mock_calls)
        self.assertTrue("resampled_scenes" in job)
        for area in ["omerc_bb", "germ", "euron1"]:
            self.assertTrue(area in job["resampled_scenes"])
            self.assertTrue(job["resampled_scenes"][area] == "foo")

        prod_list = self.product_list.copy()
        prod_list["common"] = {"resampler": "bilinear"}
        prod_list["product_list"]["euron1"]["reduce_data"] = False
        job = {"product_list": prod_list, "scene": scn}
        resample(job)
        self.assertTrue(mock.call('euron1',
                                  radius_of_influence=None,
                                  resampler="bilinear",
                                  reduce_data=False,
                                  cache_dir=None,
                                  mask_area=False,
                                  epsilon=0.0) in
                        scn.resample.mock_calls)

    def test_resample_satproj(self):
        from trollflow2 import resample
        scn = mock.MagicMock()
        scn.resample.return_value = "foo"
        job = {"scene": scn, "product_list": self.product_list.copy()}
        job['product_list']['product_list'][None] = job['product_list']['product_list']['germ']
        del job['product_list']['product_list']['germ']
        resample(job)
        self.assertTrue(mock.call('omerc_bb',
                                  radius_of_influence=None,
                                  resampler="nearest",
                                  reduce_data=True,
                                  cache_dir=None,
                                  mask_area=False,
                                  epsilon=0.0) in
                        scn.resample.mock_calls)
        self.assertTrue(mock.call('euron1',
                                  radius_of_influence=None,
                                  resampler="nearest",
                                  reduce_data=True,
                                  cache_dir=None,
                                  mask_area=False,
                                  epsilon=0.0) in
                        scn.resample.mock_calls)
        self.assertTrue(job['resampled_scenes'][None] is scn)
        self.assertTrue("resampled_scenes" in job)
        for area in ["omerc_bb", "euron1"]:
            self.assertTrue(area in job["resampled_scenes"])
            self.assertTrue(job["resampled_scenes"][area] == "foo")

    def test_minmax_area(self):
        from trollflow2 import resample
        scn = mock.MagicMock()
        scn.resample.return_value = "foo"
        product_list = self.product_list.copy()
        product_list['product_list'][None] = product_list['product_list']['germ']
        product_list['product_list'][None]['use_min_area'] = True
        del product_list['product_list']['germ']
        del product_list['product_list']['omerc_bb']
        del product_list['product_list']['euron1']
        job = {"scene": scn, "product_list": product_list.copy()}
        resample(job)
        self.assertTrue(mock.call(scn.min_area(),
                                  radius_of_influence=None,
                                  resampler="nearest",
                                  reduce_data=True,
                                  cache_dir=None,
                                  mask_area=False,
                                  epsilon=0.0) in
                        scn.resample.mock_calls)
        del product_list['product_list'][None]['use_min_area']
        product_list['product_list'][None]['use_max_area'] = True
        job = {"scene": scn, "product_list": product_list.copy()}
        resample(job)
        self.assertTrue(mock.call(scn.max_area(),
                                  radius_of_influence=None,
                                  resampler="nearest",
                                  reduce_data=True,
                                  cache_dir=None,
                                  mask_area=False,
                                  epsilon=0.0) in
                        scn.resample.mock_calls)


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

    @mock.patch('trollflow2.get_area_def')
    @mock.patch('trollflow2.Pass')
    def test_scene_coverage(self, ts_pass, get_area_def):
        from trollflow2 import get_scene_coverage
        area_coverage = mock.MagicMock()
        area_coverage.return_value = 0.2
        overpass = mock.MagicMock()
        overpass.area_coverage = area_coverage
        ts_pass.return_value = overpass
        get_area_def.return_value = 6
        res = get_scene_coverage(1, 2, 3, 4, 5)
        self.assertEqual(res, 100 * 0.2)
        ts_pass.assert_called_with(1, 2, 3, instrument=4)
        get_area_def.assert_called_with(5)
        area_coverage.assert_called_with(6)


class TestCheckPlatform(unittest.TestCase):

    @mock.patch('trollflow2.get_config_value')
    def test_check_platform(self, get_config_value):
        from trollflow2 import check_platform
        from trollflow2 import AbortProcessing
        get_config_value.return_value = None
        job = {'product_list': None, 'input_mda': {'platform_name': 'foo'}}
        self.assertIsNone(check_platform(job))
        get_config_value.return_value = ['foo', 'bar']
        self.assertIsNone(check_platform(job))
        get_config_value.return_value = ['bar']
        with self.assertRaises(AbortProcessing):
            check_platform(job)


class TestMetadataAlias(unittest.TestCase):

    def test_metadata_alias(self):
        from trollflow2 import metadata_alias
        mda = {'platform_name': 'noaa15', 'not_changed': True}
        product_list = {'common': {'not_metadata_aliases': True}}
        job = {'input_mda': mda, 'product_list': product_list}
        metadata_alias(job)
        mda = job['input_mda']
        self.assertEqual(mda['platform_name'], 'noaa15')
        self.assertTrue(mda['not_changed'])
        product_list = {'common': {'metadata_aliases':
                                   {'platform_name': {'noaa15': 'NOAA-15'},
                                    'not_in_mda': {'something': 'other'}}}}
        job = {'input_mda': mda, 'product_list': product_list}
        metadata_alias(job)
        mda = job['input_mda']
        self.assertEqual(mda['platform_name'], 'NOAA-15')
        self.assertTrue(mda['not_changed'])
        self.assertTrue('not_in_mda' not in mda)


class TestGetPluginConf(unittest.TestCase):

    def test_get_plugin_conf(self):
        from trollflow2 import _get_plugin_conf
        conf = {"common": {"val1": "foo1"},
                "product_list": {"val2": "bar2"}}
        path = "/product_list"
        defaults = {"val1": "foo0", "val2": "bar0", "val3": "baz0"}
        res = _get_plugin_conf(conf, path, defaults)
        self.assertTrue("val1" in res)
        self.assertTrue("val2" in res)
        self.assertTrue("val3" in res)
        self.assertEqual(res["val1"], "foo1")
        self.assertEqual(res["val2"], "bar2")
        self.assertEqual(res["val3"], "baz0")


class TestSZACheck(unittest.TestCase):

    @mock.patch("trollflow2.sun_zenith_angle")
    def test_sza_check(self, sun_zenith_angle):
        from trollflow2 import sza_check
        job = {}
        scene = mock.MagicMock()
        scene.attrs = {'start_time': 42}
        job['scene'] = scene
        product_list = yaml.load(yaml_test1)
        job['product_list'] = product_list.copy()
        # Run without any settings
        sza_check(job)
        sun_zenith_angle.assert_not_called()

        # Add SZA limits to couple of products
        # Day product
        product_list['product_list']['omerc_bb']['products']['ct']['sunzen_maximum_angle'] = 95.
        product_list['product_list']['omerc_bb']['products']['ct']['sunzen_check_lon'] = 25.
        product_list['product_list']['omerc_bb']['products']['ct']['sunzen_check_lat'] = 60.
        # Night product
        product_list['product_list']['germ']['products']['cloudtype']['sunzen_minimum_angle'] = 85.
        product_list['product_list']['germ']['products']['cloudtype']['sunzen_check_lon'] = 25.
        product_list['product_list']['germ']['products']['cloudtype']['sunzen_check_lat'] = 60.

        # Zenith angle that removes nothing
        sun_zenith_angle.return_value = 90.
        sza_check(job)
        sun_zenith_angle.assert_called_with(42, 25., 60.)
        self.assertDictEqual(job['product_list'], product_list)

        # Zenith angle that removes day products
        sun_zenith_angle.return_value = 100.
        sza_check(job)
        self.assertTrue('cloud_top_height' in product_list['product_list']['omerc_bb']['products'])
        self.assertFalse('ct' in product_list['product_list']['omerc_bb']['products'])

        # Zenith angle that removes night products
        sun_zenith_angle.return_value = 45.
        sza_check(job)
        # There was only one product, so the whole area is deleted
        self.assertFalse('germ' in job['product_list']['product_list'])


def suite():
    """The test suite for test_writers."""
    loader = unittest.TestLoader()
    my_suite = unittest.TestSuite()
    my_suite.addTest(loader.loadTestsFromTestCase(TestProdList))
    my_suite.addTest(loader.loadTestsFromTestCase(TestSaveDatasets))
    my_suite.addTest(loader.loadTestsFromTestCase(TestConfigValue))
    my_suite.addTest(loader.loadTestsFromTestCase(TestCreateScene))
    my_suite.addTest(loader.loadTestsFromTestCase(TestLoadComposites))
    my_suite.addTest(loader.loadTestsFromTestCase(TestResample))
    my_suite.addTest(loader.loadTestsFromTestCase(TestCovers))
    my_suite.addTest(loader.loadTestsFromTestCase(TestCheckPlatform))
    my_suite.addTest(loader.loadTestsFromTestCase(TestMetadataAlias))
    my_suite.addTest(loader.loadTestsFromTestCase(TestGetPluginConf))
    my_suite.addTest(loader.loadTestsFromTestCase(TestSZACheck))

    return my_suite


if __name__ == '__main__':
    unittest.main()
