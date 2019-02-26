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

yaml_test1 = """common:
  something: foo
  min_coverage: 5.0
product_list:
  euron1:
    areaname: euron1
    min_coverage: 20.0
    priority: 1
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

yaml_test_minimal = """common:
  output_dir: &output_dir
    /mnt/output/
  publish_topic: /MSG_0deg/L3
  reader: seviri_l1b_hrit
  fname_pattern:
    "{start_time:%Y%m%d_%H%M}_{platform_name}_{areaname}_{productname}.{format}"
  formats:
    - format: tif
      writer: geotiff

product_list:
  euro4:
    areaname: euro4
    products:
      overview:
        productname: overview
      airmass:
        productname: airmass
      natural_color:
        productname: natural_color
      night_fog:
        productname: night_fog

# workers:
#   - fun: !!python/name:trollflow2.create_scene
#   - fun: !!python/name:trollflow2.load_composites
#   - fun: !!python/name:trollflow2.resample
#   - fun: !!python/name:trollflow2.save_datasets
#   - fun: !!python/object:trollflow2.FilePublisher {}
"""


class TestGetAreaPriorities(unittest.TestCase):

    def test_get_area_priorities(self):
        from trollflow2.launcher import get_area_priorities
        prodlist = yaml.load(yaml_test1)

        priorities = get_area_priorities(prodlist)
        self.assertTrue(1 in priorities)
        self.assertTrue(isinstance(priorities[1], list))
        self.assertTrue('euron1' in priorities[1])
        self.assertTrue(999 in priorities)
        self.assertTrue(isinstance(priorities[999], list))
        self.assertTrue('omerc_bb' in priorities[999])
        self.assertTrue('germ' in priorities[999])


class TestMessageToJobs(unittest.TestCase):

    def test_message_to_jobs(self):
        from trollflow2.launcher import message_to_jobs
        prodlist = yaml.load(yaml_test1)
        msg = mock.MagicMock()
        msg.data = {'uri': 'foo'}

        jobs = message_to_jobs(msg, prodlist)
        self.assertEqual(set(jobs.keys()), {1, 999})
        for i in jobs.keys():
            self.assertEqual(set(jobs[i].keys()),
                             {'input_filenames', 'input_mda', 'product_list'})
            self.assertEqual(jobs[i]['input_filenames'], ['foo'])
            self.assertEqual(jobs[i]['input_mda'], msg.data)
            self.assertEqual(set(jobs[i]['product_list'].keys()),
                             {'common', 'product_list'})
        self.assertEqual(set(jobs[1]['product_list']['product_list'].keys()),
                         set(['euron1']))
        self.assertEqual(set(jobs[999]['product_list']['product_list'].keys()),
                         set(['germ', 'omerc_bb']))

        prodlist['product_list']['germ']['priority'] = None
        jobs = message_to_jobs(msg, prodlist)
        self.assertTrue('germ' in jobs[999]['product_list']['product_list'])

    def test_message_to_jobs_minimal(self):
        from trollflow2.launcher import message_to_jobs
        prodlist = yaml.load(yaml_test_minimal)
        msg = mock.MagicMock()
        msg.data = {'uri': 'foo'}
        jobs = message_to_jobs(msg, prodlist)

        expected = dict([('euro4',
                          {'areaname': 'euro4',
                           'products': {'airmass': {'formats': [{'format': 'tif',
                                                                 'writer': 'geotiff'}],
                                                    'productname': 'airmass'},
                                        'natural_color': {'formats': [{'format': 'tif',
                                                                       'writer': 'geotiff'}],
                                                          'productname': 'natural_color'},
                                        'night_fog': {'formats': [{'format': 'tif',
                                                                   'writer': 'geotiff'}],
                                                      'productname': 'night_fog'},
                                        'overview': {'formats': [{'format': 'tif',
                                                                  'writer': 'geotiff'}],
                                                     'productname': 'overview'}}})])
        self.assertDictEqual(jobs[999]['product_list']['product_list'], expected)

class TestRun(unittest.TestCase):

    @mock.patch('trollflow2.launcher.process')
    @mock.patch('trollflow2.launcher.time.sleep')
    @mock.patch('trollflow2.launcher.Process')
    @mock.patch('trollflow2.launcher.ListenerContainer')
    def test_run(self, lc_, Process, sleep, process):
        from trollflow2.launcher import run
        listener = mock.MagicMock()
        listener.output_queue.get.return_value = 'foo'
        lc_.return_value = listener
        proc_ret = mock.MagicMock()
        Process.return_value = proc_ret
        # stop looping
        sleep.side_effect = KeyboardInterrupt
        prod_list = 'bar'
        topics = 'baz'
        try:
            run(topics, prod_list)
        except KeyboardInterrupt:
            pass
        listener.output_queue.called_once()
        Process.assert_called_with(args=('foo', prod_list), target=process)
        proc_ret.start.assert_called_once()
        proc_ret.join.assert_called_once()
        sleep.called_once_with(5)

    @mock.patch('trollflow2.launcher.ListenerContainer')
    def test_run_keyboard_interrupt(self, lc_):
        from trollflow2.launcher import run
        listener = mock.MagicMock()
        get = mock.Mock()
        get.side_effect = KeyboardInterrupt
        listener.output_queue.get = get
        lc_.return_value = listener
        run(0, 1)
        listener.stop.assert_called_once()


class TestExpand(unittest.TestCase):
    def test_expand(self):
        from trollflow2.launcher import expand
        inside = {'a': 'b'}
        outside = {'c': inside, 'd': inside}
        expanded = expand(outside)
        self.assertIsNot(expanded['d'], expanded['c'])


class TestProcess(unittest.TestCase):

    @mock.patch('trollflow2.launcher.expand')
    @mock.patch('trollflow2.launcher.yaml')
    @mock.patch('trollflow2.launcher.message_to_jobs')
    @mock.patch('trollflow2.launcher.open')
    def test_process(self, open_, message_to_jobs, yaml, expand):
        from trollflow2.launcher import process
        fid = mock.MagicMock()
        fid.read.return_value = yaml_test1
        open_.return_value.__enter__.return_value = fid
        yaml.load.return_value = "foo"
        fun1 = mock.MagicMock()
        # Return something resembling a config
        expand.return_value = {"workers": [{"fun": fun1}]}

        message_to_jobs.return_value = {1: {"job1": dict([])}}
        process("msg", "prod_list")
        open_.assert_called_with("prod_list")
        yaml.load.assert_called_once()
        message_to_jobs.assert_called_with("msg", {"workers": [{"fun": fun1}]})
        fun1.assert_called_with({'job1': {}, 'processing_priority': 1})
        # Test that errors are propagated
        yaml.load.side_effect = KeyboardInterrupt
        with self.assertRaises(KeyboardInterrupt):
            process("msg", "prod_list")


def suite():
    """The test suite for test_writers."""
    loader = unittest.TestLoader()
    my_suite = unittest.TestSuite()
    my_suite.addTest(loader.loadTestsFromTestCase(TestGetAreaPriorities))
    my_suite.addTest(loader.loadTestsFromTestCase(TestMessageToJobs))
    my_suite.addTest(loader.loadTestsFromTestCase(TestRun))
    my_suite.addTest(loader.loadTestsFromTestCase(TestProcess))
    my_suite.addTest(loader.loadTestsFromTestCase(TestExpand))

    return my_suite


if __name__ == '__main__':
    unittest.main()
