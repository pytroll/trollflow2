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
"""Test the launcher module."""

import unittest

import yaml

try:
    from yaml import UnsafeLoader
except ImportError:
    from yaml import Loader as UnsafeLoader
from unittest import mock
from trollflow2.tests.utils import TestCase

yaml_test1 = """
product_list:
  something: foo
  min_coverage: 5.0
  subscribe_topics:
    - /topic1
    - /topic2
  areas:
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

yaml_test_minimal = """
product_list:
  output_dir: &output_dir
    /mnt/output/
  publish_topic: /MSG_0deg/L3
  reader: seviri_l1b_hrit
  fname_pattern:
    "{start_time:%Y%m%d_%H%M}_{platform_name}_{areaname}_{productname}.{format}"
  formats:
    - format: tif
      writer: geotiff
  areas:
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


class TestGetAreaPriorities(TestCase):
    """Test case for area priorities."""

    def test_get_area_priorities(self):
        """Test getting the area priorities."""
        from trollflow2.launcher import get_area_priorities
        prodlist = yaml.load(yaml_test1, Loader=UnsafeLoader)

        priorities = get_area_priorities(prodlist)
        self.assertTrue(1 in priorities)
        self.assertTrue(isinstance(priorities[1], list))
        self.assertTrue('euron1' in priorities[1])
        self.assertTrue(999 in priorities)
        self.assertTrue(isinstance(priorities[999], list))
        self.assertTrue('omerc_bb' in priorities[999])
        self.assertTrue('germ' in priorities[999])


class TestMessageToJobs(TestCase):
    """Test case for converting a message to jobs."""

    def test_message_to_jobs(self):
        """Test converting message to jobs."""
        from trollflow2.launcher import message_to_jobs
        prodlist = yaml.load(yaml_test1, Loader=UnsafeLoader)
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
                             {'product_list'})
        self.assertEqual(set(jobs[1]['product_list']['product_list']['areas'].keys()),
                         set(['euron1']))
        self.assertEqual(set(jobs[999]['product_list']['product_list']['areas'].keys()),
                         set(['germ', 'omerc_bb']))

        prodlist['product_list']['areas']['germ']['priority'] = None
        jobs = message_to_jobs(msg, prodlist)
        self.assertTrue('germ' in jobs[999]['product_list']['product_list']['areas'])

    def test_message_to_jobs_minimal(self):
        """Test converting a message to minimal jobs."""
        from trollflow2.launcher import message_to_jobs
        prodlist = yaml.load(yaml_test_minimal, Loader=UnsafeLoader)
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
        self.assertDictEqual(jobs[999]['product_list']['product_list']['areas'], expected)
        self.assertIn('output_dir', jobs[999]['product_list']['product_list'])
        # Test that the formats are not the same object
        prods = jobs[999]['product_list']['product_list']['areas']['euro4']['products']
        self.assertFalse(prods['overview']['formats'][0] is
                         prods['natural_color']['formats'][0])
        prods['overview']['formats'][0]['foo'] = 'bar'
        self.assertFalse('foo' in prods['natural_color']['formats'][0])

    def test_message_to_jobs_fsspec(self):
        """Test transforming a message containing filesystem specification."""
        with mock.patch.dict('sys.modules', {'fsspec': mock.MagicMock(),
                                             'fsspec.spec': mock.MagicMock(),
                                             'satpy': mock.MagicMock(),
                                             'satpy.readers': mock.MagicMock(),
                                             'satpy.resample': mock.MagicMock(),
                                             'satpy.writers': mock.MagicMock(),
                                             'satpy.dataset': mock.MagicMock(),
                                             'satpy.version': mock.MagicMock()}):
            from fsspec.spec import AbstractFileSystem as abs_fs
            from satpy.readers import FSFile as fsfile
            from trollflow2.launcher import message_to_jobs
            import json

            filename = "/S3A_OL_2_WFR____20201210T080758_20201210T080936_20201210T103707_0097_066_078_1980_MAR_O_NR_002.SEN3/Oa01_reflectance.nc"  # noqa
            fs = {"cls": "fsspec.implementations.zip.ZipFileSystem",
                  "protocol": "abstract",
                  "args": ["sentinel-s3-ol2wfr-zips/2020/12/10/S3A_OL_2_WFR____20201210T080758_20201210T080936_20201210T103707_0097_066_078_1980_MAR_O_NR_002.zip"],  # noqa
                  "target_protocol": "s3",
                  "target_options": {"anon": False,
                                     "client_kwargs": {"endpoint_url": "https://my.dismi.se"}}}
            msg_data = {"dataset": [{"filesystem": fs,
                                     "uid": "zip:///S3A_OL_2_WFR____20201210T080758_20201210T080936_20201210T103707_0097_066_078_1980_MAR_O_NR_002.SEN3/Oa01_reflectance.nc::s3:///sentinel-s3-ol2wfr-zips/2020/12/10/S3A_OL_2_WFR____20201210T080758_20201210T080936_20201210T103707_0097_066_078_1980_MAR_O_NR_002.zip",  # noqa
                                     "uri": "zip://" + filename
                                     }]
                        }

            msg = mock.MagicMock()
            msg.data = msg_data

            prodlist = yaml.load(yaml_test_minimal, Loader=UnsafeLoader)
            jobs = message_to_jobs(msg, prodlist)
            filesystemfile = jobs[999]['input_filenames'][0]

            assert filesystemfile == fsfile.return_value
            fsfile.assert_called_once_with(filename, abs_fs.from_json.return_value)
            abs_fs.from_json.assert_called_once_with(json.dumps(fs))


class TestRun(TestCase):
    """Test case for running the plugins."""

    def setUp(self):
        """Set up the test case."""
        super().setUp()
        self.config = yaml.load(yaml_test1, Loader=UnsafeLoader)

    def test_run(self):
        """Test running."""
        from trollflow2.launcher import run
        with mock.patch('trollflow2.launcher.yaml.load') as yaml_load,\
                mock.patch('trollflow2.launcher.open'),\
                mock.patch('trollflow2.launcher.process') as process,\
                mock.patch('multiprocessing.Process') as Process,\
                mock.patch('trollflow2.launcher.ListenerContainer') as lc_,\
                mock.patch('multiprocessing.Queue') as queue:
            listener = mock.MagicMock()
            listener.output_queue.get.return_value = 'foo'
            lc_.return_value = listener
            proc_ret = mock.MagicMock()
            Process.return_value = proc_ret
            # stop looping
            proc_ret.join.side_effect = KeyboardInterrupt
            yaml_load.return_value = self.config
            the_queue = mock.MagicMock()
            queue.return_value = the_queue
            prod_list = 'bar'
            try:
                run(prod_list)
            except KeyboardInterrupt:
                pass
            listener.output_queue.called_once()
            Process.assert_called_with(args=('foo', prod_list, the_queue), target=process)
            proc_ret.start.assert_called_once()
            proc_ret.join.assert_called_once()
            lc_.assert_called_with(addresses=None, nameserver='localhost',
                                   topics=['/topic1', '/topic2'])
            # Subscriber topics are removed from config
            self.assertTrue('subscribe_topics' not in self.config['product_list'])
            # Topics are given as command line option
            lc_.reset_mock()
            try:
                run(prod_list, topics=['/topic3'])
            except KeyboardInterrupt:
                pass
            lc_.assert_called_with(addresses=None, nameserver='localhost',
                                   topics=['/topic3'])

    def test_run_keyboard_interrupt(self):
        """Test interrupting the run with a ctrl-C."""
        from trollflow2.launcher import run
        with mock.patch('trollflow2.launcher.yaml.load'),\
                mock.patch('trollflow2.launcher.open'),\
                mock.patch('trollflow2.launcher.ListenerContainer') as lc_:
            listener = mock.MagicMock()
            get = mock.Mock()
            get.side_effect = KeyboardInterrupt
            listener.output_queue.get = get
            lc_.return_value = listener
            run(0)
            listener.stop.assert_called_once()


class TestExpand(TestCase):
    """Test expanding the product list."""

    def test_expand(self):
        """Test expanding the product list."""
        from trollflow2.launcher import expand
        inside = {'a': 'b'}
        outside = {'c': inside, 'd': inside}
        expanded = expand(outside)
        self.assertIsNot(expanded['d'], expanded['c'])


class TestProcess(TestCase):
    """Test case for the subprocessing."""

    def test_process(self):
        """Test subprocessing."""
        from trollflow2.launcher import process
        with mock.patch('trollflow2.launcher.traceback') as traceback,\
                mock.patch('trollflow2.launcher.sendmail') as sendmail,\
                mock.patch('trollflow2.launcher.expand') as expand,\
                mock.patch('trollflow2.launcher.yaml') as yaml_,\
                mock.patch('trollflow2.launcher.message_to_jobs') as message_to_jobs,\
                mock.patch('trollflow2.launcher.open') as open_,\
                mock.patch('trollflow2.launcher.get_dask_client') as gdc:

            fid = mock.MagicMock()
            fid.read.return_value = yaml_test1
            open_.return_value.__enter__.return_value = fid
            mock_config = mock.MagicMock()
            yaml_.load.return_value = mock_config
            yaml_.YAMLError = yaml.YAMLError
            # Make a client that has no `.close()` method (for coverage)
            client = mock.MagicMock()
            client.close.side_effect = AttributeError
            gdc.return_value = client
            fun1 = mock.MagicMock()
            # Return something resembling a config
            expand.return_value = {"workers": [{"fun": fun1}]}

            message_to_jobs.return_value = {1: {"job1": dict([])}}
            the_queue = mock.MagicMock()
            fun1.stop.assert_not_called()
            process("msg", "prod_list", the_queue)
            fun1.stop.assert_called_once()

            open_.assert_called_with("prod_list")
            yaml_.load.assert_called_once()
            message_to_jobs.assert_called_with("msg", {"workers": [{"fun": fun1}]})
            fun1.assert_called_with({'job1': {}, 'processing_priority': 1, 'produced_files': the_queue})
            gdc.assert_called_once()
            client.close.assert_called_once()

            fun1.stop = mock.MagicMock(side_effect=AttributeError('boo'))
            process("msg", "prod_list", the_queue)

            # Test that errors are propagated
            fun1.side_effect = KeyboardInterrupt
            with self.assertRaises(KeyboardInterrupt):
                process("msg", "prod_list", the_queue)
            # Test crash hander call.  This will raise KeyError as there
            # are no configured workers in the config returned by expand()
            traceback.format_exc.return_value = 'baz'
            crash_handlers = {"crash_handlers": {"config": {"foo": "bar"},
                                                 "handlers": [{"fun": sendmail}]}}
            expand.return_value = crash_handlers
            with self.assertRaises(KeyError):
                process("msg", "prod_list", the_queue)
            config = crash_handlers['crash_handlers']['config']
            sendmail.assert_called_once_with(config, 'baz')

            # Test failure in open(), e.g. a missing file
            open_.side_effect = IOError
            with self.assertRaises(IOError):
                process("msg", "prod_list", the_queue)

            # Test failure in yaml.load(), e.g. bad formatting
            open_.side_effect = yaml.YAMLError
            with self.assertRaises(yaml.YAMLError):
                process("msg", "prod_list", the_queue)


class TestDistributed(TestCase):
    """Test functions for distributed processing."""

    def test_get_dask_client(self):
        """Test getting dask client."""
        from trollflow2.launcher import get_dask_client

        ncores = mock.MagicMock()
        ncores.return_value = {}
        client = mock.MagicMock(ncores=ncores)
        client_class = mock.MagicMock()
        client_class.return_value = client

        # No client configured
        config = {}
        res = get_dask_client(config)
        assert res is None

        # Config is valid, but no workers are available
        config = {"dask_distributed": {"class": client_class,
                                       "settings": {"foo": 1, "bar": 2}
                                       }
                  }
        res = get_dask_client(config)
        assert res is None
        ncores.assert_called_once()
        client.close.assert_called_once()

        # The scheduler had no workers, the client doesn't have `.close()`
        client.close.side_effect = AttributeError
        res = get_dask_client(config)
        assert res is None

        # Config is valid, scheduler has workers
        ncores.return_value = {'a': 1, 'b': 1}
        res = get_dask_client(config)
        assert res is client
        assert ncores.call_count == 3

        # Scheduler couldn't connect to workers
        client_class.side_effect = OSError
        res = get_dask_client(config)
        assert res is None
        assert ncores.call_count == 3


if __name__ == '__main__':
    unittest.main()
