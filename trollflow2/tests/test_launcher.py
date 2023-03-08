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

import datetime
import logging
import os
import queue
import time
import unittest
from contextlib import contextmanager

import pytest
import yaml
from yaml import YAMLError

from trollflow2.launcher import launch
from trollflow2.tests.test_logging import duplicate_lines

try:
    from yaml import UnsafeLoader
except ImportError:
    from yaml import Loader as UnsafeLoader

from unittest import mock

from trollflow2.launcher import VALID_MESSAGE_TYPES, generate_messages, process
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

workers:
  - fun: !!python/name:trollflow2.plugins.create_scene
  - fun: !!python/name:trollflow2.plugins.load_composites
  - fun: !!python/name:trollflow2.plugins.resample
  - fun: !!python/name:trollflow2.plugins.save_datasets
#  - fun: !!python/object:trollflow2.FilePublisher {}
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
            import json

            from fsspec.spec import AbstractFileSystem as abs_fs
            from satpy.readers import FSFile as fsfile

            from trollflow2.launcher import message_to_jobs

            filename = "zip:///S3A_OL_2_WFR____20201210T080758_20201210T080936_20201210T103707_0097_066_078_1980_MAR_O_NR_002.SEN3/Oa01_reflectance.nc"  # noqa
            fs = {"cls": "fsspec.implementations.zip.ZipFileSystem",
                  "protocol": "abstract",
                  "args": ["sentinel-s3-ol2wfr-zips/2020/12/10/S3A_OL_2_WFR____20201210T080758_20201210T080936_20201210T103707_0097_066_078_1980_MAR_O_NR_002.zip"],  # noqa
                  "target_protocol": "s3",
                  "target_options": {"anon": False,
                                     "client_kwargs": {"endpoint_url": "https://my.dismi.se"}}}
            msg_data = {"dataset": [{"filesystem": fs,
                                     "uid": filename,
                                     "uri": filename + "::s3:///sentinel-s3-ol2wfr-zips/2020/12/10/S3A_OL_2_WFR____20201210T080758_20201210T080936_20201210T103707_0097_066_078_1980_MAR_O_NR_002.zip",  # noqa
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

    def test_message_to_jobs_with_fsspec_uri_without_fs_info(self):
        """Test transforming a message containing a url without fsspec specification."""
        with mock.patch.dict('sys.modules', {'fsspec': mock.MagicMock(),
                                             'fsspec.spec': mock.MagicMock(),
                                             'satpy': mock.MagicMock(),
                                             'satpy.readers': mock.MagicMock(),
                                             'satpy.resample': mock.MagicMock(),
                                             'satpy.writers': mock.MagicMock(),
                                             'satpy.dataset': mock.MagicMock(),
                                             'satpy.version': mock.MagicMock()}):
            from trollflow2.launcher import message_to_jobs

            filename = "/S3A_OL_2_WFR____20201210T080758_20201210T080936_20201210T103707_0097_066_078_1980_MAR_O_NR_002.SEN3/Oa01_reflectance.nc"  # noqa
            uri = "simplecache::ssh://someserver.example.com" + filename

            msg_data = {"dataset": [{"uid": filename,
                                     "uri": uri,
                                     }]
                        }

            msg = mock.MagicMock()
            msg.data = msg_data

            prodlist = yaml.load(yaml_test_minimal, Loader=UnsafeLoader)
            jobs = message_to_jobs(msg, prodlist)
            extracted_filename = jobs[999]['input_filenames'][0]

            assert extracted_filename == uri

    def test_message_to_jobs_non_fsspec_uri(self):
        """Test transforming a message containing a url without fsspec specification."""
        with mock.patch.dict('sys.modules', {'fsspec': mock.MagicMock(),
                                             'fsspec.spec': mock.MagicMock(),
                                             'satpy': mock.MagicMock(),
                                             'satpy.readers': mock.MagicMock(),
                                             'satpy.resample': mock.MagicMock(),
                                             'satpy.writers': mock.MagicMock(),
                                             'satpy.dataset': mock.MagicMock(),
                                             'satpy.version': mock.MagicMock()}):
            from trollflow2.launcher import message_to_jobs

            filename = "/S3A_OL_2_WFR____20201210T080758_20201210T080936_20201210T103707_0097_066_078_1980_MAR_O_NR_002.SEN3/Oa01_reflectance.nc"  # noqa
            uri = "ssh://someserver.example.com" + filename

            msg_data = {"dataset": [{"uid": filename,
                                     "uri": uri
                                     }]
                        }

            msg = mock.MagicMock()
            msg.data = msg_data

            prodlist = yaml.load(yaml_test_minimal, Loader=UnsafeLoader)
            jobs = message_to_jobs(msg, prodlist)
            extracted_filename = jobs[999]['input_filenames'][0]

            assert extracted_filename == uri


def test_subprocess_uses_queued_logging(tmp_path, caplog):
    """Test that the subprocess logs are handled correctly."""
    with mock.patch('trollflow2.launcher._create_listener_from_connection_parameters') as listener_creator:
        from posttroll.message import Message
        msg = Message('/my/topic', atype='file', data={'filename': 'foo'})
        listener_creator.return_value.output_queue.get.side_effect = [msg, KeyboardInterrupt]

        config_filename = os.fspath(tmp_path / "myconfig.yaml")
        with open(config_filename, mode="w") as fd:
            fd.write(yaml_test_minimal)
        try:
            launch(["my_topic", config_filename])
        except KeyboardInterrupt:
            pass
        assert "All 0 files produced nominally in" in caplog.text
        assert config_filename in caplog.text
        assert "Creating scene" in caplog.text
        assert not duplicate_lines(caplog.text)


pnuus_log_config = """
version: 1
formatters:
  fmt:
    format: '[%(asctime)s %(levelname)-8s %(name)s] %(message)s'
handlers:
  console:
    class: logging.StreamHandler
    formatter: fmt
    stream: ext://sys.stdout
loggers:
  '':
    level: WARNING
    handlers: [console]
  'trollflow2':
    level: INFO
"""


def test_subprocess_uses_custom_queued_logging(tmp_path, caplog):
    """Test that the subprocess logs with custom log config are handled correctly."""
    with mock.patch('trollflow2.launcher._create_listener_from_connection_parameters') as listener_creator:
        from posttroll.message import Message
        msg = Message('/my/topic', atype='file', data={'filename': 'foo'})
        listener_creator.return_value.output_queue.get.side_effect = [msg, KeyboardInterrupt]

        config_filename = os.fspath(tmp_path / "myconfig.yaml")
        with open(config_filename, mode="w") as fd:
            fd.write(yaml_test_minimal)

        log_config_filename = os.fspath(tmp_path / "mylogconfig.yaml")
        with open(log_config_filename, mode="w") as fd:
            fd.write(pnuus_log_config)

        try:
            launch(["my_topic", config_filename, "-c", log_config_filename])
        except KeyboardInterrupt:
            pass
        assert "All 0 files produced nominally in" in caplog.text
        assert "Creating scene" in caplog.text
        assert not duplicate_lines(caplog.text)


class TestRun(TestCase):
    """Test case for running the plugins."""

    def setUp(self):
        """Set up the test case."""
        super().setUp()
        self.config = yaml.load(yaml_test1, Loader=UnsafeLoader)

    def test_run_does_not_call_process_directly(self):
        """Test that process is called through Process."""
        from trollflow2.launcher import Runner
        with mock.patch('trollflow2.launcher.yaml.load'),\
                mock.patch('trollflow2.launcher.open'),\
                mock.patch('trollflow2.launcher.generate_messages') as generate_messages,\
                mock.patch('trollflow2.launcher.process') as process,\
                mock.patch('trollflow2.launcher.check_results'),\
                mock.patch('multiprocessing.get_context'):
            generate_messages.side_effect = ['foo', KeyboardInterrupt]
            prod_list = {'product_list': {}}
            try:
                runner = Runner(prod_list)
                runner.run()
            except KeyboardInterrupt:
                pass
            process.assert_not_called()

    def test_run_relies_on_listener(self):
        """Test running relies on listener."""
        from trollflow2.launcher import Runner
        with mock.patch('trollflow2.launcher.yaml.load') as yaml_load,\
                mock.patch('trollflow2.launcher.open'),\
                mock.patch('multiprocessing.get_context') as get_context, \
                mock.patch('trollflow2.launcher.ListenerContainer') as lc_:
            msg = mock.MagicMock()
            msg.type = 'file'
            listener = mock.MagicMock()
            listener.output_queue.get.return_value = msg
            lc_.return_value = listener
            proc_ret = mock.MagicMock()
            get_context.return_value.Process.return_value = proc_ret
            # stop looping
            proc_ret.join.side_effect = KeyboardInterrupt
            yaml_load.return_value = self.config
            prod_list = {'product_list': {}}
            try:
                runner = Runner(prod_list)
                runner.run()
            except KeyboardInterrupt:
                pass
            listener.output_queue.called_once()
            lc_.assert_called_with(addresses=None, nameserver='localhost',
                                   topics=['/topic1', '/topic2'])
            # Subscriber topics are removed from config
            self.assertTrue('subscribe_topics' not in self.config['product_list'])
            # Topics are given as command line option
            lc_.reset_mock()
            try:
                runner = Runner(prod_list, connection_parameters=dict(topic=['/topic3']))
                runner.run()
            except KeyboardInterrupt:
                pass
            lc_.assert_called_with(addresses=None, nameserver='localhost',
                                   topics=['/topic3'])

    def test_run_starts_and_joins_process(self):
        """Test running."""
        with run_on_a_simple_product_list(self.config) as (yaml_load, get_context, lc_, proc_ret):
            proc_ret.start.assert_called_once()
            proc_ret.join.assert_called_once()

    def test_subprocess_is_spawned(self):
        """Test that the subprocess is spawned, not forked."""
        with run_on_a_simple_product_list(self.config) as (yaml_load, get_context, lc_, proc_ret):
            get_context.assert_called_once_with("spawn")


@contextmanager
def run_on_a_simple_product_list(config):
    """Run a simple (fake) product list."""
    from trollflow2.launcher import Runner
    with mock.patch('trollflow2.launcher.yaml.load') as yaml_load,\
            mock.patch('trollflow2.launcher.open'),\
            mock.patch('multiprocessing.get_context') as get_context,\
            mock.patch('trollflow2.launcher.ListenerContainer') as lc_:

        msg = mock.MagicMock()
        msg.type = 'file'
        listener = mock.MagicMock()
        listener.output_queue.get.return_value = msg
        lc_.return_value = listener
        proc_ret = mock.MagicMock()
        get_context.return_value.Process.return_value = proc_ret
        # stop looping
        proc_ret.join.side_effect = KeyboardInterrupt
        yaml_load.return_value = config
        prod_list = {'product_list': {}}
        try:
            runner = Runner(prod_list)
            runner.run()
        except KeyboardInterrupt:
            pass
        yield yaml_load, get_context, lc_, proc_ret


class TestInterruptRun(TestCase):
    """Test case for running the plugins."""

    def setUp(self):
        """Set up the test case."""
        super().setUp()
        self.config = yaml.load(yaml_test1, Loader=UnsafeLoader)

    def test_run_keyboard_interrupt(self):
        """Test interrupting the run with a ctrl-C."""
        from trollflow2.launcher import Runner
        with mock.patch('trollflow2.launcher.yaml.load'),\
                mock.patch('trollflow2.launcher.open'),\
                mock.patch('trollflow2.launcher.ListenerContainer') as lc_:
            listener = mock.MagicMock()
            get = mock.Mock()
            get.side_effect = KeyboardInterrupt
            listener.output_queue.get = get
            lc_.return_value = listener
            runner = Runner({'product_list': {}})
            runner.run()
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
    """Test the process function."""

    def setUp(self):
        """Set up the test."""
        self.patcher = mock.patch.multiple("trollflow2.launcher",
                                           traceback=mock.DEFAULT,
                                           sendmail=mock.DEFAULT,
                                           expand=mock.DEFAULT,
                                           yaml=mock.DEFAULT,
                                           message_to_jobs=mock.DEFAULT,
                                           open=mock.DEFAULT,
                                           get_dask_client=mock.DEFAULT)
        mocks = self.patcher.start()

        self.traceback = mocks['traceback']
        self.sendmail = mocks['sendmail']

        self.open = mocks['open']
        fid = mock.MagicMock()
        fid.read.return_value = yaml_test1
        self.open.return_value.__enter__.return_value = fid

        self.yaml = mocks['yaml']
        # Is this necessary?
        self.yaml.YAMLError = YAMLError

        self.get_dask_client = mocks['get_dask_client']
        # Make a client that has no `.close()` method (for coverage)
        self.client = mock.MagicMock()
        self.client.close.side_effect = AttributeError
        self.get_dask_client.return_value = self.client

        self.expand = mocks['expand']
        self.fake_plugin = mock.MagicMock()
        # Return something resembling a config
        self.expand.return_value = {"workers": [{"fun": self.fake_plugin}]}

        self.message_to_jobs = mocks['message_to_jobs']
        self.message_to_jobs.return_value = {1: {"job1": dict([])}}

        self.queue = mock.MagicMock()

    def tearDown(self):
        """Tear down the test case."""
        self.patcher.stop()

    def test_plugin_is_stopped_after_processing(self):
        """Test plugin is stopped after processing."""
        self.fake_plugin.stop.assert_not_called()
        process("msg", "prod_list", self.queue)
        self.fake_plugin.stop.assert_called_once()

    def test_product_list_is_opened(self):
        """Test product list is opened."""
        process("msg", "prod_list", self.queue)
        self.open.assert_called_with("prod_list")

    def test_yaml_config_is_read_only_once(self):
        """Test that the yaml config is read only once."""
        process("msg", "prod_list", self.queue)
        self.yaml.load.assert_called_once()

    def test_workers_config_is_passed_down(self):
        """Test that the workers config is used."""
        process("msg", "prod_list", self.queue)
        self.message_to_jobs.assert_called_with("msg", {"workers": [{"fun": self.fake_plugin}]})

    def test_plugin_is_used(self):
        """Test that the plugin is being used."""
        process("msg", "prod_list", self.queue)
        self.fake_plugin.assert_called_with({'job1': {}, 'processing_priority': 1, 'produced_files': self.queue})

    def test_dask_client_is_used(self):
        """Test that the dask client is used."""
        process("msg", "prod_list", self.queue)
        self.get_dask_client.assert_called_once()

    def test_dask_client_is_closed(self):
        """Test that the dask client is closed."""
        process("msg", "prod_list", self.queue)
        self.client.close.assert_called_once()

    def test_plugin_with_no_stop_work(self):
        """Test that plugins with no `stop` method (like regular functions) can be used."""
        self.fake_plugin.stop = mock.MagicMock(side_effect=AttributeError('boo'))
        process("msg", "prod_list", self.queue)

    def test_error_propagation(self):
        """Test that errors are propagated."""
        self.fake_plugin.side_effect = KeyboardInterrupt
        with pytest.raises(KeyboardInterrupt):
            process("msg", "prod_list", self.queue)

    def test_crash_handler_call(self):
        """Test crash hander call.

        This will raise KeyError as there are no configured workers in the config returned by expand().
        """
        self.traceback.format_exc.return_value = 'baz'
        crash_handlers = {"crash_handlers": {"config": {"foo": "bar"},
                                             "handlers": [{"fun": self.sendmail}]}}
        self.expand.return_value = crash_handlers
        with pytest.raises(KeyError):
            process("msg", "prod_list", self.queue)
        config = crash_handlers['crash_handlers']['config']
        self.sendmail.assert_called_once_with(config, 'baz')

    def test_open_missing_file(self):
        """Test failure in open() due to a missing config file."""
        self.open.side_effect = IOError
        with pytest.raises(IOError):
            process("msg", "prod_list", self.queue)

    def test_open_bad_yaml(self):
        """Test failure in yaml.load(), e.g. bad formatting."""
        self.open.side_effect = YAMLError
        with pytest.raises(YAMLError):
            process("msg", "prod_list", self.queue)

    def test_timeout_in_running_job(self):
        """Test timeout in running job."""
        def wait(job):
            del job
            time.sleep(0.1)

        self.fake_plugin.side_effect = wait

        self.expand.return_value = {"workers": [{"fun": self.fake_plugin, "timeout": 0.05}]}
        with pytest.raises(TimeoutError, match="Timeout for .* expired "
                                               "after 0.1 seconds"):
            process("msg", "prod_list", self.queue)
        # wait a little to ensure alarm is not raised later
        time.sleep(0.11)


def test_workers_initialized():
    """Test that the config loading works when workers are defined."""
    import os
    from tempfile import NamedTemporaryFile

    queue = mock.MagicMock()

    with NamedTemporaryFile(mode='w+t', delete=False) as tmp_file:
        fname = tmp_file.name
        tmp_file.write(yaml_test_minimal)
        tmp_file.close()

        try:
            with mock.patch("trollflow2.launcher.get_dask_client") as gdc:
                # `get_dask_client()` is called just after config reading, so if we get there loading worked
                gdc.side_effect = StopIteration
                try:
                    process("msg", fname, queue)
                except StopIteration:
                    pass
        finally:
            os.remove(fname)


def test_get_dask_client(caplog):
    """Test getting dask client."""
    from trollflow2.launcher import get_dask_client

    ncores = mock.MagicMock()
    ncores.return_value = {}
    client = mock.MagicMock(ncores=ncores)
    client_class = mock.MagicMock()
    client_class.return_value = client

    # No client configured
    config = {}
    with caplog.at_level(logging.DEBUG):
        res = get_dask_client(config)
    assert "Distributed processing not configured" in caplog.text
    caplog.clear()
    assert res is None

    # Config is valid, but no workers are available
    config = {"dask_distributed": {"class": client_class,
                                   "settings": {"foo": 1, "bar": 2}
                                   }
              }
    with caplog.at_level(logging.WARNING):
        res = get_dask_client(config)
    assert "No workers available, reverting to default scheduler" in caplog.text
    caplog.clear()
    assert res is None
    ncores.assert_called_once()
    client.close.assert_called_once()

    # The scheduler had no workers, the client doesn't have `.close()`
    client.close.side_effect = AttributeError
    with caplog.at_level(logging.WARNING):
        res = get_dask_client(config)
    assert res is None

    # Config is valid, scheduler has workers
    ncores.return_value = {'a': 1, 'b': 1}
    with caplog.at_level(logging.DEBUG):
        res = get_dask_client(config)
    assert "Using dask distributed client" in caplog.text
    caplog.clear()
    assert res is client
    assert ncores.call_count == 3

    # Scheduler couldn't connect to workers
    client_class.side_effect = OSError
    with caplog.at_level(logging.ERROR):
        res = get_dask_client(config)
    assert "Scheduler not found, reverting to default scheduler" in caplog.text
    caplog.clear()
    assert res is None
    assert ncores.call_count == 3


class FakeQueue:
    """Face queue class."""

    def __init__(self, lo, hi, tmp_path, skip=None, write=True):
        """Initialize fake queue."""
        if skip is None:
            skip = []
        self._files = set()
        for i in range(lo, hi):
            f = os.path.join(tmp_path, f"file{i:d}")
            self._files.add(str(f))
            if i not in skip:
                if write:
                    with open(f, mode="wt") as fp:
                        fp.write("zucchini" * i)

    def get(self, block=None):
        """Get item from queue."""
        try:
            return self._files.pop()
        except KeyError:
            raise queue.Empty

    def qsize(self):
        """Get queue size."""
        return len(self._files)


def _run_check_results(start_time, exitcode, produced_files, caplog):
    from trollflow2.launcher import check_results

    with caplog.at_level(logging.DEBUG):
        check_results(produced_files, start_time, exitcode)
    return caplog.text


def test_check_results_empty_file(tmp_path, caplog):
    """Test functionality for check_results."""
    start_time = datetime.datetime(1900, 1, 1)
    exitcode = 0
    produced_files = FakeQueue(0, 3, tmp_path)
    caplog_text = _run_check_results(start_time, exitcode, produced_files, caplog)
    assert "Empty file detected" in caplog_text
    assert "files produced nominally" not in caplog_text


def test_check_results_missing_file(tmp_path, caplog):
    """Test functionality for check_results."""
    start_time = datetime.datetime(1900, 1, 1)
    exitcode = 0
    produced_files = FakeQueue(5, 8, tmp_path, skip=[6])
    caplog_text = _run_check_results(start_time, exitcode, produced_files, caplog)
    assert "Missing file" in caplog_text
    assert "files produced nominally" not in caplog_text


def test_check_results_nominal(tmp_path, caplog):
    """Test functionality for check_results."""
    start_time = datetime.datetime(1900, 1, 1)
    exitcode = 0
    produced_files = FakeQueue(10, 13, tmp_path)
    with mock.patch("trollflow2.launcher.datetime") as dd:
        dd.now.return_value = datetime.datetime(1927, 5, 20, 0, 0)
        caplog_text = _run_check_results(start_time, exitcode, produced_files, caplog)
    assert "All 3 files produced nominally in 10000 days" in caplog_text


def test_check_results_crash(tmp_path, caplog):
    """Test functionality for check_results."""
    start_time = datetime.datetime(1900, 1, 1)
    exitcode = 1
    produced_files = FakeQueue(10, 13, tmp_path)
    caplog_text = _run_check_results(start_time, exitcode, produced_files, caplog)
    assert "Process crashed with exit code 1" in caplog_text


def test_check_results_killed(tmp_path, caplog):
    """Test functionality for check_results."""
    start_time = datetime.datetime(1900, 1, 1)
    exitcode = -1
    produced_files = FakeQueue(10, 13, tmp_path)
    caplog_text = _run_check_results(start_time, exitcode, produced_files, caplog)
    assert "Process killed with signal 1" in caplog_text


def test_check_results_s3_nominally(tmp_path, caplog):
    """Test functionality for check_results."""
    start_time = datetime.datetime(1900, 1, 1)
    exitcode = 0
    s3fs_mock = mock.MagicMock()
    with mock.patch.dict("sys.modules", {"s3fs": s3fs_mock}):
        produced_files = FakeQueue(0, 3, "s3://bucket-name", write=False)
        s3fs_mock.S3FileSystem.return_value.stat.return_value = {'size': 0}
        caplog_text = _run_check_results(
            start_time, exitcode, produced_files, caplog)
        assert "Empty file detected" in caplog_text
        assert "files produced nominally" not in caplog_text


def test_check_results_s3_missing_file(tmp_path, caplog):
    """Test functionality for check_results."""
    start_time = datetime.datetime(1900, 1, 1)
    exitcode = 0
    s3fs_mock = mock.MagicMock()
    with mock.patch.dict("sys.modules", {"s3fs": s3fs_mock}):
        produced_files = FakeQueue(5, 8, tmp_path, skip=[6])
        s3fs_mock.S3FileSystem.return_value.stat.side_effect = FileNotFoundError
        caplog_text = _run_check_results(
            start_time, exitcode, produced_files, caplog)
        assert "Missing file" in caplog_text
        assert "files produced nominally" not in caplog_text


def test_check_results_s3_file_exists(tmp_path, caplog):
    """Test functionality for check_results."""
    start_time = datetime.datetime(1900, 1, 1)
    exitcode = 0
    s3fs_mock = mock.MagicMock()
    with mock.patch.dict("sys.modules", {"s3fs": s3fs_mock}):
        from trollflow2.launcher import check_results
        produced_files = FakeQueue(10, 13, 's3://bucket-name/', write=False)
        check_results(produced_files, start_time, exitcode)
        assert mock.call('s3://bucket-name/file10') in s3fs_mock.S3FileSystem.return_value.stat.mock_calls
        assert mock.call('s3://bucket-name/file11') in s3fs_mock.S3FileSystem.return_value.stat.mock_calls
        assert mock.call('s3://bucket-name/file12') in s3fs_mock.S3FileSystem.return_value.stat.mock_calls


def test_argparse_nameserver_is_none():
    """Test that '-n false' is sets nameserver as False."""
    from trollflow2.launcher import parse_args

    res = parse_args(['-n', 'false', 'product_list.yaml'])
    assert res['nameserver'] is False


def test_launch():
    """Test launcher.launch()."""
    with mock.patch("trollflow2.launcher.Runner") as Runner:
        from trollflow2.launcher import launch

        args_in = ['-n', 'localhost', '-a', 'localhost:12345', 'product_list.yaml']
        launch(args_in)
        # See that the commandline arguments are passed to Runner
        assert "'nameserver': 'localhost'" in str(Runner.mock_calls)
        assert "'addresses': ['localhost:12345']" in str(Runner.mock_calls)
        assert "product_list.yaml" in str(Runner.mock_calls)
        Runner.return_value.run.assert_called_once()


def test_generate_messages():
    """Test the generate_messages function."""
    messages = [mock.MagicMock(type=_type) for _type in VALID_MESSAGE_TYPES + ("foo",)]
    with mock.patch('trollflow2.launcher.ListenerContainer') as lc_:
        listener = mock.MagicMock()
        listener.output_queue.get.side_effect = messages + [KeyboardInterrupt]
        lc_.return_value = listener
        for msg in generate_messages(connection_parameters=dict(topic=['/topic3'])):
            assert msg.type in VALID_MESSAGE_TYPES


if __name__ == '__main__':
    unittest.main()
