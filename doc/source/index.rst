.. Trollflow2 documentation master file, created by
   sphinx-quickstart on Wed Oct  2 23:07:10 2019.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to Trollflow2's documentation!
======================================

.. toctree::
   :maxdepth: 2
   :caption: Contents:


Trollflow2 is an operational generation chain runner for Satpy.

See the example playlist (``pl.yaml``) for inspiration.

The launcher
------------

.. automodule:: trollflow2.launcher
    :members:
    :undoc-members:
    :show-inheritance:


Plugins
-------

The plugins are callables that are to be run in order for the batch processing
to happen. These are usually functions, but they can be a class with a
``__call__`` method implemented. If they are to be instanciated from the yaml
configuration file, a ``__setstate__`` method will need to be implemented if
arguments are to be passed for the initialization of the class.

If the callable has a ``stop`` method, it will be called without arguments at the
end of each run (one scene).

An example of such a callable class used in trollflow2 is the
``FilePublisher`` plugin.

Most of the configuration are done within the product list.  The
notable exception are the initialization options for the class-based
plugins.

Main plugins
++++++++++++

.. currentmodule:: trollflow2.plugins

Scene creation
**************

The ``create_scene`` plugin creates a scene object.  The configuration
options are:

 - ``reader`` - Name of the reader to use to open the data.  If not
   given, Satpy will try to find the reader automatically.
 - ``reader_kwargs`` - A dictionary of special keyword arguments passed
   to the reader.
 - ``ppp_config_dir`` - Path to the Pytroll configuration directory.
   If not given, environment variable ``$PPP_CONFIG_DIR`` is used.

This plugin needs to be defined before data are accessed in any way.

Dataset/composite loading
*************************

After the scene has been created with ``create_scene``, composites and
other datasets can be loaded with ``load_composites``. The
composites/datasets are defined in the product list YAML file under
``products`` section within a section of target area.  There are two
special keyword arguments:

 - ``resolution`` - If there are several resolutions of the same dataset,
   this argument can be defined to select the correct one.  By default
   the highest resolution data are used.
 - ``delay_composites`` - The composite generation can be delayed until
   resampling.  This can save a lot of time if the same channels are
   used in several composites.  Default: ``True``.

Resampling
**********

After the data are loaded, it can be resampled with ``resample`` plugin.
There are multiple different options depending on the resampler.  For
complete list of resampler options see documentation of `Satpy
resampling <https://satpy.readthedocs.io/en/latest/resample.html>`_.

The default values are:

 - ``resampler: nearest``
 - ``radius_of_influence: null``
 - ``reduce_data: True``
 - ``cache_dir: None``
 - ``mask_area: False``
 - ``epsilon: 0.0``

For dynamic area definitions, which are not named, one can define the area as ``null`` and define one of

 - ``use_min_area: True`` or
 - ``use_max_area: True``

The ``null`` area is also used when saving data without reprojecting.
If the composites need matching of different resolutions, the native
resampler can be used:

 - ``resampler: native``

Saving the data
***************

The ``save_datasets`` plugin initiates the actual computations.  There
are several options:

 - ``formats`` dictionary with a list of wanted output file formats,
   writers and file patterns.  The handled arguments, with default values, are:
  - ``format: tif``
  - ``writer: geotiff``
  - ``fname_pattern: '{platform_name}_{start_time:%Y%m%d_%H%M}_{areaname}_{productname}.{format}'``
 - ``use_tmp_file: bool`` - First save the data to a temporary filename
   and then rename to the final version.

The ``fname_pattern`` can be a global setting at the top-level of the
configuration, and overridden on area and product levels.

Messaging for saved datasets
****************************

If there are further software that need to receive Posttroll messages
of the saved datasets, ``FilePublisher`` can be used.  This is typically
the last plugin in the chain, right after ``save_datasets``.

The available options, with the default values, are:

 - ``port: 0`` - publish messages in this port.  By default a random
   port is selected.
 - ``nameservers: null`` - a list of nameservers to connect to.  Defining
   any nameserver turns multicast messaging off.  Default to the
   nameserver on localhost and use multicast

These options are given in the list of workers as a dictionary
argument to the ``FilePublisher`` class.

Auxiliary plugins
+++++++++++++++++

These plugins are used to filter metadata, skip creation of scenes
with unwanted sensors or platforms, skip generation of products with
too low/high sun lit area coverage, and so on.

Sun light coverage
******************

The ``check_sunlight_coverage`` plugin can be used to estimate the area
coverage of the Sun-lit area within the target area.  If the coverage
is outside the specification for a given product, it is discarded for
that scene.  This plugin needs ``pytroll-schedule`` to be installed.

Options within a ``sunlight_coverage`` section:
 - ``min: <float>`` - Minimum required Sun-lit coverage in percentages
   for the product to be generated.
 - ``max: <float>`` - Maximum allowed Sun-lit coverage in percentages for
   the product to be generated.
 - ``check_pass: <bool>`` - Use orbital parameters to compute the
   overpass coverage for a polar satellite.

The check can be in three different places, depending on the areas
being processed:

 - after ``create_scene`` when area is defined in areas.yaml
 - after ``load_composites`` when area is ``null``
  - using ``use_min_area: True`` or ``use_max_area: True``
  - the original data are to be saved without resampling
  - ``resampler: native`` is used
 - after ``resampler`` for backwards compatibility, although this wastes time

For explanation on the individual resampler options see the
`Satpy <https://satpy.readthedocs.io/en/latest/resample.html>`_ and
further
`Pyresample <https://pyresample.readthedocs.io/en/latest/swath.html>`_
documentation.

Add overviews
*************

Add overview (multi-scale embedded) images in a TIFF file with
``add_overviews`` plugin.  The overviews are configured within the
``formats`` section:

 - ``overviews: [ 4, 8 16, 32, 128, 256]``

This plugins should be used after ``save_datasets`` and before
``FilePublisher`` plugins.

Sun zenith angle check
**********************

The ``sza_check`` plugin can be used to check Sun zenith angle (SZA) at
a given location.  The product will be discarded for the scene if the
SZA value is out of the configured values.

Options:
 - ``sunzen_check_lon: <float>`` - Longitude of the SZA check.
 - ``sunzen_check_lat: <float>`` - Latitude of the SZA check.
 - ``sunzen_minimum_angle: <float>`` - Minimum required SZA for the
   product to be processed.  Used for night-time products.
 - ``sunzen_maximum_angle: <float>`` - Maximum allowed SZA for the
   product to be processed.  Used for day-time products.

Area coverage
*************

The ``covers`` plugin can be used to check that the received data covers
the target areas.  If the area coverage is too low (see options
below), the area is discarded from the processing of this scene.  This
plugin needs ``pytroll-schedule`` to be installed.

Options:
 - ``coverage_by_collection_area: False`` - If ``True``, the
   ``'collection_area_id'`` in the incoming message needs to match the
   name of the target area.
 - ``min_coverage: 0`` - Minimum required coverage.  If coverage is less
   than defined, the data are not processed for this area.  By default
   process all areas.

Platform name check
*******************

The ``check_platform`` plugin can be used to check that the platform
name in the incoming message is in the configured list of platforms.
The best place for this plugin is before any data handling is done.

Options:
 - ``processed_platforms: null`` - A list of allowed platform names.  By
   default (``null``) all platforms are processed.

Metadata checks
***************

The ``check_metadata`` plugin can be used to check that for example the
sensor name, platform name, or any item in the incoming message
matches with the configuration.  If the metadata item name is defined
in the config, but the value is not listed, the whole scene is
discarded.  If the configured item isn't in the input message
metadata, a warning is printed and the processing continues.  The best
place for this plugin is before any data handling is done.

Options:
 - ``check_metadata: null`` - A dictionary of metadata names and list(s)
   of values that need to match.  By default (``null``) all scenes are
   processed.

Metadata aliasing
*****************

The ``metadata_alias`` plugin can be used to replace any metadata value
in the incoming message with an alias.  This might be required if
e.g. platform name or sensor name is not the one supported by Satpy
(``'AVHRR/3'`` vs. ``'avhrr-3'``).

Options:
 - ``metadata_aliases: null`` - A nested dictionary with a structure
   ``{'metadata_item_name': {'original_value': 'replacement_value'}}``.

Product list
------------

 - ``resolution`` is available only at the root and product levels
 - ``productname``, ``areaname`` are the names to use for product and area
   in the filename. If not provided, they default to the actual product
   and area names.

Example
*******

.. code-block:: yaml

  product_list:
    output_dir: &output_dir
      "/data/{variant}/"
    use_extern_calib: false
    fname_pattern: &fname
      "{platform_name}_{start_time:%Y%m%d_%H%M}_{areaname}_{productname}.{format}"
    subscribe_topics:
      - /incoming/topic1
      - /incoming/topic2
    publish_topic: /raster/2A/avhrr/{areaname}/{productname}
    reader: avhrr_l1b_aapp
    mask_area: True
    delay_composites: True
    use_tmp_file: True
    metadata_aliases:
      variant:
        EARS: regional
        DR: direct_readout
    check_metadata:
      platform_name:
      - NOAA-19
      - Metop-A
      - Metop-B
      - Metop-C
      sensor:
      - avhrr-3

    min_coverage: 25
    areas:
      baws:
        areaname: baws
        products:
          overview_sun:
            sunlight_coverage:
              min: 10
              check_pass: True
            productname: overview
            output_dir: "/data/some/other/place"
            formats:
              - format: png
                writer: simple_image
                fname_pattern: "{start_time:%Y%m%d_%H%M}_{platform_name:s}_{productname:s}_{variant:s}.{format}"
          ("1", "2"):  # This will load both channels one and two, but will keep them together when being saved to a single file.
            productname: visible_channels
            formats:
              - format: nc
                writer: cf
                fname_pattern: "{start_time:%Y%m%d_%H%M}_{platform_name:s}_{productname:s}_{variant:s}.{format}"
          green_snow:
            sunlight_coverage:
              min: 10
              check_pass: True
            productname: green_snow
            formats:
              - format: tif
                writer: geotiff
                fname_pattern: "{start_time:%Y%m%d_%H%M}_{platform_name:s}_{productname:s}_{variant:s}.{format}"
          natural_color_sun:
            sunlight_coverage:
              min: 10
              check_pass: True
            productname: natural_color
            formats:
              - format: tif
                writer: geotiff
                fname_pattern: "{start_time:%Y%m%d_%H%M}_{platform_name:s}_natural_{variant:s}.{format}"
          cloudtop:
            productname: cloudtop
            formats:
              - format: tif
                writer: geotiff
                fname_pattern: "{start_time:%Y%m%d_%H%M}_{platform_name:s}_{productname:s}_{variant:s}.{format}"

  workers:
    - fun: !!python/name:trollflow2.plugins.check_platform
    - fun: !!python/name:trollflow2.plugins.check_sensor
    - fun: !!python/name:trollflow2.plugins.create_scene
    - fun: !!python/name:trollflow2.plugins.check_sunlight_coverage
    - fun: !!python/name:trollflow2.plugins.covers
    - fun: !!python/name:trollflow2.plugins.load_composites
    - fun: !!python/name:trollflow2.plugins.resample
    - fun: !!python/name:trollflow2.plugins.save_datasets
    - fun: !!python/name:trollflow2.plugins.add_overviews
    - fun: !!python/object:trollflow2.plugins.FilePublisher {port: 40004, nameservers: [localhost]}



Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
