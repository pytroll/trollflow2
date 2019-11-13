.. Trollflow2 documentation master file, created by
   sphinx-quickstart on Thu Sep 19 23:01:42 2019.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to Trollflow2's documentation!
======================================

.. toctree::
   :maxdepth: 2
   :caption: Contents:


Trollflow2 is a batch runner for Satpy.

See the example playlist (`pl.yaml`) for inspiration.

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
`__call__` method implemented. If they are to be instanciated from the yaml
configuration file, a `__setstate__` method will need to be implemented if
arguments are to be passed for the initialization of the class.

If the callable has a `stop` method, it will be called without arguments at the
end of each run (one scene).

An example of such a callable class used in trollflow2 is the
`class:FilePublisher`.


Available plugins
+++++++++++++++++

.. currentmodule:: trollflow2.plugins

The `check_sunlight_coverage` plugin
************************************

.. function:: check_sunlight_coverage



Product list
------------

* `resolution` is available only at the root and product levels
* `productname`, `areaname` are the names to use for product and area in the
  filename. If not provided, they default to the actual product and area names.

Example
*******

.. code-block:: yaml

  product_list:
    output_dir: &output_dir
      "/data/{variant}/"
    use_extern_calib: false
    fname_pattern: &fname
      "{platform_name}_{start_time:%Y%m%d_%H%M}_{areaname}_{productname}.{format}"
    publish_topic: /raster/2A/avhrr
    reader: avhrr_l1b_aapp
    mask_area: True
    delay_composites: True
    use_tmp_file: True
    metadata_aliases:
      variant:
        EARS: regional
        DR: direct_readout
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




Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
