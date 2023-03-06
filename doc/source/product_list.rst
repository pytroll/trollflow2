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
    # For S3 object storage
    # output_dir: &output_dir
    #   "s3://bucket/"
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
    # For temporary storage of files for certain writers and S3 storage
    # staging_zone: /path/to/local/directory/
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
    # - fun: !!python/name:trollflow2.plugins.s3.uploader
    - fun: !!python/object:trollflow2.plugins.FilePublisher {port: 40004, nameservers: [localhost]}
