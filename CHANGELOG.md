## Version 0.12.0 (2021/12/20)

### Issues Closed

* [Issue 125](https://github.com/pytroll/trollflow2/issues/125) - Writing ninjo(geo)tiff with use_tmp_file writes temporary filename to metadata ([PR 126](https://github.com/pytroll/trollflow2/pull/126) by [@gerritholl](https://github.com/gerritholl))

In this release 1 issue was closed.

### Pull Requests Merged

#### Bugs fixed

* [PR 128](https://github.com/pytroll/trollflow2/pull/128) - Fix scene attribute usage in sza_check ([1943](https://github.com/pytroll/satpy/issues/1943))

#### Features added

* [PR 128](https://github.com/pytroll/trollflow2/pull/128) - Fix scene attribute usage in sza_check ([1943](https://github.com/pytroll/satpy/issues/1943))
* [PR 126](https://github.com/pytroll/trollflow2/pull/126) - Add flag to create temporary file in temporary directory ([125](https://github.com/pytroll/trollflow2/issues/125), [125](https://github.com/pytroll/trollflow2/issues/125))

In this release 3 pull requests were closed.


## Version 0.11.1 (2021/11/22)

### Pull Requests Merged

#### Bugs fixed

* [PR 124](https://github.com/pytroll/trollflow2/pull/124) - Logging fix using global QueueHandler

In this release 1 pull request was closed.


## Version 0.11.0 (2021/10/21)

### Issues Closed

* [Issue 121](https://github.com/pytroll/trollflow2/issues/121) - trollflow2 fails to launch with pyyaml 6.0 ([PR 122](https://github.com/pytroll/trollflow2/pull/122) by [@pnuu](https://github.com/pnuu))
* [Issue 116](https://github.com/pytroll/trollflow2/issues/116) - Netcdf product with multiple dataset can't be created anymore ([PR 119](https://github.com/pytroll/trollflow2/pull/119) by [@mraspaud](https://github.com/mraspaud))
* [Issue 113](https://github.com/pytroll/trollflow2/issues/113) - Improve usefulness of "all files produced nominally in x seconds" ([PR 114](https://github.com/pytroll/trollflow2/pull/114) by [@gerritholl](https://github.com/gerritholl))
* [Issue 111](https://github.com/pytroll/trollflow2/issues/111) - Check channel-dependent validity ([PR 112](https://github.com/pytroll/trollflow2/pull/112) by [@gerritholl](https://github.com/gerritholl))
* [Issue 103](https://github.com/pytroll/trollflow2/issues/103) - Dpath isn't maintained anymore

In this release 5 issues were closed.

### Pull Requests Merged

#### Bugs fixed

* [PR 120](https://github.com/pytroll/trollflow2/pull/120) - Fix MacOS timeout test skip
* [PR 119](https://github.com/pytroll/trollflow2/pull/119) - Allow products based on multiple datasets to be published ([116](https://github.com/pytroll/trollflow2/issues/116))
* [PR 115](https://github.com/pytroll/trollflow2/pull/115) - Use queued logging for running in subprocesses

#### Features added

* [PR 122](https://github.com/pytroll/trollflow2/pull/122) - Use safe loading for logging config ([121](https://github.com/pytroll/trollflow2/issues/121))
* [PR 114](https://github.com/pytroll/trollflow2/pull/114) - Add no. of files to check_results log message ([113](https://github.com/pytroll/trollflow2/issues/113))
* [PR 112](https://github.com/pytroll/trollflow2/pull/112) - Skip products when data contain too many fill values ([111](https://github.com/pytroll/trollflow2/issues/111))
* [PR 108](https://github.com/pytroll/trollflow2/pull/108) - Make it possible to use `FilePublisher` without running a nameserver

In this release 7 pull requests were closed.


## Version 0.10.0 (2021/04/12)

### Issues Closed

* [Issue 100](https://github.com/pytroll/trollflow2/issues/100) - Trollflow2 stops processing if plugin gets stuck (messages go unprocessed, no error message in log) ([PR 104](https://github.com/pytroll/trollflow2/pull/104))

In this release 1 issue was closed.

### Pull Requests Merged

#### Bugs fixed

* [PR 110](https://github.com/pytroll/trollflow2/pull/110) - For unstable build, also install pyresample.

#### Features added

* [PR 104](https://github.com/pytroll/trollflow2/pull/104) - Add an option to configure a timeout for plugins ([100](https://github.com/pytroll/trollflow2/issues/100))

In this release 2 pull requests were closed.


## Version 0.9.0 (2021/03/31)

### Issues Closed

* [Issue 96](https://github.com/pytroll/trollflow2/issues/96) - Latest trollflow2 master fails with latest satpy master ([PR 97](https://github.com/pytroll/trollflow2/pull/97))
* [Issue 92](https://github.com/pytroll/trollflow2/issues/92) - Go through logging levels and adjust as necessary ([PR 93](https://github.com/pytroll/trollflow2/pull/93))
* [Issue 77](https://github.com/pytroll/trollflow2/issues/77) - Sometimes weird products is send to plugins.FilePublisher ([PR 109](https://github.com/pytroll/trollflow2/pull/109))
* [Issue 28](https://github.com/pytroll/trollflow2/issues/28) - Filename sometimes defined in format section

In this release 4 issues were closed.

### Pull Requests Merged

#### Bugs fixed

* [PR 109](https://github.com/pytroll/trollflow2/pull/109) - Check that products exist before publishing ([77](https://github.com/pytroll/trollflow2/issues/77))
* [PR 105](https://github.com/pytroll/trollflow2/pull/105) - Fix fsfile-induced test failures
* [PR 97](https://github.com/pytroll/trollflow2/pull/97) - Adapt trollflow2 for removal of ppp_config_dir ([96](https://github.com/pytroll/trollflow2/issues/96), [96](https://github.com/pytroll/trollflow2/issues/96))
* [PR 90](https://github.com/pytroll/trollflow2/pull/90) - Fix a default resolution bug making satpy 0.23 crash

#### Features added

* [PR 102](https://github.com/pytroll/trollflow2/pull/102) - Add github actions
* [PR 101](https://github.com/pytroll/trollflow2/pull/101) - Add area name to "area kept" log message ([101](https://github.com/pytroll/trollflow2/issues/101))
* [PR 94](https://github.com/pytroll/trollflow2/pull/94) - Add possibility to provide a jsonified filesystem spec in the message
* [PR 93](https://github.com/pytroll/trollflow2/pull/93) - Adjust message levels and wordings ([92](https://github.com/pytroll/trollflow2/issues/92))
* [PR 91](https://github.com/pytroll/trollflow2/pull/91) - Add the aggregate plugin

In this release 9 pull requests were closed.


## Version 0.8.0 (2020/09/24)

### Issues Closed

* [Issue 88](https://github.com/pytroll/trollflow2/issues/88) - Failure when saving in satellite-native projection ([PR 89](https://github.com/pytroll/trollflow2/pull/89))
* [Issue 82](https://github.com/pytroll/trollflow2/issues/82) - Add support for dask.distributed ([PR 83](https://github.com/pytroll/trollflow2/pull/83))

In this release 2 issues were closed.

### Pull Requests Merged

#### Bugs fixed

* [PR 89](https://github.com/pytroll/trollflow2/pull/89) - Fix null resampling with newer versions of satpy ([88](https://github.com/pytroll/trollflow2/issues/88))

#### Features added

* [PR 86](https://github.com/pytroll/trollflow2/pull/86) - Replace usage of DatasetID by DataQuery for newer satpy versions
* [PR 83](https://github.com/pytroll/trollflow2/pull/83) - Add support to dask.distributed ([82](https://github.com/pytroll/trollflow2/issues/82))

In this release 3 pull requests were closed.


## Version v0.7.1 (2020/07/16)


### Pull Requests Merged

#### Bugs fixed

* [PR 85](https://github.com/pytroll/trollflow2/pull/85) - Fix broken scm versioning

#### Features added

* [PR 85](https://github.com/pytroll/trollflow2/pull/85) - Fix broken scm versioning

In this release 2 pull requests were closed.


## Version v0.7.0 (2020/07/16)

### Issues Closed

* [Issue 74](https://github.com/pytroll/trollflow2/issues/74) - Add documentation on configuration options ([PR 75](https://github.com/pytroll/trollflow2/pull/75))
* [Issue 36](https://github.com/pytroll/trollflow2/issues/36) - Document Trollflow production chain - Meteosat
* [Issue 21](https://github.com/pytroll/trollflow2/issues/21) - Don't create scene object for sensor data not asked for

In this release 3 issues were closed.

### Pull Requests Merged

#### Bugs fixed

* [PR 84](https://github.com/pytroll/trollflow2/pull/84) - Upgrade packages on travis installation
* [PR 69](https://github.com/pytroll/trollflow2/pull/69) - Publish topic was wrong in config file
* [PR 68](https://github.com/pytroll/trollflow2/pull/68) - Fix conda channel priority issue

#### Features added

* [PR 73](https://github.com/pytroll/trollflow2/pull/73) - Add a pluging to check message metadata
* [PR 71](https://github.com/pytroll/trollflow2/pull/71) - Implement call to native resampler
* [PR 70](https://github.com/pytroll/trollflow2/pull/70) - Use get_geostationary_bounding_box from pyresample instead of satpy
* [PR 67](https://github.com/pytroll/trollflow2/pull/67) - Use area definition names to check sunlight coverage ([228](https://github.com/pytroll/pyresample/issues/228))
* [PR 58](https://github.com/pytroll/trollflow2/pull/58) - Fix versioning using setuptools_scm and leave versioneer.py

#### Documentation changes

* [PR 76](https://github.com/pytroll/trollflow2/pull/76) - Fix RST formatting
* [PR 75](https://github.com/pytroll/trollflow2/pull/75) - Document plugins ([74](https://github.com/pytroll/trollflow2/issues/74))

In this release 10 pull requests were closed.


## Version 0.6.1 (2019/11/15)


### Pull Requests Merged

#### Bugs fixed

* [PR 66](https://github.com/pytroll/trollflow2/pull/66) - Fix formats dictionary being the same object

In this release 1 pull request was closed.


## Version 0.6.0 (2019/11/14)


### Pull Requests Merged

#### Bugs fixed

* [PR 65](https://github.com/pytroll/trollflow2/pull/65) - Stop the workers when the process is done
* [PR 63](https://github.com/pytroll/trollflow2/pull/63) - Fix publisher stopping after first processed area
* [PR 61](https://github.com/pytroll/trollflow2/pull/61) - Ensure composites are generated for scenes without resampling
* [PR 60](https://github.com/pytroll/trollflow2/pull/60) - Check the area from correct level of the product list
* [PR 55](https://github.com/pytroll/trollflow2/pull/55) - Make productname, areaname and format optional in the published message

#### Features added

* [PR 65](https://github.com/pytroll/trollflow2/pull/65) - Stop the workers when the process is done
* [PR 62](https://github.com/pytroll/trollflow2/pull/62) - Check if the composites are actually produced when the trollflow2 process is over
* [PR 61](https://github.com/pytroll/trollflow2/pull/61) - Ensure composites are generated for scenes without resampling
* [PR 59](https://github.com/pytroll/trollflow2/pull/59) - Add "nameserver" and "addresses" command-line options
* [PR 57](https://github.com/pytroll/trollflow2/pull/57) - Add the possibility to add extra metadata to the published messages
* [PR 56](https://github.com/pytroll/trollflow2/pull/56) - Add GitHub templates and make flake8 happy
* [PR 53](https://github.com/pytroll/trollflow2/pull/53) - Add max daylight coverage feature
* [PR 52](https://github.com/pytroll/trollflow2/pull/52) - Add the possibility to provide a log config to satpy_laucher.py
* [PR 51](https://github.com/pytroll/trollflow2/pull/51) - Allow multiple bands tuple to be passed as a single composite
* [PR 50](https://github.com/pytroll/trollflow2/pull/50) - Fix the RTD pages
* [PR 49](https://github.com/pytroll/trollflow2/pull/49) - Add some metadata to the published file messages
* [PR 48](https://github.com/pytroll/trollflow2/pull/48) - Switch to pytest and add codecov reports
* [PR 47](https://github.com/pytroll/trollflow2/pull/47) - Add dispatch order messages
* [PR 46](https://github.com/pytroll/trollflow2/pull/46) - Make writing via tmp file more robust

In this release 19 pull requests were closed.

## Version v0.5.0 (2019/07/01)


### Pull Requests Merged

#### Bugs fixed

* [PR 38](https://github.com/pytroll/trollflow2/pull/38) - Handle gracefully the situation when a dataset is not loaded
* [PR 37](https://github.com/pytroll/trollflow2/pull/37) - Handle the situation when data is not covering the area at all

#### Features added

* [PR 45](https://github.com/pytroll/trollflow2/pull/45) - Add pre-commit config
* [PR 44](https://github.com/pytroll/trollflow2/pull/44) - Remove print statement
* [PR 43](https://github.com/pytroll/trollflow2/pull/43) - Adding .stickler.yml configuration file
* [PR 42](https://github.com/pytroll/trollflow2/pull/42) - Add option to run in test-mode providing a specific message on the co…
* [PR 41](https://github.com/pytroll/trollflow2/pull/41) - Allow saving files to a temporary name and small fixes
* [PR 40](https://github.com/pytroll/trollflow2/pull/40) - Add the areas level
* [PR 39](https://github.com/pytroll/trollflow2/pull/39) - Refactor trollflow2 to put plugins in their own file
* [PR 35](https://github.com/pytroll/trollflow2/pull/35) - Add pass coverage computation to sunlight coverage checking
* [PR 34](https://github.com/pytroll/trollflow2/pull/34) - Add debuginfo area coverage

In this release 11 pull requests were closed.

## Version 0.4.1 (2019/04/10)


### Pull Requests Merged

#### Bugs fixed

* [PR 33](https://github.com/pytroll/trollflow2/pull/33) - Fix hanging publisher

#### Features added

* [PR 32](https://github.com/pytroll/trollflow2/pull/32) - Feature sunlight coverage

In this release 2 pull requests were closed.

## Version 0.4.0 (2019/04/08)


### Pull Requests Merged

#### Bugs fixed

* [PR 30](https://github.com/pytroll/trollflow2/pull/30) - Use only one sensor for coverage calculations
* [PR 26](https://github.com/pytroll/trollflow2/pull/26) - Handle aliases for iterable metadata values

#### Features added

* [PR 30](https://github.com/pytroll/trollflow2/pull/30) - Use only one sensor for coverage calculations
* [PR 27](https://github.com/pytroll/trollflow2/pull/27) - Add overviews to output images
* [PR 26](https://github.com/pytroll/trollflow2/pull/26) - Handle aliases for iterable metadata values
* [PR 25](https://github.com/pytroll/trollflow2/pull/25) - Add a possibility to send emails about crashes
* [PR 24](https://github.com/pytroll/trollflow2/pull/24) - Check `collection_area_id` in the input metadata
* [PR 23](https://github.com/pytroll/trollflow2/pull/23) - Add a possibility to define subscribe topics in config file
* [PR 22](https://github.com/pytroll/trollflow2/pull/22) - Make publish topic composable
* [PR 6](https://github.com/pytroll/trollflow2/pull/6) - Add a docker example

In this release 10 pull requests were closed.

## Version 0.3.0 (2019/03/19)


### Pull Requests Merged

#### Bugs fixed

* [PR 20](https://github.com/pytroll/trollflow2/pull/20) - Handling nicer the situation where the scene cannot be created when th…
* [PR 19](https://github.com/pytroll/trollflow2/pull/19) - Fix compatibility issues caused by changes introduced in pyyaml 5.1
* [PR 18](https://github.com/pytroll/trollflow2/pull/18) - First take the info from the scene object, then update with what is p…
* [PR 15](https://github.com/pytroll/trollflow2/pull/15) - Fix plist_iter to provide the area and product keys too

#### Features added

* [PR 20](https://github.com/pytroll/trollflow2/pull/20) - Handling nicer the situation where the scene cannot be created when th…
* [PR 17](https://github.com/pytroll/trollflow2/pull/17) - Allow formats to be specified at any level
* [PR 16](https://github.com/pytroll/trollflow2/pull/16) - Make it possible to delay composite creation

In this release 7 pull requests were closed.

## Version 0.2.0 (2019/02/28)


### Pull Requests Merged

#### Bugs fixed

* [PR 12](https://github.com/pytroll/trollflow2/pull/12) - Fix and test the expand function
* [PR 4](https://github.com/pytroll/trollflow2/pull/4) - Skip bogus composites to allow saving the rest

#### Features added

* [PR 14](https://github.com/pytroll/trollflow2/pull/14) - Add support for passing a `resolution` parameter to the scene loading
* [PR 11](https://github.com/pytroll/trollflow2/pull/11) - Expose more kwargs for scn.resample()
* [PR 10](https://github.com/pytroll/trollflow2/pull/10) - Report error when process crashes
* [PR 9](https://github.com/pytroll/trollflow2/pull/9) - Expand minimal product lists before processing
* [PR 8](https://github.com/pytroll/trollflow2/pull/8) - Add support for products in satellite projection
* [PR 7](https://github.com/pytroll/trollflow2/pull/7) - Add Sun zenith angle filtering
* [PR 5](https://github.com/pytroll/trollflow2/pull/5) - Add possibility to prioritize production by area
* [PR 3](https://github.com/pytroll/trollflow2/pull/3) - Add a plugin to update input metadata with aliases
* [PR 2](https://github.com/pytroll/trollflow2/pull/2) - Add a plugin to check platform name
* [PR 1](https://github.com/pytroll/trollflow2/pull/1) - Make plugins configurable

In this release 12 pull requests were closed.
