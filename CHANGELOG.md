
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
