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


Available plugins
-----------------

.. currentmodule:: trollflow2.plugings

The `check_sunlight_coverage` plugin
************************************

.. function:: check_sunlight_coverage


              
Product list
------------

* `resolution` is available only at the root and product levels
* `productname`, `areaname` are the names to use for product and area in the
  filename. If not provided, they default to the actual product and area names. 




Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
