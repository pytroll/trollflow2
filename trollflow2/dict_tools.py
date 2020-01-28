#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2019 Pytroll developers

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>

# Workaround for unittests so that satpy and posttroll installations
# are not necessary
"""Tools for product list operations."""

import dpath.util


def plist_iter(product_list, base_mda=None, level=None):
    """Iterate over the product list at a configurable level.

    This function walks through the product-list (depth-wise) and yields
    two configuration items each time: a flattened version of the configurations
    accumulated accross the levels and the current item's configuration.

    *base_mda* provides the base configuration to include, and *level* (one of
    'area', 'product', or None (default, all levels included)) is the max depth
    to walk the product list at.
    """
    if base_mda is None:
        base_mda = {}
    else:
        base_mda = base_mda.copy()
    for area, area_config in product_list['areas'].items():
        aconfig = base_mda.copy()
        aconfig.update(product_list)
        aconfig.pop('areas', None)
        aconfig.update(area_config)
        aconfig.pop('products', None)
        aconfig['area'] = area
        if level == 'area':
            yield aconfig, area_config
            continue
        for prod, prod_config in area_config['products'].items():
            pconfig = aconfig.copy()
            pconfig.update(prod_config)
            pconfig['product'] = prod
            if level == 'product':
                yield pconfig, prod_config
                continue
            for file_config in pconfig.get('formats', [{'format': 'tif', 'writer': 'geotiff'}]):
                fconfig = pconfig.copy()
                fconfig.pop('formats', None)
                fconfig.update(file_config)
                yield fconfig, file_config


def gen_dict_extract(var, key):
    """Generate the values of *key* recusively from the dict *var*."""
    if hasattr(var, 'items'):
        for k, v in var.items():
            if k == key:
                yield v
            if hasattr(v, 'items'):
                for result in gen_dict_extract(v, key):
                    yield result
            elif isinstance(v, list):
                for d in v:
                    for result in gen_dict_extract(d, key):
                        yield result


def get_config_value(config, path, key, default=None):
    """Get the most local config value for key *key* starting from the dictionary path *path*.

    If nothing is found, path "/common/" is also checked, and if still nothing is found, return *default*.
    """
    path_parts = path.split('/')
    # Loop starting from the current path, and continue upwards
    # towards the root until something is found
    num = len(path_parts)
    for i in range(num, 1, -1):
        pwd = "/".join(path_parts[:i] + [key])
        vals = dpath.util.values(config, pwd)
        if len(vals) > 0:
            return vals[0]

    vals = dpath.util.values(config, "/common/" + key)
    if len(vals) > 0:
        return vals[0]

    return default
