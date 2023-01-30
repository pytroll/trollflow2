# Copyright (c) 2019 Pytroll developers
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
#
# Workaround for unittests so that satpy and posttroll installations
# are not necessary

"""S3 object store plugins for Trollflow2."""

from trollflow2.dict_tools import plist_iter


def uploader(job):
    """Upload data to S3 and update the filenames.

    Optionally also delete the files after the upload.
    """
    from trollmoves.movers import S3Mover

    s3_config = job['product_list']['product_list'].get('s3_config', {}).copy()
    delete_files = s3_config.pop('delete_files', False)

    for fmt, fmt_config in plist_iter(job['product_list']['product_list']):
        local_fname = fmt_config['filename']
        s3_target = fmt['s3_config']['target']
        mover = S3Mover(local_fname, s3_target)
        if delete_files:
            mover.move()
        else:
            mover.copy()
        s3_uri = fmt['filename'].replace(fmt['output_dir'], s3_target)
        fmt_config['filename'] = s3_uri
