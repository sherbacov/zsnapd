# Copyright (c) 2014-2017 Kenneth Henderick <kenneth@ketronic.be>
# Copyright (c) 2019 Matthew Grant <matt@mattgrant.net.nz>
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.

"""
Provides functionality for cleaning up old ZFS snapshots
"""

import re
from datetime import datetime

from magcode.core.globals_ import log_info

from scripts.zfs import ZFS

CLEANER_REGEX = r'^((?P<hours>[0-9]+)h){0,1}(?P<days>[0-9]+)d(?P<weeks>[0-9]+)w(?P<months>[0-9]+)m(?P<years>[0-9]+)y$'

class Cleaner(object):
    """
    Cleaner class, containing all methods for cleaning up ZFS snapshots
    """

    logger = None  # The manager will fill this object

    @staticmethod
    def clean(dataset, snapshots, schema):
        today = datetime.now()

        # Parsing schema
        match = re.match(CLEANER_REGEX, schema)
        if not match:
            log_info('Got invalid schema for dataset {0}: {1}'.format(dataset, schema))
            return
        matchinfo = match.groupdict()
        settings = {}
        for key in list(matchinfo.keys()):
            settings[key] = int(matchinfo[key] if matchinfo[key] is not None else 0)

        # Loading snapshots
        snapshot_dict = []
        held_snapshots = []
        for snapshot in snapshots:
            if re.match('^(\d{4})(1[0-2]|0[1-9])(0[1-9]|[1-2]\d|3[0-1])(([0-1][0-9]|2[0-3])([0-5][0-9])){0,1}$', snapshot) is not None:
                if ZFS.is_held(dataset, snapshot):
                    held_snapshots.append(snapshot)
                    continue
                if (len(snapshot) > 8):
                    snapshot_date = datetime.strptime(snapshot, '%Y%m%d%H%M')
                else:
                    snapshot_date = datetime.strptime(snapshot, '%Y%m%d')
                snapshot_dict.append({'name': snapshot,
                                      'time': snapshot_date,
                                      'age': int((today - snapshot_date).total_seconds()/3600)})
        buckets = {}
        counter = -1
        for i in range(settings['hours']):
            counter += 1
            buckets[counter] = []
        for i in range(settings['days']):
            counter += (1 * 24)
            buckets[counter] = []
        for i in range(settings['weeks']):
            counter += (7 * 24)
            buckets[counter] = []
        for i in range(settings['months']):
            counter += (30 * 24)
            buckets[counter] = []
        for i in range(settings['years']):
            counter += (30 * 12 * 24)
            buckets[counter] = []

        will_delete = False

        end_of_life_snapshots = []
        for snapshot in snapshot_dict:
            possible_keys = []
            for key in buckets:
                if snapshot['age'] <= key:
                    possible_keys.append(key)
            if possible_keys:
                buckets[min(possible_keys)].append(snapshot)
            else:
                will_delete = True
                end_of_life_snapshots.append(snapshot)

        to_delete = {}
        to_keep = {}
        for key in buckets:
            oldest = None
            if len(buckets[key]) == 1:
                oldest = buckets[key][0]
            else:
                for snapshot in buckets[key]:
                    if oldest is None:
                        oldest = snapshot
                    elif snapshot['age'] > oldest['age']:
                        oldest = snapshot
                    else:
                        will_delete = True
                        to_delete[key] = to_delete.get(key, []) + [snapshot]
            to_keep[key] = oldest
            to_delete[key] = to_delete.get(key, [])

        if will_delete is True:
            log_info('Cleaning {0}'.format(dataset))
            for snapshot in held_snapshots:
                log_info('  Skipping held {0}@{1}'.format(dataset, snapshot))

        keys = list(to_delete.keys())
        keys.sort()
        for key in keys:
            for snapshot in to_delete[key]:
                log_info('  Destroying {0}@{1}'.format(dataset, snapshot['name']))
                ZFS.destroy(dataset, snapshot['name'])
        for snapshot in end_of_life_snapshots:
            log_info('  Destroying {0}@{1}'.format(dataset, snapshot['name']))
            ZFS.destroy(dataset, snapshot['name'])

        if will_delete is True:
            log_info('Cleaning {0} complete'.format(dataset))
