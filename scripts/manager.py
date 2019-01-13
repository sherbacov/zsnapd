#!/usr/bin/python3
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
Provides the overall functionality
"""

import time
import os
from datetime import datetime

from magcode.core.globals_ import *
from magcode.core.utility import connect_test_address

from scripts.zfs import ZFS
from scripts.clean import Cleaner
from scripts.helper import Helper

class Manager(object):
    """
    Manages the ZFS snapshotting process
    """

    @staticmethod
    def touch_trigger(ds_settings, *args):
        """
        Runs around creating .trigger files for datasets with time = trigger
        """
        result = True
        snapshots = ZFS.get_snapshots()
        datasets = ZFS.get_datasets()
        ds_candidates = [ds for ds in args if ds[0] != '/']
        mnt_candidates = [m for m in args if m[0] == '/']
        trigger_mnts_dict = {ds_settings[ds]['mountpoint']:ds for ds in ds_settings if ds_settings[ds]['time'] == 'trigger'}
        if len(ds_candidates):
            for candidate in ds_candidates:
                if candidate not in datasets:
                    log_error("Dataset '{0}' does not exist.".format(candidate))
                    sys.exit(os.EX_DATAERR)
                if candidate not in ds_settings:
                    log_error("Dataset '{0}' is not configured fo zsnapd.".format(candidate))
                    sys.exit(os.EX_DATAERR)
        if len(mnt_candidates):
            for candidate in mnt_candidates:
                if candidate not in trigger_mnts_dict:
                    log_error("Trigger mount '{0}' not configured for zsnapd".format(candidate))
                    sys.exit(os.EX_DATAERR)
                if trigger_mnts_dict[candidate] not in datasets:
                    log_error("Dataset '{0}' for trigger mount {1} does not exist.".format(candidate, trigger_mnts_dict[candidate]))
                    sys.exit(os.EX_DATAERR)
                ds_candidates.append(trigger_mnts_dict[candidate])

        for dataset in datasets:
            if dataset in ds_settings:
                if (len(ds_candidates) and dataset not in ds_candidates):
                    continue
                try:
                    dataset_settings = ds_settings[dataset]

                    take_snapshot = dataset_settings['snapshot'] is True
                    replicate = dataset_settings['replicate'] is not None

                    if take_snapshot is True or replicate is True:
                        if dataset_settings['time'] == 'trigger':
                            # Trigger file testing and creation
                            trigger_filename = '{0}/.trigger'.format(dataset_settings['mountpoint'])
                            if os.path.exists(trigger_filename):
                                continue
                            if (not os.path.isdir(dataset_settings['mountpoint'])):
                                log_error("Directory '{0}' does not exist.".format(dataset_settings['mountpoint']))
                                result = False
                                continue
                            trigger_file = open(trigger_filename, 'wt')
                            trigger_file.close()
                except Exception as ex:
                    log_error('Exception: {0}'.format(str(ex)))

        return result

    @staticmethod
    def run(ds_settings):
        """
        Executes a single run where certain datasets might or might not be snapshotted
        """

        now = datetime.now()
        today = '{0:04d}{1:02d}{2:02d}'.format(now.year, now.month, now.day)

        snapshots = ZFS.get_snapshots()
        datasets = ZFS.get_datasets()
        for dataset in datasets:
            if dataset in ds_settings:
                try:
                    dataset_settings = ds_settings[dataset]
                    local_snapshots = snapshots.get(dataset, [])

                    take_snapshot = dataset_settings['snapshot'] is True
                    replicate = dataset_settings['replicate'] is not None

                    # Decide whether we need to handle this dataset
                    execute = False
                    if take_snapshot is True or replicate is True:
                        if dataset_settings['time'] == 'trigger':
                            # We wait until we find a trigger file in the filesystem
                            trigger_filename = '{0}/.trigger'.format(dataset_settings['mountpoint'])
                            if os.path.exists(trigger_filename):
                                log_info('Trigger found on {0}'.format(dataset))
                                os.remove(trigger_filename)
                                execute = True
                        else:
                            trigger_time = dataset_settings['time'].split(':')
                            hour = int(trigger_time[0])
                            minutes = int(trigger_time[1])
                            if (now.hour > hour or (now.hour == hour and now.minute >= minutes)) and today not in local_snapshots:
                                log_info('Time passed for {0}'.format(dataset))
                                execute = True

                    if execute is True:
                        # Pre exectution command
                        if dataset_settings['preexec'] is not None:
                            Helper.run_command(dataset_settings['preexec'], '/')

                        if take_snapshot is True:
                            # Take today's snapshotzfs
                            log_info('Taking snapshot {0}@{1}'.format(dataset, today))
                            try:
                                ZFS.snapshot(dataset, today)
                            except Exception as ex:
                                # if snapshot fails move onto next one
                                log_error('Exception: {0}'.format(str(ex)))
                                continue
                            local_snapshots.append(today)
                            log_info('Taking snapshot {0}@{1} complete'.format(dataset, today))

                        # Replicating, if required
                        if replicate is True:
                            log_info('Replicating {0}'.format(dataset))
                            replicate_settings = dataset_settings['replicate']
                            push = replicate_settings['target'] is not None
                            remote_dataset = replicate_settings['target'] if push else replicate_settings['source']
                            remote_snapshots = ZFS.get_snapshots(remote_dataset, replicate_settings['endpoint'])
                            last_common_snapshot = None
                            if remote_dataset in remote_snapshots:
                                if push is True:  # If pushing, we search for the last local snapshot that is remotely available
                                    for snapshot in local_snapshots:
                                        if snapshot in remote_snapshots[remote_dataset]:
                                            last_common_snapshot = snapshot
                                else:  # Else, we search for the last remote snapshot that is locally available
                                    for snapshot in remote_snapshots[remote_dataset]:
                                        if snapshot in local_snapshots:
                                            last_common_snapshot = snapshot
                            if last_common_snapshot is not None:  # There's a common snapshot
                                previous_snapshot = None
                                if push is True:
                                    for snapshot in local_snapshots:
                                        if snapshot == last_common_snapshot:
                                            previous_snapshot = last_common_snapshot
                                            continue
                                        if previous_snapshot is not None:
                                            # There is a snapshot on this host that is not yet on the other side.
                                            size = ZFS.get_size(dataset, previous_snapshot, snapshot)
                                            log_info('  {0}@{1} > {0}@{2} ({3})'.format(dataset, previous_snapshot, snapshot, size))
                                            ZFS.replicate(dataset, previous_snapshot, snapshot, remote_dataset, replicate_settings['endpoint'], direction='push', compression=replicate_settings['compression'])
                                            ZFS.hold(dataset, snapshot)
                                            ZFS.hold(remote_dataset, snapshot, replicate_settings['endpoint'])
                                            ZFS.release(dataset, previous_snapshot)
                                            ZFS.release(remote_dataset, previous_snapshot, replicate_settings['endpoint'])
                                            previous_snapshot = snapshot
                                else:
                                    for snapshot in remote_snapshots[remote_dataset]:
                                        if snapshot == last_common_snapshot:
                                            previous_snapshot = last_common_snapshot
                                            continue
                                        if previous_snapshot is not None:
                                            # There is a remote snapshot that is not yet on the local host.
                                            size = ZFS.get_size(remote_dataset, previous_snapshot, snapshot, replicate_settings['endpoint'])
                                            log_info('  {0}@{1} > {0}@{2} ({3})'.format(remote_dataset, previous_snapshot, snapshot, size))
                                            ZFS.replicate(remote_dataset, previous_snapshot, snapshot, dataset, replicate_settings['endpoint'], direction='pull', compression=replicate_settings['compression'])
                                            ZFS.hold(dataset, snapshot)
                                            ZFS.hold(remote_dataset, snapshot, replicate_settings['endpoint'])
                                            ZFS.release(dataset, previous_snapshot)
                                            ZFS.release(remote_dataset, previous_snapshot, replicate_settings['endpoint'])
                                            previous_snapshot = snapshot
                            elif push is True and len(local_snapshots) > 0:
                                # No common snapshot
                                if remote_dataset not in remote_snapshots:
                                    # No remote snapshot, full replication
                                    snapshot = local_snapshots[-1]
                                    size = ZFS.get_size(dataset, None, snapshot)
                                    log_info('  {0}@         > {0}@{1} ({2})'.format(dataset, snapshot, size))
                                    ZFS.replicate(dataset, None, snapshot, remote_dataset, replicate_settings['endpoint'], direction='push', compression=replicate_settings['compression'])
                                    ZFS.hold(dataset, snapshot)
                                    ZFS.hold(remote_dataset, snapshot, replicate_settings['endpoint'])
                            elif push is False and remote_dataset in remote_snapshots and len(remote_snapshots[remote_dataset]) > 0:
                                # No common snapshot
                                if len(local_snapshots) == 0:
                                    # No local snapshot, full replication
                                    snapshot = remote_snapshots[remote_dataset][-1]
                                    size = ZFS.get_size(remote_dataset, None, snapshot, replicate_settings['endpoint'])
                                    log_info('  {0}@         > {0}@{1} ({2})'.format(remote_dataset, snapshot, size))
                                    ZFS.replicate(remote_dataset, None, snapshot, dataset, replicate_settings['endpoint'], direction='pull', compression=replicate_settings['compression'])
                                    ZFS.hold(dataset, snapshot)
                                    ZFS.hold(remote_dataset, snapshot, replicate_settings['endpoint'])
                            log_info('Replicating {0} complete'.format(dataset))

                        # Post execution command
                        if dataset_settings['postexec'] is not None:
                            Helper.run_command(dataset_settings['postexec'], '/')

                    # Cleaning the snapshots (cleaning is mandatory)
                    if today in local_snapshots:
                        Cleaner.clean(dataset, local_snapshots, dataset_settings['schema'])

                except Exception as ex:
                    log_error('Exception: {0}'.format(str(ex)))

