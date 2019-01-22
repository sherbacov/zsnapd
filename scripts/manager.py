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
import re
from collections import OrderedDict

from magcode.core.globals_ import *
from magcode.core.utility import connect_test_address
from magcode.core.utility import get_numeric_setting

from scripts.zfs import ZFS
from scripts.clean import Cleaner
from scripts.helper import Helper
from scripts.config import MeterTime
from scripts.globals_ import SNAPSHOTNAME_FMTSPEC

PROC_FAILURE = 0
PROC_EXECUTED = 1
PROC_CHANGED = 2

class IsConnected(object):
    """
    Test object class for caching endpoint connectivity and testing for it as well
    """
    def __init__(self):
        self.unconnected_list = []
        self.connected_list = []

    def _test_connected(self, host, port):
        connect_retry_wait = get_numeric_setting('connect_retry_wait', float)
        exc_msg = ''
        for t in range(3):
            try:
                # Transform any hostname to an IP address
                connect_test_address(host, port)
                break
            except(IOError, OSError) as exc:
                exc_msg = str(exc)
                time.sleep(connect_retry_wait)
                continue
        else:
            log_error("Can't reach endpoint '{0}:{1}' - {2}".format(host, port, exc_msg))
            return False
        return True

    def test_unconnected(self, dataset_settings):
        """
        Check that endpoint is unconnected
        """
        replicate_param = dataset_settings['replicate']
        if (replicate_param and replicate_param['endpoint_host']):
            host = replicate_param['endpoint_host']
            port = replicate_param['endpoint_port']
            if ((host, port) in self.unconnected_list):
                return(True)
            if ((host, port) not in self.connected_list):
                if self._test_connected(host, port):
                    self.connected_list.append((host, port))
                    # Go and write trigger
                else:
                    self.unconnected_list.append((host, port))
                    return(True)
        return(False)

class Manager(object):
    """
    Manages the ZFS snapshotting process
    """

    @staticmethod
    def touch_trigger(ds_settings, test_reachable, *args):
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

        is_connected = IsConnected()
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
                            # Check endpoint for trigger is connected
                            if test_reachable and is_connected.test_unconnected(dataset_settings):
                                continue
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
        del is_connected
        return result

    @staticmethod
    def take_snapshot(dataset, local_snapshots, now):
        result = PROC_EXECUTED
        this_time = time.strftime(SNAPSHOTNAME_FMTSPEC, time.localtime(now))
        # Take this_time's snapshotzfs
        log_info('Taking snapshot {0}@{1}'.format(dataset, this_time))
        try:
            ZFS.snapshot(dataset, this_time)
        except Exception as ex:
            # if snapshot fails move onto next one
            log_error('Exception: {0}'.format(str(ex)))
            return PROC_FAILURE
        else:
            local_snapshots.update({this_time:{'name': this_time, 'creation': now}})
            log_info('Taking snapshot {0}@{1} complete'.format(dataset, this_time))
            result = PROC_CHANGED
        return result

    @staticmethod
    def replicate_push_byparts(dataset, local_snapshots, dataset_settings):
        result = PROC_EXECUTED
        log_info('Replicating {0}'.format(dataset))
        replicate_settings = dataset_settings['replicate']
        push = replicate_settings['target'] is not None
        remote_dataset = replicate_settings['target'] if push else replicate_settings['source']
        remote_snapshots = ZFS.get_snapshots(remote_dataset, replicate_settings['endpoint'])
        last_common_snapshot = None
        if remote_dataset in remote_snapshots:
            # If pushing, we search for the last local snapshot that is remotely available
            for snapshot in local_snapshots:
                if snapshot in remote_snapshots[remote_dataset]:
                    last_common_snapshot = snapshot
        if last_common_snapshot is not None:  # There's a common snapshot
            previous_snapshot = None
            for snapshot in local_snapshots:
                if snapshot == last_common_snapshot:
                    previous_snapshot = last_common_snapshot
                    continue
                if previous_snapshot is not None:
                    prevsnap_name = local_snapshots[previous_snapshot]['name']
                    snap_name = local_snapshots[snapshot]['name']
                    # There is a snapshot on this host that is not yet on the other side.
                    size = ZFS.get_size(dataset, prevsnap_name, snap_name)
                    log_info('  {0}@{1} > {0}@{2} ({3})'.format(dataset, previous_snapshot, snapshot, size))
                    ZFS.replicate(dataset, prevsnap_name, snap_name, remote_dataset, replicate_settings['endpoint'], direction='push', compression=replicate_settings['compression'])
                    ZFS.hold(dataset, snap_name)
                    ZFS.hold(remote_dataset, snap_name, replicate_settings['endpoint'])
                    ZFS.release(dataset, prevsnap_name)
                    ZFS.release(remote_dataset, prevsnap_name, replicate_settings['endpoint'])
                    previous_snapshot = snapshot
                    result = PROC_CHANGED
        elif len(local_snapshots) > 0 and remote_dataset not in remote_snapshots:
            # No remote snapshot, full replication
            snapshot = list(local_snapshots)[-1]
            snap_name = local_snapshots[snapshot]['name']
            size = ZFS.get_size(dataset, None, snap_name)
            log_info('  {0}@         > {0}@{1} ({2})'.format(dataset, snapshot, size))
            ZFS.replicate(dataset, None, snap_name, remote_dataset, replicate_settings['endpoint'], direction='push', compression=replicate_settings['compression'])
            ZFS.hold(dataset, snap_name)
            ZFS.hold(remote_dataset, snap_name, replicate_settings['endpoint'])
            result = PROC_CHANGED
        log_info('Replicating {0} complete'.format(dataset))
        return result

    @staticmethod
    def replicate_pull_byparts(dataset, local_snapshots, dataset_settings):
        result = PROC_EXECUTED
        log_info('Replicating {0}'.format(dataset))
        replicate_settings = dataset_settings['replicate']
        push = replicate_settings['target'] is not None
        remote_dataset = replicate_settings['target'] if push else replicate_settings['source']
        remote_snapshots = ZFS.get_snapshots(remote_dataset, replicate_settings['endpoint'])
        last_common_snapshot = None
        if remote_dataset in remote_snapshots:
            # Else, we search for the last remote snapshot that is locally available
            for snapshot in remote_snapshots[remote_dataset]:
                if snapshot in local_snapshots:
                    last_common_snapshot = snapshot
        if last_common_snapshot is not None:  # There's a common snapshot
            previous_snapshot = None
            for snapshot in remote_snapshots[remote_dataset]:
                if snapshot == last_common_snapshot:
                    previous_snapshot = last_common_snapshot
                    continue
                if previous_snapshot is not None:
                    # There is a remote snapshot that is not yet on the local host.
                    prevsnap_name = local_snapshots[previous_snapshot]['name']
                    snap_name = local_snapshots[snapshot]['name']
                    size = ZFS.get_size(remote_dataset, prevsnap_name, snap_name, replicate_settings['endpoint'])
                    log_info('  {0}@{1} > {0}@{2} ({3})'.format(remote_dataset, previous_snapshot, snapshot, size))
                    ZFS.replicate(remote_dataset, prevsnap_name, snap_name, dataset, replicate_settings['endpoint'], direction='pull', compression=replicate_settings['compression'])
                    ZFS.hold(dataset, snap_name)
                    ZFS.hold(remote_dataset, snap_name, replicate_settings['endpoint'])
                    ZFS.release(dataset, prevsnap_name)
                    ZFS.release(remote_dataset, prevsnap_name, replicate_settings['endpoint'])
                    previous_snapshot = snapshot
                    result = PROC_CHANGED
        elif remote_dataset in remote_snapshots and len(remote_snapshots[remote_dataset]) > 0:
            # No common snapshot
            if len(local_snapshots) == 0:
                # No local snapshot, full replication
                snapshot = list(remote_snapshots[remote_dataset])[-1]
                snap_name = local_snapshots[snapshot]['name']
                size = ZFS.get_size(remote_dataset, None, snap_name, replicate_settings['endpoint'])
                log_info('  {0}@         > {0}@{1} ({2})'.format(remote_dataset, snapshot, size))
                ZFS.replicate(remote_dataset, None, snap_name, dataset, replicate_settings['endpoint'], direction='pull', compression=replicate_settings['compression'])
                ZFS.hold(dataset, snap_name)
                ZFS.hold(remote_dataset, snap_name, replicate_settings['endpoint'])
                result = PROC_CHANGED
        log_info('Replicating {0} complete'.format(dataset))
        return result

    @staticmethod
    def run(ds_settings, sleep_time):
        """
        Executes a single run where certain datasets might or might not be snapshotted
        """

        meter_time = MeterTime(sleep_time)
        now = int(time.time())
        this_time = time.strftime(SNAPSHOTNAME_FMTSPEC, time.localtime(now))

        snapshots = ZFS.get_snapshots()
        datasets = ZFS.get_datasets()
        is_connected = IsConnected()
        for dataset in datasets:
            if dataset in ds_settings:
                try:
                    dataset_settings = ds_settings[dataset]
                    local_snapshots = snapshots.get(dataset, OrderedDict())

                    take_snapshot = dataset_settings['snapshot'] is True
                    replicate = dataset_settings['replicate'] is not None

                    # Decide whether we need to handle this dataset
                    if not take_snapshot and not replicate:
                        continue
                    if dataset_settings['time'] == 'trigger':
                        # We wait until we find a trigger file in the filesystem
                        trigger_filename = '{0}/.trigger'.format(dataset_settings['mountpoint'])
                        if os.path.exists(trigger_filename):
                            log_info('Trigger found on {0}'.format(dataset))
                            os.remove(trigger_filename)
                        else:
                            continue
                    else:
                        if not meter_time.has_time_passed(dataset_settings['time'], now):
                            continue
                        log_info('Time passed for {0}'.format(dataset))


                    push = dataset_settings['replicate']['target'] is not None if replicate else True
                    if push:
                        # Pre exectution command
                        if dataset_settings['preexec'] is not None:
                            Helper.run_command(dataset_settings['preexec'], '/')

                        if (take_snapshot is True and this_time not in local_snapshots):
                            result = Manager.take_snapshot(dataset, local_snapshots, now)
                            # Execute postexec command
                            if result and dataset_settings['postexec'] is not None:
                                    Helper.run_command(dataset_settings['postexec'], '/')
                            if (result == PROC_CHANGED):
                                # Clean snapshots if one has been taken
                                Cleaner.clean(dataset, local_snapshots, dataset_settings['schema'])

                        # Replicating, if required
                        # If network replicating, check connectivity here
                        if (replicate is True and not is_connected.test_unconnected(dataset_settings)):
                            result = Manager.replicate_push_byparts(dataset, local_snapshots, dataset_settings)
                            # Post execution command
                            if (result and dataset_settings['replicate_postexec'] is not None):
                                Helper.run_command(dataset_settings['replicate_postexec'], '/')
                    else:
                        # Pre exectution command
                        if dataset_settings['preexec'] is not None:
                            Helper.run_command(dataset_settings['preexec'], '/')

                        # Replicating, if required
                        # If network replicating, check connectivity here
                        if (replicate is True and not is_connected.test_unconnected(dataset_settings)):
                            result = Manager.replicate_pull_byparts(dataset, local_snapshots, dataset_settings)
                            # Post execution command
                            if (result and dataset_settings['replicate_postexec'] is not None):
                                Helper.run_command(dataset_settings['replicate_postexec'], '/')
                            if (result == PROC_CHANGED):
                                # Clean snapshots if one has been taken
                                Cleaner.clean(dataset, local_snapshots, dataset_settings['schema'])

                except Exception as ex:
                    log_error('Exception: {0}'.format(str(ex)))

        # Clean up
        del is_connected

