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

import configparser
import time
import os
import re
from datetime import datetime

from magcode.core.globals_ import *
from magcode.core.utility import MagCodeConfigError

from scripts.zfs import ZFS
from scripts.clean import Cleaner
from scripts.clean import CLEANER_REGEX
from scripts.helper import Helper

ds_name_syntax = r'^[-_:.a-zA-Z0-9][-_:./a-zA-Z0-9]*$'
ds_name_reserved_regex = r'^(c[0-9]|log/|mirror|raidz|raidz1|raidz2|raidz3|spare).*$'
BOOLEAN_REGEX = r'^([tT]rue|[fF]alse|[oO]n|[oO]ff|0|1)$'
PATH_REGEX = r'[-_./~a-zA-Z0-9]+'
NETCMD_REGEX = r'^[-_./~a-zA-Z0-9 	]*$'
SHELLCMD_REGEX = r'^[-_./~a-zA-Z0-9 	:@|]+$'
ds_syntax_dict = {'snapshot': BOOLEAN_REGEX,
        'replicate': BOOLEAN_REGEX,
        'time': r'^(trigger|[0-9]{1,2}:[0-9][0-9])$',
        'mountpoint': r'^(None|/|/' + PATH_REGEX + r')$',
        'preexec': SHELLCMD_REGEX,
        'postexec': SHELLCMD_REGEX,
        'replicate_target': ds_name_syntax,
        'replicate_source': ds_name_syntax,
        'replicate_endpoint': NETCMD_REGEX,
        'compression': BOOLEAN_REGEX,
        'schema': CLEANER_REGEX,
        }

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

    @staticmethod
    def check_dataset_syntax (config):
        """
        Checks the dataset syntax of read in items
        """
        result = True
        for dataset in config.sections():
            if (not re.match(ds_name_syntax, dataset)
                    or re.match(ds_name_reserved_regex, dataset)):
                log_error("Dataset name '{0}' is invalid.".format(dataset))
                result = False
            for item in config[dataset].keys():
                try:
                    value_syntax = ds_syntax_dict[item]
                except AttributeError as ex:
                    log_error("[{0}] - item '{1}' is not a valid dataset keyword.".format(dataset, item))
                    result = False
                if (not ds_syntax_dict[item]):
                    continue
                value = config[dataset][item]
                if (not re.match(ds_syntax_dict[item], value)):
                    log_error("[{0}] {1} - value '{2}' invalid. Must match regex '{3}'.".format(dataset, item, value, ds_syntax_dict[item]))
                    result = False
                if item in ('replicate_source', 'replicate_target'):
                    if re.match(ds_name_reserved_regex, value):
                        log_error("[{0}] {1} - value '{2}' invalid. Must not start with a ZFS reserved keyword.".format(dataset, item, value))
                        result = False
        return result

    @staticmethod
    def read_ds_config ():
        """
        Read dataset configuration
        """
        ds_settings = {}
        try:
            config = configparser.RawConfigParser()
            config.read(settings['dataset_config_file'])
            if not Manager.check_dataset_syntax(config):
                raise MagCodeConfigError("Invalid dataset syntax in config file '{0}'".format(settings['dataset_config_file']))
            for dataset in config.sections():
                ds_settings[dataset] = {'mountpoint': config.get(dataset, 'mountpoint') \
                                            if config.has_option(dataset, 'mountpoint') else None,
                                     'time': config.get(dataset, 'time'),
                                     'snapshot': config.getboolean(dataset, 'snapshot'),
                                     'replicate': None,
                                     'schema': config.get(dataset, 'schema'),
                                     'preexec': config.get(dataset, 'preexec') if config.has_option(dataset, 'preexec') else None,
                                     'postexec': config.get(dataset, 'postexec') if config.has_option(dataset, 'postexec') else None}
                if config.has_option(dataset, 'replicate_endpoint') and (config.has_option(dataset, 'replicate_target') or
                                                                         config.has_option(dataset, 'replicate_source')):
                    ds_settings[dataset]['replicate'] = {'endpoint': config.get(dataset, 'replicate_endpoint'),
                                                      'target': config.get(dataset, 'replicate_target')
                                                      if config.has_option(dataset, 'replicate_target') else None,
                                                      'source': config.get(dataset, 'replicate_source')
                                                      if config.has_option(dataset, 'replicate_source') else None,
                                                      'compression': config.get(dataset, 'compression')
                                                      if config.has_option(dataset, 'compression') else None}
       # Handle file opening and read errors
        except (IOError,OSError) as e:
            log_error('Exception while parsing configuration file: {0}'.format(str(e)))
            if (e.errno == errno.EPERM or e.errno == errno.EACCES):
                systemd_exit(os.EX_NOPERM, SDEX_NOPERM)
            else:
                systemd_exit(os.EX_IOERR, SDEX_GENERIC)

        # Handle all configuration file parsing errors
        except configparser.Error as e:
            log_error('Exception while parsing configuration file: {0}'.format(str(e)))
            systemd_exit(os.EX_CONFIG, SDEX_CONFIG)
        
        except MagCodeConfigError as e:
            log_error(str(e))
            systemd_exit(os.EX_CONFIG, SDEX_CONFIG)

        return ds_settings

