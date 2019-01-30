zsnapd
======

ZFS Snapshot Daemon

A rework of ZFS Snapshot Manager by Kenneth Henderick <kenneth@ketronic.be>

ZFS dataset configuration file /etc/zfssnapmanager.cfg should be upwards compatible with /etc/zsnapd/dataset.conf.

Usage
-----

All the functional code is in the scripts folder.  Execute zsnapd, it deamonizes itself.

Features
--------

* Fully Python3 based
* Laptop friendly as has built in connectivity test to check remote port reachability
* Remote mode - snapshotting, script execution, and snapshot aging from central backup server.
* Native Systemd daemon compitability via py-magcode-core python daemon and logging support library
* Debug command line switch and stderr logging
* Systemd journalctl logging.
* Full standard Unix daemon support via py-magcode-core, with logging to syslog or logfile
* Configuration is stored in configuration files with the ini file format.  There is a template file, and a dataset file.
* Triggers the configured actions based on time or a '.trigger' file present in the dataset's mountpoint.
* Can take snapshots (with a yyyymmdd timestamp format)
* Can replicate snapshots to/from other nodes
  * Push based when the replication source has access to the replication target
  * Pull based when the replication source has no access to the replication target. Typically when you don't want to give
    all nodes access to the backup/replication target
* Full clone replication mode, to copy sub folders and all ZFS properties
* Cleans all snapshots with the yyyymmdd timestamp format based on a GFS schema (Grandfather, Father, Son).
* Supports pre and post commands
  * Pre command is executed before any action is executed
  * Post command is executed after the actions are executed, but before cleaning
* Has sshd remote execution filter script zsnapd-rcmd.  See etc/zsnapd/zsnapd-rcmd.conf for configuration.

Configuration
-------------

The daemon configuration file is in /etc/zsnapd/process.conf in ini format and a sample is as follows:

		[DEFAULT]
		run_as_user = root

		[zsnapd]
		# Use following setting to check daemon reconfiguring
		daemon_canary = blau
		debug_mark = True
		# Both below in seconds
		sleep_time = 300
		debug_sleep_time = 15
    # dataset configuration file
    # dataset_config_file = /etc/zsnapd/datasets.conf
    # dataset_config_file = /etc/zfssnapmanager.cfg
		# Uncomment to set up syslog logging
		# see pydoc3 syslog and man 3 syslog for value names with 'LOG_'
		# prefix stripped
		#syslog_facility = DAEMON
		#syslog_level = INFO
		# Uncomment to set up file logging
		#log_file = /var/log/zsnapd.log
		#log_file_max_size_kbytes = 1024
		#log_file_backup_count = 7

    [zsnapd-cfgtest]
    log_level = DEBUG

    [zsnapd-trigger]
    log_level = DEBUG

Adjust sleep_time (in seconds) to set interval zsnapd runs code.  For 30 minute intervals, set to
1800 seconds.

Command line arguments to zsnapd are:

		Usage: zsnapd [-dhv] [-c config_file]

			ZFS Snap Managment Daemon

			-c, --config-file       set configuration file
			-d, --debug             set debug level {0-3|none|normal|verbose|extreme}
			-h, --help              help message
			-b, --memory-debug      memory debug output
			-S, --systemd           Run as a systemd daemon, no fork
			-v, --verbose           verbose output
			-r, --rpdb2-wait        Wait for rpdb2 (seconds)

Note the default configuration file is /etc/zsnapd/process.conf, and systemd native mode is via the
--systemd switch

The dataset configuration file is located in /etc/zsnapd and is called dataset.conf. It's an .ini
file containing a section per dataset/volume that needs to be managed.  There is also a template .ini
file called template.conf in the same directory

zsnapd-cfgtest tests the data set conifugration file, and zsnapd-trigger writes out .trigger files
based on the data set configuration. It takes either the mount point of the target dataset as an argument, 
or the full dataset name including storage pool. zsnapd-trigger can optionally do a connectivity test first
before writing out the .trigger file. The same connectivty test is done before zsnapd attempts replication, and
it uses the replicate_endpoint_host and replicate_endpoint_port settings for the dataset. 

Examples

/etc/zsnapd/template.conf:

    [DEFAULT]
    replicate_endpoint_host = nas.local

    [backup-local]
    replicate_endpoint_port = 2345
    replicate_endpoint_command = ssh -l backup -p {port} {host}
    compression = gzip

    [backup-other]
    replicate_endpoint_host = other.remote.server.org

/etc/zsnapd/dataset.conf:
    [DEFAULT]
    time = trigger

    [zroot]
    template = backup-local
    mountpoint = /
    time = 21:00
    snapshot = True
    schema = 7d3w11m5y

    [zpool/data]
    mountpoint = /mnt/data
    time = trigger
    snapshot = True
    schema = 5d0w0m0y
    preexec = echo "starting" | mail somebody@example.com
    postexec = echo "finished" | mail somebody@exampe.com

    [zpool/myzvol]
    mountpoint = None
    time = 21:00
    snapshot = True
    schema = 7d3w0m0y

    [zpool/backups/data]
    template = backup-other
    mountpoint = /mnt/backups/data
    time = 23:00
    snapshot = False
    replicate_source = zpool/data
    schema = 7d3w11m4y

A summary of the different options:

* mountpoint: Points to the location to which the dataset is mounted, None for volumes
* time: Can be either a timestamp in 24h hh:mm notation after which a snapshot needs to be taken, or a comma separated list of such times. It can also be 'trigger' indicating that it will take a snapshot as soon as a file with name '.trigger' is found in the dataset's mountpoint. This can be used in case data is for example rsynced to the dataset.
* snapshot: Indicates whether a snapshot should be taken or not. It might be possible that only cleaning needs to be executed if this dataset is actually a replication target for another machine.
* replicate_endpoint: Deprecated. Can be left empty if replicating on localhost (e.g. copying snapshots to other pool). Should be omitted if no replication is required.
* replicate_endpoint_host: Deprecated. Can be left empty if replicating on localhost (e.g. copying snapshots to other pool). Should be omitted if no replication is required.
* replicate_endpoint_port: port that has to be remotely accessed
* replicate_endpoint_command: Command template for remote access. Takes two keys {port} and {host}
* replicate_target: The target to which the snapshots should be send. Should be omitted if no replication is required or a replication_source is specified.
* replicate_source: The source from which to pull the snapshots to receive onto the local dataset. Should be omitted if no replication is required or a replication_target is specified.
* compression: Indicates the compression program to pipe remote replicated snapshots through (for use in low-bandwidth setups.) The compression utility should accept standard compression flags (`-c` for standard output, `-d` for decompress.)
* schema: In case the snapshots should be cleaned, this is the schema the manager will use to clean.
* local_schema: For local snapshot cleaning/aging when dataset is receptical for remote source when snapshots are pulled
* local_schema: For remote snapshot cleaning/aging when remote target is receptical for backup when snapshots are pushed
* preexec: A command that will be executed, before snapshot/replication. Should be omitted if nothing should be executed
* postexec: A command that will be executed, after snapshot/replication,  but before the cleanup. Should be omitted if nothing should be executed
* clean_all: Clean/age all snapshots in dataset - default is False - ie zsnapd only
* local_clean_all: Setting for local dataset when replicating source is remote
* all_snapshots: Replicate all snapshots in dataset - Default is True - ie all snapshots in dataset
* log_commands: Per dataset log all commands executed for the dataset to DEBUG.  For checking what the program is doing exactly, helpful for auditing and security. 

Naming convention
-----------------

This script's snapshot will always given a timestamp (format yyyymmddhhmm) as name. For pool/tank an
example snapshot name could be pool/tank@201312311323.  The daemon is still compatible with the olderyyyymmdd 
snapshot aging convention, and will replicate and age them.  (Internally, the snapshot 'handles' are now created from
the snapshot creation time (using Unix timestamp seconds) - this means that manual snapshot names are covered too.)

All snapshots are currently used for replication (both snapshots taken by the script as well as snapshots taken by
other means (other scripts or manually), regardless of their name.

However, the script will only clean snapshots with the timestamp naming convention. In case you don't want a snapshot to
be cleaned by the script, just make sure it has any other name, not matching this convention.

Buckets
-------

The system will use "buckets" to apply the GFS schema.
From every bucket, the oldest snapshot will be kept. At any given time the script is
executed, it will place the snapshots in their buckets, and then clean out all buckets.

Bucket schema
-------------

For example, the schema '7d3w11m4y' means:

* 7 daily buckets (starting from today)
* 3 weekly buckets (7 days a week)
* 11 monthly buckets (30 days a month)
* 4 yearly buckets (12 * 30 days a year)

This wraps up to 5 years (where a year is 12 * 30 days - so not mapping to a real year)

Other schema's are possible. One could for example only be intrested in keeping only the
snapshots for last week, in which the schema '7d0w0m0y' would be given. Any combination is possible.

Since from each bucket, the oldest snapshot is kept, snapshots will seem to "roll"
trough the buckets.

The number of 'keep' days before aging can be given, as well as hourly buckets before the days kick in, 
ie '2k24h7d3w11m4y':

* 2 keep days starting from midnight, no snapshots deleted
* 24 hourly buckets starting from midnight in 2 days
* 7 daily buckets
* 3 weekly buckets (7 days a week)
* 11 monthly buckets (30 days a month)
* 4 yearly buckets (12 * 30 days a year)

Remote Execution Security
-------------------------

Sudo with a backup user on the remote was considered, but after reviewing the
sshd ForceCommand mechanism for remote excution, this was chosen as far easier
and superior. Thus, zsnapd-rcmd, the remote sshd command checker for ZFS
Snapshot Daemon

zsnapd-rcmd is the security plugin command for sshd that implements ForceCommand
functionality, or the command functionality in the .ssh/authorized_keys file
(See the sshd_config(8) and sshd(8) man pages respectively).

It executes commands from the SSH_ORIGINAL_COMMAND variable after checking
them against a list of configured regular expressions. 

Edit the zsnapd-rcmd.conf files in /etc/zsnapd, to set up the check regexps
for the remote preexec, postexec, and replicate_postexec commands.  Settings
are also available for 10 extra remote commands, labeled rcmd_aux0 - rcmd_aux9

Read the sshd(8) manpage on the ForceCommand setting, and the sshd(8) manpage
on the /root/.ssh/authorized_keys file, command entry for the remote pub key
for zsnapd access.

Example .ssh/authorized_keys entry (single line - unline wrap it):

no-pty,no-agent-forwarding,no-X11-forwarding,no-port-forwarding,
command="/usr/sbin/zsnapd-rcmd" ssh-rsa AAAABBBBBBBBCCCCCCCCCCCCC
DDDDDDDD== root@blah.org

Hint: command line arguments can be given, such as a different config file,
and debug level:

no-pty,no-agent-forwarding,no-X11-forwarding,no-port-forwarding,
command="/usr/sbin/zsnapd-rcmd -c /etc/zsnapd/my-rcmd.conf --debug 3" 
ssh-rsa AAAABBBBBBBBCCCCCCCCCCCCCDDDDDDDD== root@blah.org

Examples
--------

The examples directory contains 3 example configuration files, almost identical as my own 'production' setup.

* A non-ZFS device (router), rsyncing its filesystem to an NFS shared dataset.
* A laptop, having a single root ZFS setup, containing 2 normal filesystems and a ZFS dataset
* A local NAS with lots of data and the replication target of most systems
* A remote NAS (used as normal NAS by these people) used with two-way replication as offsite backup setup.

Dependencies
------------

This python program/script has a few dependencies. When using the Archlinux AUR, these will be installed automatically.

* zfs
* python3
* openssh
* mbuffer
* python3-magcode-core >= 1.5.4 - on pypi.org
* python3-psutil
* python3-setproctitle

Logging
-------

The script is logging into systemd journals, and /var/log/syslog

License
-------

This program/script is licensed under MIT, which basically means you can do anything you want with it. You can find
the license text in the 'LICENSE' file.

If you like the software or if you're using it, feel free to leave a star as a toke of appreciation.

Warning
-------

As with any script deleting snapshots, use with caution. Make sure to test the script on
a dummy dataset first when you use it directly from the repo. This to ensure no unexpected things will happen.

The releases should be working fine, as I use these on my own environment, and for customers.

In case you find a bug, feel free to create a bugreport and/or fork and send a pull-request
in case you fixed the bug yourself.

Packages
--------

ZFS Snapshot Manager is available in the following distributions:

* ArchLinux: https://aur.archlinux.org/packages/zfs-snap-manager (AUR)
  * The PKGBUILD and install scripts are now available through the AUR git repo

zsnapd is available in following distributions:

* Debian: http://packages.debian.org as part of the main repostitory
* Ubuntu (eventually)

ZFS
---

From Wikipedia:

    ZFS is a combined file system and logical volume manager designed by Sun Microsystems.
    The features of ZFS include protection against data corruption, support for high storage capacities,
    efficient data compression, integration of the concepts of filesystem and volume management, snapshots
    and copy-on-write clones, continuous integrity checking and automatic repair, RAID-Z and native NFSv4 ACLs.

    ZFS was originally implemented as open-source software, licensed under the Common Development and
    Distribution License (CDDL). The ZFS name is registered as a trademark of Oracle Corporation.

ZFS Snapshot Manager and zsnapd are standalone projects, and is not affiliated with ZFS or Oracle Corporation.
