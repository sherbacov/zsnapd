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
Processes and reads in configuration
"""

import os
import sys
import re
import errno
import configparser

from magcode.core.globals_ import *
from magcode.core.utility import MagCodeConfigError

from scripts.clean import CLEANER_REGEX

ds_name_syntax = r'^[-_:.a-zA-Z0-9][-_:./a-zA-Z0-9]*$'
ds_name_reserved_regex = r'^(log|DEFAULT|(c[0-9]|log/|mirror|raidz|raidz1|raidz2|raidz3|spare).*)$'
template_name_syntax = r'^[a-zA-Z0-9][-_:.a-zA-Z0-9]*$'
BOOLEAN_REGEX = r'^([tT]rue|[fF]alse|[oO]n|[oO]ff|0|1)$'
PATH_REGEX = r'[-_./~a-zA-Z0-9]+'
SHELLCMD_REGEX = r'^[-_./~a-zA-Z0-9 	:@|]+$'
SHELLFORMAT_REGEX = r'^[-_./~a-zA-Z0-9 	:@|{}]+$'
NETCMD_REGEX = r'^[-_./~a-zA-Z0-9 	:@|]*$'
HOST_REGEX = r'^[0-9a-zA-Z][-_.a-zA-Z0-9]*$'
PORT_REGEX = r'^[0-9]{1,5}$'
USER_REGEX = r'^[a-zA-Z][-_.a-zA-Z0-9]*$'
ds_syntax_dict = {'snapshot': BOOLEAN_REGEX,
        'replicate': BOOLEAN_REGEX,
        'time': r'^(trigger|[0-9]{1,2}:[0-9][0-9])$',
        'mountpoint': r'^(None|/|/' + PATH_REGEX + r')$',
        'preexec': SHELLCMD_REGEX,
        'postexec': SHELLCMD_REGEX,
        'replicate_target': ds_name_syntax,
        'replicate_source': ds_name_syntax,
        'replicate_endpoint': NETCMD_REGEX,
        'replicate_endpoint_host': HOST_REGEX,
        'replicate_endpoint_port': PORT_REGEX,
        'replicate_endpoint_user': USER_REGEX,
        'replicate_endpoint_command': PATH_REGEX,
        'replicate_endpoint_extra_args': SHELLCMD_REGEX,
        'replicate_endpoint_template': SHELLFORMAT_REGEX,
        'replicate_use_sudo': BOOLEAN_REGEX,
        'compression': PATH_REGEX,
        'schema': CLEANER_REGEX,
        'template': template_name_syntax,
        }
DEFAULT_ENDPOINT_PORT = 22
DEFAULT_ENDPOINT_USER = 'root'
DEFAULT_ENDPOINT_CMD = 'ssh'
DEFAULT_ENDPOINT_TEMPLATE = '{command} {extra_args} -p {port} -l {user} {host}'
DEFAULT_USE_SUDO = False


class Config(object):
    @staticmethod
    def _check_section_syntax(section, section_name):
        result = True
        for item in section.keys():
            try:
                value_syntax = ds_syntax_dict[item]
            except KeyError as ex:
                log_error("[{0}] - item '{1}' is not a valid dataset keyword.".format(section_name, item))
                result = False
                continue
            if (not ds_syntax_dict[item]):
                continue
            value = section[item]
            if (not re.match(ds_syntax_dict[item], value)):
                log_error("[{0}] {1} - value '{2}' invalid. Must match regex '{3}'.".format(section_name, item, value, ds_syntax_dict[item]))
                result = False
            if item in ('replicate_source', 'replicate_target'):
                if re.match(ds_name_reserved_regex, value):
                    log_error("[{0}] {1} - value '{2}' invalid. Must not start with a ZFS reserved keyword.".format(section_name, item, value))
                    result = False
        return result
    
    @staticmethod
    def _check_template_syntax(template_config):
        """
        Checks the syntax of the template file
        """
        result = True
        # Check syntax of DEFAULT section
        if not Config._check_section_syntax(template_config.defaults(), 'DEFAULT'):
            result = False
        for dsg in template_config.sections():
            # Check name syntax of each dataset group
            if not re.match(template_name_syntax, dsg):
                result = False
            # Check syntax of each dataset group 
            if not Config._check_section_syntax(template_config[dsg], dsg):
                result = False
        return result

    @staticmethod
    def _check_dataset_syntax (ds_config):
        """
        Checks the dataset syntax of read in items
        """
        result = True
        for dataset in ds_config.sections():
            if (not re.match(ds_name_syntax, dataset)
                    or re.match(ds_name_reserved_regex, dataset)):
                log_error("Dataset name '{0}' is invalid.".format(dataset))
                result = False
            if not Config._check_section_syntax(ds_config[dataset], dataset):
                result = False
            if (ds_config.has_option(dataset, 'replicate_endpoint_host') or ds_config.has_option(dataset, 'replicate_endpoint')):
                if (not ds_config.has_option(dataset, 'replicate_target') and not ds_config.has_option('replicate_source')):
                    log_error("Dataset '{0}' is configured for replication but no replicate_source or replicate_target is specified.".format(dataset))
                    result = False
        return result

    @staticmethod
    def read_ds_config ():
        """
        Read dataset configuration
        """
        ds_settings = {}
        ds_dict = {}
        template_dict = {}
        try:
            
            template_filename = settings['template_config_file']
            template_file = open(template_filename)
            template_config = configparser.ConfigParser()
            template_config.read_file(template_file)
            template_file.close()
            if not Config._check_template_syntax(template_config):
                raise MagCodeConfigError("Invalid dataset syntax in config file '{0}'".format(template_filename))

            def get_sect_dict(config, section):
                res_dict = {}
                for item in config[section]:
                    if item in ('snapshot', ):
                        res_dict[item] = config.getboolean(section, item)
                    else:
                        res_dict[item] = config.get(section, item)
                return res_dict

            template_dict = {template_section:get_sect_dict(template_config, template_section) for template_section in template_config.sections()}
            
            ds_filename = settings['dataset_config_file']
            ds_file = open(ds_filename)
            ds_config = configparser.ConfigParser()
            ds_config.read_file(ds_file)
            ds_file.close()
            if not Config._check_dataset_syntax(ds_config):
                raise MagCodeConfigError("Invalid dataset syntax in config file '{0}'".format(ds_filename))

            # Assemble default ds_dict
            ds_dict = {}
            for ds in ds_config.sections():
                ds_group = ds_config.get(ds, 'template', fallback=None)
                if (ds_group and ds_group in template_dict):
                    ds_dict[ds] = template_dict.get(ds_group, None)

            # Destroy ds_config and re read it
            del ds_config
            ds_file = open(ds_filename)
            ds_config = configparser.ConfigParser()
            # Add dataset group defaults
            ds_config.read_dict(ds_dict)
            ds_config.read_file(ds_file)
            ds_file.close()

            for dataset in ds_config.sections():
                ds_settings[dataset] = {'mountpoint': ds_config.get(dataset, 'mountpoint', fallback=None),
                                     'time': ds_config.get(dataset, 'time'),
                                     'snapshot': ds_config.getboolean(dataset, 'snapshot'),
                                     'replicate': None,
                                     'schema': ds_config.get(dataset, 'schema'),
                                     'preexec': ds_config.get(dataset, 'preexec', fallback=None),
                                     'postexec': ds_config.get(dataset, 'postexec', fallback=None)}
                if ((ds_config.has_option(dataset, 'replicate_endpoint_host') or ds_config.has_option(dataset, 'replicate_endpoint'))
                        and (ds_config.has_option(dataset, 'replicate_target') or ds_config.has_option(dataset, 'replicate_source'))):
                    if ds_config.has_option(dataset, 'replicate_endpoint_host'):
                        template = ds_config.get(dataset, 'replicate_endpoint_template', fallback=DEFAULT_ENDPOINT_TEMPLATE)
                        command = ds_config.get(dataset, 'replicate_endpoint_command', fallback=DEFAULT_ENDPOINT_CMD)
                        extra_args = ds_config.get(dataset, 'replicate_endpoint_extra_args', fallback='')
                        host = ds_config.get(dataset, 'replicate_endpoint_host')
                        port = ds_config.get(dataset, 'replicate_endpoint_port', fallback=DEFAULT_ENDPOINT_PORT)
                        user = ds_config.get(dataset, 'replicate_endpoint_user', fallback=DEFAULT_ENDPOINT_USER)
                        if host:
                            endpoint = template.format(command=command, extra_args=extra_args, port=port, user=user, host=host)
                        else:
                            endpoint = ''
                    else:
                        endpoint = ds_config.get(dataset, 'replicate_endpoint')
                    ds_settings[dataset]['replicate'] = {'endpoint': endpoint,
                                                      'target': ds_config.get(dataset, 'replicate_target', fallback=None),
                                                      'source': ds_config.get(dataset, 'replicate_source', fallback=None),
                                                      'compression': ds_config.get(dataset, 'compression', fallback=None)}
       
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

