# redis-collectd-plugin - redis_info.py
#
# This program is free software; you can redistribute it and/or modify it
# under the terms of the GNU General Public License as published by the
# Free Software Foundation; only version 2 of the License is applicable.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin St, Fifth Floor, Boston, MA  02110-1301 USA
#
# Authors:
#   Garret Heaton <powdahound at gmail.com>
# Contributors:
#   Pierre Mavro <p.mavro@criteo.com> / Deimosfr <deimos at deimos.fr>
#   https://github.com/powdahound/redis-collectd-plugin/graphs/contributors
#
# About this plugin:
#   This plugin uses collectd's Python plugin to record Redis information.
#
# collectd:
#   http://collectd.org
# Redis:
#   http://redis.googlecode.com
# collectd-python:
#   http://collectd.org/documentation/manpages/collectd-python.5.shtml

import collectd
import socket
import re

# Verbose logging on/off. Override in config by specifying 'Verbose'.
VERBOSE_LOGGING = False

CONFIGS = []
REDIS_INFO = {}


def fetch_info(conf):
    """Connect to Redis server and request info"""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((conf['host'], conf['port']))
        log_verbose('Connected to Redis at %s:%s' % (conf['host'],
                                                     conf['port']))
    except socket.error, e:
        collectd.error('redis_info plugin: Error connecting to %s:%d - %r'
                       % (conf['host'], conf['port'], e))
        return None

    fp = s.makefile('r')

    if conf['auth'] is not None:
        log_verbose('Sending auth command')
        s.sendall('auth %s\r\n' % (conf['auth']))

        status_line = fp.readline()
        if not status_line.startswith('+OK'):
            # -ERR invalid password
            # -ERR Client sent AUTH, but no password is set
            collectd.error(
                'redis_info plugin: Error sending auth to %s:%d - %r'
                % (conf['host'], conf['port'], status_line))
            return None

    log_verbose('Sending info command')
    s.sendall('info\r\n')
    status_line = fp.readline()

    if status_line.startswith('-'):
        collectd.error('redis_info plugin: Error response from %s:%d - %r'
                       % (conf['host'], conf['port'], status_line))
        s.close()
        return None

    # status_line looks like: $<content_length>
    content_length = int(status_line[1:-1])
    data = fp.read(content_length)
    log_verbose('Received data: %s' % data)

    linesep = '\r\n' if '\r\n' in data else '\n'
    info_dict = parse_info(data.split(linesep))
    fp.close()

    # monitoring lengths of custom keys
    key_dict = get_llen_keys(s, conf)
    info_dict.update(key_dict)

    s.close()

    return info_dict


def get_llen_keys(socket, conf):
    """
    for each llen_key specified in the config file,
    grab the length of that key from the corresponding
    database index. return a dictionary of the
    keys
    """
    llen_fp = socket.makefile('r')
    key_dict = {}
    if len(conf['llen_keys']) > 0:
        for db, keys in conf['llen_keys'].items():
            socket.sendall('select  %d\r\n' % db)
            status_line = llen_fp.readline()  # +OK
            for key in keys:
                socket.sendall('llen %s\r\n' % key)
                status_line = llen_fp.readline()  # :VALUE
                try:
                    val = int(filter(str.isdigit, status_line))
                except ValueError:
                    collectd.warning('redis_info plugin: key %s is not of type list, cannot get length' % key)

                subkey = "db{0}_llen_{1}".format(db, key)
                key_dict.update({subkey: val})

    llen_fp.close()
    return key_dict


def parse_info(info_lines):
    """Parse info response from Redis"""
    info = {}
    for line in info_lines:
        if "" == line or line.startswith('#'):
            continue

        if ':' not in line:
            collectd.warning('redis_info plugin: Bad format for info line: %s'
                             % line)
            continue

        key, val = line.split(':')

        # Handle multi-value keys (for dbs and slaves).
        # db lines look like "db0:keys=10,expire=0"
        # slave lines look like
        # "slave0:ip=192.168.0.181,port=6379,
        #  state=online,offset=1650991674247,lag=1"
        if ',' in val:
            split_val = val.split(',')
            for sub_val in split_val:
                k, _, v = sub_val.rpartition('=')
                sub_key = "{0}_{1}".format(key, k)
                info[sub_key] = v
        else:
            info[key] = val

    # compatibility with pre-2.6 redis (used changes_since_last_save)
    info["changes_since_last_save"] = info.get("changes_since_last_save",
                                               info.get(
                                                "rdb_changes_since_last_save"))

    return info


def configure_callback(conf):
    """Receive configuration block"""
    host = None
    port = None
    auth = None
    instance = None
    llen_keys = {}

    for node in conf.children:
        key = node.key.lower()
        val = node.values[0]
        log_verbose('Analyzing config %s key (value: %s)' % (key, val))
        searchObj = re.search(r'redis_(.*)$', key, re.M | re.I)

        if key == 'host':
            host = val
        elif key == 'port':
            port = int(val)
        elif key == 'auth':
            auth = val
        elif key == 'verbose':
            global VERBOSE_LOGGING
            VERBOSE_LOGGING = bool(node.values[0]) or VERBOSE_LOGGING
        elif key == 'instance':
            instance = val
        elif key == 'llen_key':
            if (len(node.values)) == 2:
                llen_keys.setdefault(node.values[0], []).append(node.values[1])
            else:
                collectd.warning("redis_info plugin: monitoring length of keys requires both \
                                    database index and key value")

        elif searchObj:
            global REDIS_INFO
            log_verbose('Matching expression found: key: %s - value: %s' %
                        (searchObj.group(1), val))
            REDIS_INFO[searchObj.group(1), val] = True
        else:
            collectd.warning('redis_info plugin: Unknown config key: %s.' %
                             key)
            continue

    log_verbose(
        'Configured with host=%s, port=%s, instance name=%s, using_auth=%s'
        % (host, port, instance, (auth is not None)))

    CONFIGS.append({'host': host,
                    'port': port,
                    'auth': auth,
                    'instance': instance,
                    'llen_keys': llen_keys})


def dispatch_value(info, key, type, plugin_instance=None, type_instance=None):
    """Read a key from info response data and dispatch a value"""

    if key not in info:
        collectd.warning('redis_info plugin: Info key not found: %s' % key)
        return

    if plugin_instance is None:
        plugin_instance = 'unknown redis'
        collectd.error(
            'redis_info plugin: plugin_instance is not set, Info key: %s' %
            key)

    if not type_instance:
        type_instance = key

    try:
        value = int(info[key])
    except ValueError:
        value = float(info[key])

    log_verbose('Sending value: %s=%s' % (type_instance, value))

    val = collectd.Values(plugin='redis_info')
    val.type = type
    val.type_instance = type_instance
    val.plugin_instance = plugin_instance
    val.values = [value]

    # With some versions of CollectD, a dummy metadata map must be added
    # to each value for it to be correctly serialized to JSON by the
    # write_http plugin. See
    # https://github.com/collectd/collectd/issues/716
    val.meta = {'0': True}
    val.dispatch()


def _format_dimensions(dimensions):
    """
    Formats a dictionary of dimensions to a format that enables them to be
    specified as key, value pairs in plugin_instance to signalfx. E.g.
    >>> dimensions = {'a': 'foo', 'b': 'bar'}
    >>> _format_dimensions(dimensions)
    "[a=foo,b=bar]"
    Args:
    dimensions (dict): Mapping of {dimension_name: value, ...}
    Returns:
    str: Comma-separated list of dimensions
    """

    dim_pairs = ["%s=%s" % (k, v) for k, v in dimensions.iteritems()]
    return "[%s]" % (",".join(dim_pairs))


def read_callback():
    for conf in CONFIGS:
        get_metrics(conf)


def get_metrics(conf):
    info = fetch_info(conf)

    if not info:
        collectd.error('redis plugin: No info received')
        return

    plugin_instance = conf['instance']
    if plugin_instance is None:
        plugin_instance = '{host}:{port}'.format(host=conf['host'],
                                                 port=conf['port'])

    for keyTuple, val in REDIS_INFO.iteritems():
        key, val = keyTuple

        if key == 'total_connections_received' and val == 'counter':
            dispatch_value(info,
                           'total_connections_received',
                           'counter',
                           plugin_instance,
                           'connections_received')
        elif key == 'total_commands_processed' and val == 'counter':
            dispatch_value(info,
                           'total_commands_processed',
                           'counter',
                           plugin_instance,
                           'commands_processed')
        else:
            dispatch_value(info, key, val, plugin_instance)

    for db, keys in conf['llen_keys'].items():
        for key in keys:
            subkey = "db{0}_llen_{1}".format(db, key)
            val = info.get(subkey)
            dimensions = _format_dimensions({'key_name': key, 'db_index': db})
            plugin_dimensions = "{0}{1}".format(plugin_instance, dimensions)
            dispatch_value(info, subkey, 'gauge', plugin_dimensions, 'key_llen')


def log_verbose(msg):
    if not VERBOSE_LOGGING:
        return
    collectd.info('redis plugin [verbose]: %s' % msg)


# register callbacks
collectd.register_config(configure_callback)
collectd.register_read(read_callback)
