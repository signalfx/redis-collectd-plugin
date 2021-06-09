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

import re
import socket

import collectd
from redis_client import RedisClient, RedisError


class RedisCollector:
    def __init__(self, host, port, auth, instance, metric_types, verbose, llen_keys):
        self.host = host
        self.port = port
        self.auth = auth
        self.instance = instance
        self.metric_types = metric_types
        self.verbose = verbose
        self.llen_keys = llen_keys

    def fetch_info(self, client):
        """Request info from the Redis server"""
        self.log_verbose("Sending info command")
        client.send("info")
        try:
            data = client.read_response()
        except RedisError as e:
            collectd.error("redis_info plugin: Error response from %s:%d - %r" % (self.host, self.port, e))
            return None

        self.log_verbose("Received data: %s" % data)

        linesep = "\r\n" if "\r\n" in data else "\n"
        info_dict = self.parse_info(data.split(linesep))

        return info_dict

    def parse_info(self, info_lines):
        """Parse info response from Redis"""
        info = {}
        for line in info_lines:
            if not line or line.startswith("#"):
                continue

            if ":" not in line:
                collectd.warning("redis_info plugin: Bad format for info line: %s" % line)
                continue

            key, val = line.split(":", 1)

            # Handle multi-value keys (for dbs and slaves).
            # db lines look like "db0:keys=10,expire=0"
            # slave lines look like
            # "slave0:ip=192.168.0.181,port=6379,
            #  state=online,offset=1650991674247,lag=1"
            if "," in val:
                split_val = val.split(",")
                for sub_val in split_val:
                    k, _, v = sub_val.rpartition("=")
                    sub_key = "{0}_{1}".format(key, k)
                    info[sub_key] = v
            else:
                info[key] = val

        # compatibility with pre-2.6 redis (used changes_since_last_save)
        info["changes_since_last_save"] = info.get("changes_since_last_save", info.get("rdb_changes_since_last_save"))

        return info

    def dispatch_info(self, client):
        info = self.fetch_info(client)

        if not info:
            collectd.error("redis plugin: No info received")
            return

        for keyTuple, val in self.metric_types.items():
            key, mtype = keyTuple
            if key == "total_connections_received" and mtype == "counter":
                self.dispatch_value(info["total_connections_received"], "counter", type_instance="connections_received")
            elif key == "total_commands_processed" and mtype == "counter":
                self.dispatch_value(info["total_commands_processed"], "counter", type_instance="commands_processed")
            else:
                try:
                    self.dispatch_value(info[key], mtype, type_instance=key)
                except (KeyError, TypeError):
                    collectd.error("Metric %s not found in Redis INFO output" % key)
                    continue

    def dispatch_list_lengths(self, client):
        """
        For each llen_key specified in the config file,
        grab the length of that key from the corresponding
        database index.
        """
        key_dict = {}
        for db, patterns in list(self.llen_keys.items()):
            client.send("select %d" % db)
            try:
                resp = client.read_response()
            except RedisError as e:
                collectd.error("Could not select Redis db %s: %s" % (db, e))
                continue

            for pattern in patterns:
                keys = []
                # If there is a glob, get every key matching it
                if "*" in pattern:
                    client.send("KEYS %s" % pattern)
                    keys = client.read_response()
                else:
                    keys = [pattern]

                for key in keys:
                    self.fetch_and_dispatch_llen_for_key(client, key, db)

    def fetch_and_dispatch_llen_for_key(self, client, key, db):
        client.send("llen %s" % key)
        try:
            val = client.read_response()
        except RedisError as e:
            collectd.warning("redis_info plugin: could not get length of key %s in db %s: %s" % (key, db, e))
            return

        dimensions = {"key_name": key, "db_index": db}
        self.dispatch_value(val, "gauge", type_instance="key_llen", dimensions=dimensions)

    def dispatch_value(self, value, type, plugin_instance=None, type_instance=None, dimensions={}):
        """Read a key from info response data and dispatch a value"""

        try:
            value = int(value)
        except ValueError:
            try: 
                value = float(value)
            except ValueError:
                if type_instance == "master_link_status":
                    if value == "up":
                        value = 1
                    else:
                        value = 0
                else:
                    collectd.warning("redis_info plugin: could not cast value %s for instance %s " % (value, type_instance))
                    return

        self.log_verbose("Sending value: %s=%s (%s)" % (type_instance, value, dimensions))

        val = collectd.Values(plugin="redis_info")
        val.type = type
        val.type_instance = type_instance
        val.values = [value]

        plugin_instance = self.instance
        if plugin_instance is None:
            plugin_instance = "{host}:{port}".format(host=self.host, port=self.port)

        val.plugin_instance = "{0}{1}".format(plugin_instance, _format_dimensions(dimensions))

        # With some versions of CollectD, a dummy metadata map must be added
        # to each value for it to be correctly serialized to JSON by the
        # write_http plugin. See
        # https://github.com/collectd/collectd/issues/716
        val.meta = {"0": True}
        val.dispatch()

    def read_callback(self):
        try:
            self.get_metrics()
        except socket.error as e:
            collectd.error("redis_info plugin: Error connecting to %s:%d - %r" % (self.host, self.port, e))
            return
        except RedisError as e:
            collectd.error("redis_info plugin: Error getting metrics from %s:%d - %r" % (self.host, self.port, e))
            return

    def get_metrics(self):
        with RedisClient(self.host, self.port, self.auth) as client:
            self.log_verbose("Connected to Redis at %s:%s" % (self.host, self.port))

            self.dispatch_info(client)
            self.dispatch_list_lengths(client)

    def log_verbose(self, msg):
        if not self.verbose:
            return
        collectd.info("redis plugin [verbose]: %s" % msg)


def configure_callback(conf):
    """Receive configuration block"""
    host = None
    port = None
    auth = None
    instance = None
    llen_keys = {}
    metric_types = {}
    verbose = False

    for node in conf.children:
        key = node.key.lower()
        val = node.values[0]
        searchObj = re.search(r"redis_(.*)$", key, re.M | re.I)

        if key == "host":
            host = val
        elif key == "port":
            port = int(val)
        elif key == "auth":
            auth = val
        elif key == "verbose":
            verbose = node.values[0]
        elif key == "instance":
            instance = val
        elif key == "sendlistlength":
            if (len(node.values)) == 2:
                llen_keys.setdefault(int(node.values[0]), []).append(node.values[1])
            else:
                collectd.warning(
                    "redis_info plugin: monitoring length of keys requires both \
                                    database index and key value"
                )

        elif searchObj:
            metric_types[searchObj.group(1), val] = True
        else:
            collectd.warning("redis_info plugin: Unknown config key: %s." % key)
            continue

    if verbose:
        collectd.info(
            "Configured with host=%s, port=%s, instance name=%s, using_auth=%s, llen_keys=%s"
            % (host, port, instance, (auth is not None), llen_keys)
        )

    collector = RedisCollector(
        **{
            "host": host,
            "port": port,
            "auth": auth,
            "instance": instance,
            "metric_types": metric_types,
            "verbose": verbose,
            "llen_keys": llen_keys,
        }
    )

    collectd.register_read(collector.read_callback, name="%s:%s:%s" % (host, port, instance))


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
    if not dimensions:
        return ""

    dim_pairs = ["%s=%s" % (k, v) for k, v in dimensions.items()]
    return "[%s]" % (",".join(dim_pairs))


# register callbacks
collectd.register_config(configure_callback)
