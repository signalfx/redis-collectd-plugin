redis-collectd-plugin
=====================

A [Redis](http://redis.io) plugin for [collectd](http://collectd.org) using collectd's [Python plugin](http://collectd.org/documentation/manpages/collectd-python.5.shtml).

You can capture any kind of Redis metrics like:

 * Memory used
 * Commands processed per second
 * Number of connected clients and slaves
 * Number of blocked clients
 * Number of keys stored (per database)
 * Uptime
 * Changes since last save
 * Replication delay (per slave)
 * The length of list values


Installation and Configuration
------------

Please refer to the documentation located in the [SignalFx Integrations repo](https://github.com/signalfx/integrations/tree/release/collectd-redis) for information on the installation and configuration of this plugin.


Requirements
------------
 * collectd 4.9+
