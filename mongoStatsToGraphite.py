import commands
import time
import sys
from socket import socket

import argparse
import os
from pymongo import Connection
import yaml


class MongoDBGraphiteMonitor(object):
  CONFIG_PATH = '/etc/mongodb-monitoring.conf'

  def __init__(self):
    self._thisHost = commands.getoutput('hostname')
    self._args = self._parseCommandLineArgs(self._setDefaults(self._parseConfigFile()))

  def _parseConfigFile(self):
    if os.path.exists(self.CONFIG_PATH):
      with open(self.CONFIG_PATH, 'r') as configStream:
        config = yaml.load(configStream)
        if isinstance(config['database'], str):
          config['database'] = [config['database']]
        return config

    return dict()

  def _setDefaults(self, loadedConfig):
    if not 'host' in loadedConfig:
      loadedConfig['host'] = self._thisHost
    if not 'prefix' in loadedConfig:
      loadedConfig['prefix'] = 'DEV'
    if not 'service' in loadedConfig:
      loadedConfig['service'] = 'unspecified'
    if not 'graphitePort' in loadedConfig:
      loadedConfig['graphitePort'] = 2003
    if not 'database' in loadedConfig:
      loadedConfig['database'] = None

    return loadedConfig

  def _parseCommandLineArgs(self, defaultConfig):
    parser = argparse.ArgumentParser(
      description='Creates graphite metrics for a single mongodb instance from administation commands.')
    parser.add_argument('-host', default=defaultConfig['host'],
                        help='host name of mongodb to create metrics from.')
    parser.add_argument('-prefix', default=defaultConfig['prefix'],
                        help='prefix for all metrics.')
    parser.add_argument('-service', default=defaultConfig['service'],
                        help='service name the metrics should appear under.')
    parser.add_argument('-database', default=defaultConfig['database'], type=str, nargs='*',
                        help='database name(s), space separated, for additional metric of this database')
    parser.add_argument('-graphiteHost',
                        default=defaultConfig['graphiteHost'] if 'graphiteHost' in defaultConfig else None,
                        required=not 'graphiteHost' in defaultConfig,
                        help='host name for graphite server.')
    parser.add_argument('-graphitePort', default=defaultConfig['graphitePort'],
                        help='port garphite is listening on.')
    parser.add_argument('-username', default=defaultConfig['username'] if 'username' in defaultConfig else None,
                        help='mongodb login username')
    parser.add_argument('-password', default=defaultConfig['password'] if 'password' in defaultConfig else None,
                        help='mongodb login password')
    return parser.parse_args()

  def _uploadToCarbon(self, metrics):
    now = int(time.time())
    lines = []

    for name, value in metrics.iteritems():
      if name.find('mongo') == -1:
        name = self._mongoHost.split('.')[0] + '.' + name
      lines.append(self._metricName + name + ' %s %d' % (value, now))

    message = '\n'.join(lines) + '\n'
    # print message

    sock = socket()
    try:
      sock.connect((self._carbonHost, self._carbonPort))
    except:
      print "Couldn't connect to %(server)s on port %(port)d, is carbon-agent.py running?" % {
        'server': self._carbonHost, 'port': self._carbonPort}
      sys.exit(1)
    sock.sendall(message)

  def _calculateLagTimes(self, replStatus, primaryDate):
    lags = dict()
    for hostState in replStatus['members']:
      lag = primaryDate - hostState['optimeDate']
      hostName = hostState['name'].lower().split('.')[0]
      lags[hostName + ".lag_seconds"] = '%.0f' % (
        (lag.microseconds + (lag.seconds + lag.days * 24 * 3600) * 10 ** 6) / 10 ** 6)
    return lags

  def _gatherReplicationMetrics(self):
    replicaMetrics = dict()
    replStatus = self._connection.admin.command("replSetGetStatus")

    for hostState in replStatus['members']:
      if hostState['stateStr'] == 'PRIMARY' and hostState['name'].lower().startswith(self._mongoHost):
        lags = self._calculateLagTimes(replStatus, hostState['optimeDate'])
        replicaMetrics.update(lags)
      if hostState['name'].lower().startswith(self._mongoHost):
        thisHostsState = hostState

    replicaMetrics['state'] = thisHostsState['state']
    return replicaMetrics

  def _gatherServerStatusMetrics(self):
    def rate (value1, value2, delta_t):
      return (value2-value1)/float(delta_t)
      
    serverMetrics = dict()
    serverStatus = self._connection.admin.command("serverStatus")
    
    deltaInSeconds = 2
    time.sleep(deltaInSeconds)
    serverStatus1 = self._connection.admin.command("serverStatus") 

    if 'ratio' in serverStatus['globalLock']:
      serverMetrics['lock.ratio'] = '%.5f' % serverStatus['globalLock']['ratio']
    else:
      serverMetrics['lock.ratio'] = '%.5f' % (float(serverStatus['globalLock']['lockTime']) / float(serverStatus['globalLock']['totalTime']) * 100)

    serverMetrics['lock.queue.total'] = serverStatus['globalLock']['currentQueue']['total']
    serverMetrics['lock.queue.readers'] = serverStatus['globalLock']['currentQueue']['readers']
    serverMetrics['lock.queue.writers'] = serverStatus['globalLock']['currentQueue']['writers']

    serverMetrics['connections.current'] = serverStatus['connections']['current']
    serverMetrics['connections.available'] = serverStatus['connections']['available']

    if 'btree' in serverStatus['indexCounters']:
      serverMetrics['indexes.missRatio'] = '%.5f' % serverStatus['indexCounters']['btree']['missRatio']
      serverMetrics['indexes.hits'] = serverStatus['indexCounters']['btree']['hits']
      serverMetrics['indexes.misses'] = serverStatus['indexCounters']['btree']['misses']
    else:
      serverMetrics['indexes.missRatio'] = '%.5f' % serverStatus['indexCounters']['missRatio']
      serverMetrics['indexes.hits'] = serverStatus['indexCounters']['hits']
      serverMetrics['indexes.misses'] = serverStatus['indexCounters']['misses']

    serverMetrics['cursors.open'] = serverStatus['cursors']['totalOpen']
    serverMetrics['cursors.timedOut'] = serverStatus['cursors']['timedOut']

    serverMetrics['mem.residentMb'] = serverStatus['mem']['resident']
    serverMetrics['mem.virtualMb'] = serverStatus['mem']['virtual']
    serverMetrics['mem.mapped'] = serverStatus['mem']['mapped']
    serverMetrics['mem.pageFaults'] = serverStatus['extra_info']['page_faults']
        
    serverMetrics['flushing.lastMs'] = serverStatus['backgroundFlushing']['last_ms']

    print "Serverstatus#insert: %f" % rate(serverStatus['opcounters']['insert'], serverStatus1['opcounters']['insert'],deltaInSeconds)
    print "Serverstatus#query: %f" % rate(serverStatus['opcounters']['query'], serverStatus1['opcounters']['query'], deltaInSeconds)
    print "Serverstatus#update: %f" % rate(serverStatus['opcounters']['update'], serverStatus1['opcounters']['update'], deltaInSeconds)
    print "Serverstatus#delete: %f" % rate(serverStatus['opcounters']['delete'], serverStatus1['opcounters']['delete'],deltaInSeconds)

    print "Serverstatus#page_faults: %f" % rate(serverStatus['extra_info']['page_faults'], serverStatus1['extra_info']['page_faults'], deltaInSeconds)

    print "backgroundFlushing#last_ms: %f" % serverStatus['backgroundFlushing']['last_ms']


    for assertType, value in serverStatus['asserts'].iteritems():
      serverMetrics['asserts.' + assertType ] = value

    for assertType, value in serverStatus['dur'].iteritems():
      if isinstance(value, (int, long, float)):
         serverMetrics['dur.' + assertType ] = value

    return serverMetrics


  def _gatherDbStats(self, databaseName):
    dbStatsOfCurrentDb = dict()

    if databaseName is not None:
      dbStats = self._connection[databaseName].command('dbstats')

      for stat, value in dbStats.iteritems():
        if isinstance(value, (int, long, float)):
          dbStatsOfCurrentDb['db.' + databaseName + '.' + stat] = value

    return dbStatsOfCurrentDb


  def _gatherDatabaseSpecificMetrics(self):
    dbMetrics = dict()
    for database in self._args.database:
      dbMetrics.update(self._gatherDbStats(database))

    return dbMetrics


  def execute(self):
    self._carbonHost = self._args.graphiteHost
    self._carbonPort = int(self._args.graphitePort)

    self._mongoHost = self._args.host.lower()
    self._mongoPort = 27017
    self._connection = Connection(host=self._mongoHost, port=self._mongoPort, network_timeout=10)
    if self._args.username:
      if not self._connection.admin.authenticate(self._args.username, self._args.password):
        raise Exception("Could not authenticate at mongodb")

    self._metricName = self._args.prefix + '.' + self._args.service + '.mongodb.'

    metrics = dict()
    metrics.update(self._gatherReplicationMetrics())
    metrics.update(self._gatherServerStatusMetrics())
    metrics.update(self._gatherDatabaseSpecificMetrics())

    self._uploadToCarbon(metrics)


def main():
  MongoDBGraphiteMonitor().execute()


if __name__ == "__main__":
  main()
