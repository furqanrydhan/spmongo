#!/usr/bin/env python

__version_info__ = (0, 2, 1)
__version__ = '.'.join([str(i) for i in __version_info__])
version = __version__

import pymongo
import pymongo.errors
#import pymongo.son_manipulator
import random
import socket
import splog
import time
import traceback

# It can take up to 30 seconds for a new primary to be selected by the replicaset.  Please reduce thrashing.
MONGO_DOWN_NICE = 0.5
# Per-process connection pool.
# TODO should this be per-thread?
CONNECTION_POOL = {}



#class TimestampInjector(pymongo.son_manipulator.SONManipulator):
#    def transform_incoming(self, son, collection):
#        if not 'created_at' in son:
#            if '_id' in son:
#                son['created_at'] = time.mktime(son['_id'].generation_time.timetuple())
#            else:
#                son['created_at'] = time.time()
#        return son

class _wrapped_object(object):
    __reconnection_wrapper_in_effect = False
    __am_secondary_connection = False
    def __init__(self, obj, **kwargs):
        self.__class__ = type(obj.__class__.__name__, (self.__class__, obj.__class__), {})
        self.__dict__ = obj.__dict__
        self._slave_okay = kwargs['slave_okay']
    def _reconnect(self, fn, *args, **kwargs):
        if not self.__reconnection_wrapper_in_effect:
            warning_printed = False
            while True:
                try:
                    self.__reconnection_wrapper_in_effect = True
                    ret = fn(self, *args, **kwargs)
                    self.__reconnection_wrapper_in_effect = False
                    return ret
                except (pymongo.errors.AutoReconnect, socket.error) as e:
                    # Retry procedure.
                    # Do not disconnect, as this will cause Mongo to forget that it
                    # had checked the problematic host and re-check it, putting us
                    # into a loop that will only end when the host comes back online.
                    if not warning_printed:
                        splog.warning('Error communicating with Mongo, retrying')
                        for line in traceback.format_exc(e).splitlines():
                            splog.warning(line)
                        warning_printed = True
                    if self.__am_secondary_connection:
                        raise
                    else:
                        time.sleep(MONGO_DOWN_NICE)
        else:
            return fn(self, *args, **kwargs)

class _wrapped_cursor(_wrapped_object, pymongo.cursor.Cursor):
    _refresh = lambda self, *args, **kwargs: self._reconnect(pymongo.cursor.Cursor._refresh, *args, **kwargs)

class _wrapped_collection(_wrapped_object, pymongo.collection.Collection):
    _slave_collection = None
    _recheck_slave_status = 0
    __secondary_wrapper_in_effect = False
    def _primary(self, fn, *args, **kwargs):
        # We need to keep tabs on when we're writing to a collection, because
        # writes must go to the primary, but reads can sometimes come from the
        # secondary; this often results in a situation where we've just put a
        # doc into the master and make a followup query about the thing we
        # just inserted; this usually fails since the slave hasn't had time to
        # sync.
        #kwargs['safe'] = self._slave_okay
        return self._reconnect(fn, *args, **kwargs)
    def _secondary(self, fn, *args, **kwargs):
        if not self.__secondary_wrapper_in_effect:
            if kwargs.get('slave_okay', self._slave_okay) and not kwargs.get('_must_use_master', False):
                try:
                    if self._slave_collection is None and self._recheck_slave_status <= 0:
                        # Locate slave connection for our master connection
                        choices = {c.host:c for c in self.database.connection._slave_connections}
                        if self.database.connection.host in choices:
                            del choices[self.database.connection.host]
                        if len(choices) > 0:
                            slave_connection = choices[random.choice(choices.keys())]
                            self._slave_collection = slave_connection[self.database.name][self.name]
                            self._slave_collection.__am_secondary_connection = True
                    assert(self._slave_collection is not None)
                    self.__secondary_wrapper_in_effect = True
                    ret = fn(self._slave_collection, *args, **kwargs)
                    self.__secondary_wrapper_in_effect = False
                    # If no results, double check with the master.
                    if isinstance(ret, pymongo.cursor.Cursor) and ret.count() == 0:
                        ret = None
                    if isinstance(ret, list) and len(ret) == 0:
                        ret = None
                    if ret is not None:
                        return ret
                    # else fall through
                except AssertionError:
                    self._recheck_slave_status -= 1
                except (socket.error, pymongo.errors.AutoReconnect):
                    self._slave_collection = None
                    self._recheck_slave_status = 1000
        # If any of the following were true:
        # - The connection/database/collection did not set slave_okay
        # - There are no secondary connections to use
        # - We recieved an error talking to our chosen secondary connection
        # then we fallback to making a reliable reconnectable call against the master connection
        return self._reconnect(fn, *args, **kwargs)
    # These operations are fundamentally reads.  Route to slaves if possible.
    distinct = lambda self, *args, **kwargs: self._secondary(pymongo.collection.Collection.distinct, *args, **kwargs)
    find_one = lambda self, *args, **kwargs: self._secondary(pymongo.collection.Collection.find_one, *args, **kwargs)
    find = lambda self, *args, **kwargs: self._secondary(pymongo.collection.Collection.find, *args, **kwargs)
    # These operations are not.  Primary host only, please.
    update = lambda self, *args, **kwargs: self._primary(pymongo.collection.Collection.update, *args, **kwargs)
    insert = lambda self, *args, **kwargs: self._primary(pymongo.collection.Collection.insert, *args, **kwargs)
    remove = lambda self, *args, **kwargs: self._primary(pymongo.collection.Collection.remove, *args, **kwargs)

class _wrapped_database(_wrapped_object, pymongo.database.Database):
#    def __init__(self, *args, **kwargs):
#        _wrapped_object.__init__(self, *args, **kwargs)
#        # Add a son manipulator which will add timestamps to inserted/upserted documents.
#        self.add_son_manipulator(TimestampInjector())
    def __getattr__(self, *args, **kwargs):
        ret = pymongo.database.Database.__getattr__(self, *args, **kwargs)
        if isinstance(ret, pymongo.collection.Collection):
            return _wrapped_collection(ret, slave_okay=self._slave_okay)
        else:
            return ret

class mongo(object):
    def __init__(self, *args, **kwargs):
        self._hosts = []
        port = str(kwargs.get('port', 27017))
        hosts = kwargs.get('hosts', kwargs.get('host', '127.0.0.1'))
        if isinstance(hosts, basestring):
            hosts = hosts.split(',')
        for host in hosts:
            if ':' in host:
                self._hosts.append(host)
            else:
                self._hosts.append(':'.join([host, port]))
        # Distributing reads across slaves is not necessarily for everybody
        self._slave_okay = kwargs.get('slave_okay', False)
    def connection(self):
        global CONNECTION_POOL
        hashable_hosts = tuple(self._hosts + [self._slave_okay])
        if hashable_hosts not in CONNECTION_POOL:
            # TODO PyMongo does not do useful things with the slave_okay
            # parameter at the moment when connecting to an entire replica set.
            CONNECTION_POOL[hashable_hosts] = pymongo.Connection(self._hosts)#, slave_okay=self._slave_okay)
            CONNECTION_POOL[hashable_hosts]._slave_connections = []
            if self._slave_okay:
                # Determine the secondaries for this replicaset based on the
                # info returned by the seeds we managed to connect to.  Make
                # connections to them individually to route read-only
                # operations (like find) to.
                for (host, port) in CONNECTION_POOL[hashable_hosts].nodes:
                    host += ':' + str(port)
                    try:
                        CONNECTION_POOL[host] = pymongo.Connection(host, slave_okay=True)
                        CONNECTION_POOL[hashable_hosts]._slave_connections.append(CONNECTION_POOL[host])
                    except pymongo.errors.AutoReconnect:
                        # This host is not up.
                        pass
        return CONNECTION_POOL[hashable_hosts]
    def database(self, name):
        return _wrapped_database(self.connection()[name], slave_okay=self._slave_okay)
    def collection(self, db, collection):
        return self.database(db).__getitem__(collection)
    def __getattr__(self, key):
        return self.database(key)
    def __getitem__(self, key):
        return self.database(key)