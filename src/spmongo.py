#!/usr/bin/env python

__version_info__ = (0, 3, 0)
__version__ = '.'.join([str(i) for i in __version_info__])
version = __version__

import collections
import pymongo
import pymongo.errors
import random
import socket
import splog
import time
import traceback

try:
    import gevent.hub
    import gevent.queue
    import gevent.greenlet
    import gevent.coros
    import weakref
    
    class _gevent_safe_connection_pool(object):
        def __init__(self, *args, **kwargs):
            self.internal_timeout = 0.1
            self.network_timeout = 3.0
            self.pool_size = int(kwargs['max_pool_size'])
            del kwargs['max_pool_size']
            self._lock = gevent.coros.RLock()
            self._count = 0
            self._used = {}
            self._queue = gevent.queue.Queue(self.pool_size)
            self.args = args
            self.kwargs = kwargs

        def _make_connection(self):
            return pymongo.ReplicaSetConnection(*self.args, **self.kwargs)

        def _reference_or_greenlet(self):
            greenlet = gevent.hub.getcurrent()
            return (greenlet if isinstance(greenlet, gevent.Greenlet) else weakref.ref(greenlet, self._put))

        def get(self):
            greenlet = gevent.hub.getcurrent()
            ref = self._reference_or_greenlet()
            conn = self._used.get(ref)
            if conn is None:
                with self._lock:
                    # Prefer an idle connection from the pool over creating connections to pool limit.
                    if len(self._used) < self._count:
                        conn = self._queue.get(timeout=self.internal_timeout)
                    if conn is None and self._count < self.pool_size:
                        self._count += 1
                        conn = self._make_connection()
                if conn is None:
                    conn = self._queue.get(timeout=self.network_timeout)

            if isinstance(greenlet, gevent.Greenlet):
                greenlet.link(self._put)
            self._used[ref] = conn
            return conn

        def put(self):
            greenlet = gevent.hub.getcurrent()
            self._put(greenlet)

        def _put(self, greenlet):
            try:
                # It's actually okay to just look up by the greenlet here (instead of greenlet-or-weakref)
                # because the weakref hashes the same as the underlying object, so if we stored a weakref,
                # we can look up by greenlet.
                conn = self._used.get(greenlet)
                if conn is not None:
                    del self._used[greenlet]
                    self._queue.put(conn)
            except:
                with self._lock:
                    self._count -= 1
except ImportError:
    pass

# It can take up to 30 seconds for a new primary to be selected by the replicaset.  Please reduce thrashing.
MONGO_DOWN_NICE = 0.5



class _wrapped_object(object):
    __reconnection_wrapper_in_effect = False
    def __init__(self, obj, reporter, **kwargs):
        self.__class__ = type(obj.__class__.__name__, (self.__class__, obj.__class__), {})
        self.__dict__ = obj.__dict__
        self._reporter = reporter
    def _report(self, *args, **kwargs):
        self._reporter._report(*args, **kwargs)
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
                    time.sleep(MONGO_DOWN_NICE)
        else:
            return fn(self, *args, **kwargs)

class _wrapped_cursor(_wrapped_object, pymongo.cursor.Cursor):
    _refresh = lambda self, *args, **kwargs: self._reconnect(pymongo.cursor.Cursor._refresh, *args, **kwargs)

class _wrapped_collection(_wrapped_object, pymongo.collection.Collection):
    distinct = lambda self, *args, **kwargs: self._reconnect(pymongo.collection.Collection.distinct, *args, **kwargs)
    find = lambda self, *args, **kwargs: _wrapped_cursor(self._reconnect(pymongo.collection.Collection.find, *args, **kwargs), self._reporter)
    map_reduce = lambda self, *args, **kwargs: self._reconnect(pymongo.collection.Collection.map_reduce, *args, **kwargs)
    update = lambda self, *args, **kwargs: self._reconnect(pymongo.collection.Collection.update, *args, **kwargs)
    insert = lambda self, *args, **kwargs: self._reconnect(pymongo.collection.Collection.insert, *args, **kwargs)
    remove = lambda self, *args, **kwargs: self._reconnect(pymongo.collection.Collection.remove, *args, **kwargs)
    # Why is find_one different?
    # find_one is different because the applications which use spmongo have a
    # similar common use case:
    # 1. the architecture of the application is intended to make use of
    #    reusable logic blocks.
    # 2. many of these logic blocks operate on the _id field, to minimize the
    #    amount of context required.
    # 3. when creating a new object, the application often creates the object,
    #    inserts into mongo, and then passes the _id field to a logic block
    #    which then attempts to retrieve the document from mongo.
    # 4. in the case where we are reading from secondaries but writing to the
    #    primary, this will often result in documents missing due to
    #    replication lag.
    # So here's the strategy we use to try to route as many requests to the
    # secondaries as possible while minimizing the appearance of strange logic
    # errors due to replication lag:
    # 1. make the query without any special consideration, which will probably
    #    be routed to a secondary.
    # 2. if the query returns without any results, explicitly query the
    #    primary server, which is not subject to replication lag.
    def find_one(self, *args, **kwargs):
        # We must retry on primary if this find_one query went to a secondary and found no results.
        try:
            start = time.time()
            ret = self._reconnect(pymongo.collection.Collection.find_one, *args, read_preference=pymongo.ReadPreference.SECONDARY, **kwargs)
            self._report('find_one', 'secondary', time.time() - start)
            assert(ret is not None)
        except AssertionError:
            start = time.time()
            ret = self._reconnect(pymongo.collection.Collection.find_one, *args, read_preference=pymongo.ReadPreference.PRIMARY, **kwargs)
            self._report('find_one', 'primary', time.time() - start)
        return ret

class _wrapped_database(_wrapped_object, pymongo.database.Database):
    def __getattr__(self, *args, **kwargs):
        ret = pymongo.database.Database.__getattr__(self, *args, **kwargs)
        if isinstance(ret, pymongo.collection.Collection):
            return _wrapped_collection(ret, self._reporter)
        else:
            return ret

class mongo(object):
    _totals = {
        'operations':collections.defaultdict(lambda: collections.defaultdict(lambda: {'count':0, 'times':{'total':0, 'max':0}}))
    }
    def __init__(self, *args, **kwargs):
        self._connection = None
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
        self._rsname = kwargs.get('replicaset')
        try:
            assert(self._rsname is not None)
            self._pool = _gevent_safe_connection_pool(','.join(self._hosts), max_pool_size=kwargs.get('max_pool_size', 10), replicaSet=self._rsname, read_preference=pymongo.ReadPreference.SECONDARY)
        except (AssertionError, NameError):
            self._pool = None
    def _get_connection(self):
        if self._pool is not None:
            # Pull from pool.  This should return the one already in use by this greenlet if any.
            return self._pool.get()
        else:
            if self._connection is None:
                self._connection = pymongo.Connection(self._hosts, max_pool_size=1)
            return self._connection
    def disconnect(self):
        if self._pool is not None:
            self._pool.get().disconnect()
            self._pool.put()
        elif self._connection is not None:
            self._connection.disconnect()
    def end_request(self):
        if self._pool is not None:
            self._pool.put()
        elif self._connection is not None:
            self._connection.end_request()
            self._connection = None
    def database(self, name):
        return _wrapped_database(self._get_connection()[name], self)
    def collection(self, db, collection):
        return self.database(db).__getitem__(collection)
    def __getattr__(self, key):
        return self.database(key)
    def __getitem__(self, key):
        return self.database(key)
    def _report(self, operation, host, duration):
        self._totals['operations'][operation][host]['count'] += 1
        self._totals['operations'][operation][host]['times']['total'] += duration
        self._totals['operations'][operation][host]['times']['max'] = max(self._totals['operations'][operation][host]['times']['max'], duration)
    def statistics(self):
        return self._totals