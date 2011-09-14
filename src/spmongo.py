#!/usr/bin/env python

__version_info__ = (0, 1, 7)
__version__ = '.'.join([str(i) for i in __version_info__])
version = __version__

import pymongo
import pymongo.errors
import socket
import splog
import time
import traceback

# It can take up to 30 seconds for a new primary to be selected by the replicaset.  Please reduce thrashing.
MONGO_DOWN_NICE = 0.5
# Per-process connection pool.
# TODO should this be per-thread?
CONNECTION_POOL = {}



class _wrapped_object(object):
    __reconnection_wrapper_in_effect = False
    def __init__(self, obj):
        self.__class__ = type(obj.__class__.__name__, (self.__class__, obj.__class__), {})
        self.__dict__ = obj.__dict__
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
    find_one = lambda self, *args, **kwargs: self._reconnect(pymongo.collection.Collection.find_one, *args, **kwargs)
    find = lambda self, *args, **kwargs: self._reconnect(pymongo.collection.Collection.find, *args, **kwargs)
    update = lambda self, *args, **kwargs: self._reconnect(pymongo.collection.Collection.update, *args, **kwargs)
    insert = lambda self, *args, **kwargs: self._reconnect(pymongo.collection.Collection.insert, *args, **kwargs)
    remove = lambda self, *args, **kwargs: self._reconnect(pymongo.collection.Collection.remove, *args, **kwargs)

class _wrapped_database(_wrapped_object, pymongo.database.Database):
    def __getattr__(self, *args, **kwargs):
        ret = pymongo.database.Database.__getattr__(self, *args, **kwargs)
        if isinstance(ret, pymongo.collection.Collection):
            return _wrapped_collection(ret)
        else:
            return ret

class mongo(object):
    def __init__(self, *args, **kwargs):
        self._hosts = str(kwargs.get('hosts', kwargs.get('host', '127.0.0.1'))).split(',')
        self._hosts = [(host.split(':')[0] if ':' in host else host) + ':' + str(kwargs.get('port', (host.split(':')[1] if ':' in host else 27017))) for host in self._hosts]
    def connection(self):
        global CONNECTION_POOL
        if tuple(self._hosts) not in CONNECTION_POOL:
            # TODO PyMongo does not do useful things with the slave_okay parameter at the moment.
            CONNECTION_POOL[tuple(self._hosts)] = pymongo.Connection(self._hosts)#, slave_okay=True)
        return CONNECTION_POOL[tuple(self._hosts)]
    def database(self, name):
        return _wrapped_database(self.connection()[name])
    def collection(self, db, collection):
        return self.database(db).__getitem__(collection)
    def __getattr__(self, key):
        return self.database(key)
    def __getitem__(self, key):
        return self.database(key)