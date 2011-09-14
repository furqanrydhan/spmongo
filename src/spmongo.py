#!/usr/bin/env python

__version_info__ = (0, 1, 6)
__version__ = '.'.join([str(i) for i in __version_info__])
version = __version__

import pymongo
import pymongo.errors
import socket
import splog
import time
import traceback

# It can take up to 30 seconds for a new primary to be selected by the replicaset.  Please reduce thrashing.
MONGO_DOWN_NICE = 1

# Patch up basic Mongo functions to handle reconnect
if not hasattr(pymongo, '_spmongo_monkeyed'):
    pymongo._spmongo_monkeyed = False
if not pymongo._spmongo_monkeyed:
    # Helper function to reconnect.
    # The first argument is the function which should be wrapped.
    # The second argument is a helper function to locate the connection object
    def _reconnect(fn, locate_connection):
        def __reconnect(obj, *args, **kwargs):
            while True:
                try:
                    return fn(obj, *args, **kwargs)
                #except socket.error as e:
                #    splog.warning('socket error, disconnecting and reconnecting')
                #    locate_connection(obj).disconnect()
                except pymongo.errors.AutoReconnect as e:
                    # Retry procedure.
                    # Do not disconnect, as this will cause Mongo to forget that it
                    # had checked the problematic host and re-check it, putting us
                    # into a loop that will only end when the host comes back online.
                    splog.warning('Error communicating with Mongo, retrying')
                    for line in traceback.format_exc(e).splitlines():
                        splog.warning(line)
                finally:
                    time.sleep(MONGO_DOWN_NICE)
        return __reconnect
    
    #_connection_module = __import__('pymongo.connection')
    #_reconnect_connection = lambda fn: _reconnect(fn, lambda obj: obj)
    #_connection_module.connection.Connection._Connection__find_node = _reconnect_connection(_connection_module.connection.Connection._Connection__find_node)
    #_connection_module.connection.Connection._send_message= _reconnect_connection(_connection_module.connection.Connection._send_message)
    #_connection_module.connection.Connection._send_message_with_response = _reconnect_connection(_connection_module.connection.Connection._send_message_with_response)

    #_cursor_module = __import__('pymongo.cursor')
    #_reconnect_cursor = lambda fn: _reconnect(fn, lambda obj: obj.collection.database.connection)
    #_cursor_module.cursor.Cursor._refresh = _reconnect_cursor(_cursor_module.cursor.Cursor._refresh)
    
    #_collection_module = __import__('pymongo.collection')
    #_reconnect_collection = lambda fn: _reconnect(fn, lambda obj: obj.database.connection)
    #for fn in dir(_collection_module.collection.Collection):
    #    try:
    #        assert(not fn.startswith('_'))
    #        assert(fn not in ['super'])
    #        assert(fn in _collection_module.collection.Collection.__dict__)
    #        assert(hasattr(_collection_module.collection.Collection.__dict__[fn], '__call__'))
    #        setattr(_collection_module.collection.Collection, fn, _reconnect_collection(_collection_module.collection.Collection.__dict__[fn]))
    #        splog.info('Monkeyed with pymongo.collection.Collection.' + fn)
    #    except AssertionError:
    #        pass

    # Connection pooling on a per-process basis.
    # TODO should this be per-thread?
    CONNECTION_POOL = {}

    # Don't redo all this
    pymongo._spmongo_monkeyed = True

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
        return self.connection()[name]
    def collection(self, db, collection):
        return self.database(db).__getitem__(collection)
    def __getattr__(self, key):
        return self.database(key)
    def __getitem__(self, key):
        return self.database(key)