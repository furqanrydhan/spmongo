#!/usr/bin/env python

import pymongo
import pymongo.errors
import socket
import splog
import time
import traceback



try:
    from _version import __version_info__, __version__, version
except ImportError:
    pass



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
            #attempts = 0
            while True:
                try:
                    return fn(obj, *args, **kwargs)
                except (pymongo.errors.AutoReconnect, socket.error) as e:
                    #attempts += 1
                    splog.warning('Error communicating with Mongo, reconnecting')
                    for line in traceback.format_exc(e).splitlines():
                        splog.warning(line)
                    locate_connection(obj).disconnect()
                    #if attempts % 10 == 0:
                    #    # PyMongo sometimes chokes on a seed list where the first seed does not answer.
                    #    print locate_connection(obj).host
                    #    print locate_connection(obj).nodes
                    time.sleep(MONGO_DOWN_NICE)
        return __reconnect
    
    _connection_module = __import__('pymongo.connection')
    _reconnect_connection = lambda fn: _reconnect(fn, lambda obj: obj)
    _connection_module.connection.Connection._Connection__find_node = _reconnect_connection(_connection_module.connection.Connection._Connection__find_node)
    _connection_module.connection.Connection._send_message= _reconnect_connection(_connection_module.connection.Connection._send_message)
    _connection_module.connection.Connection._send_message_with_response = _reconnect_connection(_connection_module.connection.Connection._send_message_with_response)

    _cursor_module = __import__('pymongo.cursor')
    _reconnect_cursor = lambda fn: _reconnect(fn, lambda obj: obj.collection.database.connection)
    _cursor_module.cursor.Cursor._refresh = _reconnect_cursor(_cursor_module.cursor.Cursor._refresh)

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