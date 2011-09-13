#!/usr/bin/env python

import pymongo
import pymongo.connection
import pymongo.cursor
import pymongo.errors
import socket
import splog
import time



MONGO_DOWN_NICE = 0.02



# Patch up basic Mongo functions to handle reconnect
if not hasattr(pymongo, '_spmongo_monkeyed'):
    pymongo._spmongo_monkeyed = False
if not pymongo._spmongo_monkeyed:
    CONNECTION_POOL = {}
    def _reconnect(fn):
        def __reconnect(*args, **kwargs):
            while True:
                try:
                    return fn(*args, **kwargs)
                except (pymongo.errors.AutoReconnect, socket.error) as e:
                    splog.exception(e)
                time.sleep(MONGO_DOWN_NICE)
        return __reconnect
    pymongo.connection.Connection._Connection__find_node = _reconnect(pymongo.connection.Connection._Connection__find_node)
    pymongo.connection.Connection._send_message = _reconnect(pymongo.connection.Connection._send_message)
    pymongo.connection.Connection._send_message_with_response = _reconnect(pymongo.connection.Connection._send_message_with_response)
    pymongo.cursor.Cursor._Cursor__send_message = _reconnect(pymongo.cursor.Cursor._Cursor__send_message)
    pymongo._spmongo_monkeyed = True



class mongo(object):
    def __init__(self, *args, **kwargs):
        self._hosts = str(kwargs.get('hosts', kwargs.get('host', '127.0.0.1'))).split(',')
        self._hosts = [host + ':' + str(kwargs.get('port', (host.split(':')[1] if ':' in host else 27017))) for host in self._hosts]
    def connection(self):
        global CONNECTION_POOL
        if tuple(self._hosts) not in CONNECTION_POOL:
            CONNECTION_POOL[tuple(self._hosts)] = pymongo.Connection(self._hosts)
        return CONNECTION_POOL[tuple(self._hosts)]
    def database(self, name):
        return self.connection()[name]
    def collection(self, db, collection):
        return self.database(db).__getitem__(collection)
    def __getattr__(self, key):
        return self.database(key)
    def __getitem__(self, key):
        return self.database(key)