#!/usr/bin/env python

import time

#try:
#    import asyncmongo
#except ImportError:
#    pass
import pymongo

MONGO_DOWN_NICE = 1

class mongo(object):
    _connections = None
    def __init__(self, *args, **kwargs):
        # Connection pooling
        self._connections = kwargs.get('connections', {})
        self._host = str(kwargs.get('host', '127.0.0.1'))
        self._port = int(kwargs.get('port', 27017))
    def connection(self):
        mongo_down_reported = False
        #if not self._async:
        while True:
                try:
                    if not (self._host, self._port) in self._connections:
                        self._connections[(self._host, self._port)] = pymongo.Connection(self._host, self._port)
                    return self._connections[(self._host, self._port)]
                except pymongo.errors.AutoReconnect:
                    if not mongo_down_reported:
                        print 'PyMongo AutoReconnect error; no Mongo server at ' + self._host + '?'
                        mongo_down_reported = True
                    time.sleep(MONGO_DOWN_NICE)
    def database(self, name):
        #if self._async:
        #    return asyncmongo.Client('asyncpool', host=self._host, port=self._port, dbname=name)
        #else:
            return self.connection()[name]
    def collection(self, db, collection):
        return self.database(db).__getattr__(collection)
    def __getattr__(self, key):
        return self.database(key)
    def __getitem__(self, key):
        return self.database(key)