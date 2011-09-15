#!/bin/bash

mkdir -p /tmp/failover/alpha
mongod --port 1011 --dbpath /tmp/failover/alpha/ --replSet=failover &
ALPHA_PID=$!

mkdir -p /tmp/failover/beta
mongod --port 1012 --dbpath /tmp/failover/beta/ --replSet=failover &
BETA_PID=$!

# Initialize the replica set
mongo localhost:1011 --eval ''

# Determine who is primary

# Launch a process to write data to the replica set

# Take down the primary

# Wait until the secondary becomes the primary

# Verify that the process is still running and has recognized the new primary





#mongo localhost:1011/user --eval 'printjson(db.user.findOne())'