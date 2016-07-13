#!/bin/sh

function clean() {
    pkill -u $USER ndb && sleep 3
    rm -rf nicedb*
    make clean
}

clean

# Build tests.
make

# Start ndb.
../../ndb/ndb

# Run stress and compaction tests.
sleep 1
echo "[Stress Test]"
./stress.test
echo "[Setup Compaction]"
./compaction_setup :9736  100000
echo "[Check Compaction]"
./compaction_check nicedb 100000

clean
