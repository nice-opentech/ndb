#!/bin/sh

function clean() {
    # Kill all ndb processes.
    pkill -u $USER ndb && sleep 5
    rm -rf nicedb* repldb* backup restore
}

function execcmd() {
    echo "$2" | redis-cli -p $1
}

clean

# Start master and replica.
alias ndb="../../ndb/ndb"
ndb --logger.filename nicedb.log --engine.dbname nicedb \
    --server.address 0.0.0.0:9736
ndb --logger.filename repldb.log --engine.dbname repldb \
    --server.address 0.0.0.0:9737 --replica.address 127.0.0.1:9736

# Run redis benchmark.
echo "Create namespaces ..."
sleep 1
execcmd 9736 "NSNEW key"
execcmd 9736 "NSNEW counter"
execcmd 9737 "NSNEW key"
execcmd 9737 "NSNEW counter"
redis-benchmark -p 9736 -n 100000 -r 100000

# Backup master to backup.
echo "Backup ..."
sleep 1
execcmd 9736 "BACKUP backup"
sleep 1
execcmd 9736 "BACKUP stop"
sleep 1
execcmd 9736 "BACKUP backup"
sleep 60
execcmd 9736 "INFO backup"
execcmd 9736 "INFO engine"
execcmd 9737 "INFO engine"

# Restore backup dir to restore dir.
echo "Restore ..."
ndb --restore backup --engine.dbname restore

# Check consistence of nicedb dir, repldb dir and restore dir.
echo "Check RocksDB ..."
alias rocksdb_check="../../tools/rocksdb_check"
rocksdb_check nicedb repldb
rocksdb_check nicedb restore
rocksdb_check repldb restore

clean
