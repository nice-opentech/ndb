#!/bin/sh

ADDRESS="0.0.0.0:2333"
LOG=/tmp/ndb-units.log

function clean() {
    rm -f $LOG
    make clean
}

function build() {
    clean
    make -j 10
}

function run() {
    echo "[TEST $1]"
    echo "[TEST $1]" >> $LOG
    echo "########################################" >> $LOG
    $1 >> $LOG 2>&1 || exit 1
    echo "----------------------------------------" >> $LOG
}

function stress() {
    cd stress
    ./stress.sh
    cd ..
}

build

run "common/channel"
run "common/logger"
run "common/encode"
run "common/socket $ADDRESS"
run "common/eventd $ADDRESS"

run "server/request"
run "server/server $ADDRESS"

run "engine/encode"
run "engine/backup"
run "engine/hashlock"
run "engine/namespace"

run "command/common"

cd stress
run "./stress_test.sh"
run "./replica_test.sh"
cd ..

clean

echo "\\o/ All tests passed without errors!"
