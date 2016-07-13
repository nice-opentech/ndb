#!/bin/sh

alias ncov="lcov -c -d ndb"

echo "-------------------- Make ndb --------------------"
{
    cd ndb
    make clean
    make coverage -j 10
    cd ..
    ncov -i -o cov.bases
}

echo "-------------------- Make tools --------------------"
{
    cd tools
    make clean
    make -j 10
    cd ..
}

echo "-------------------- Run tests --------------------"
{
    ndb/ndb -h
    ndb/ndb -v
    ndb/ndb -c ndb.conf -t

    cd units
    ./units.sh
    cd ..

    ./runtest
    pkill -u $USER ndb && sleep 3

    ncov -o cov.tests
}

echo "-------------------- Generate html --------------------"
{
    lcov -a cov.bases -a cov.tests -o cov.final
    genhtml -q --no-branch-coverage -o coverage cov.final
}

echo "-------------------- Cleanup --------------------"
{
    cd ndb
    make clean
    cd ..

    cd units
    make clean
    cd ..

    rm -f cov.*
    find . -name "*.gcda" -exec rm {} +
    find . -name "*.gcno" -exec rm {} +
}
