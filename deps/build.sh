#!/bin/bash

ROOT=`pwd`

# Build jemalloc
function build_jemalloc() {
    JEMALLOC=jemalloc-4.0.4
    if [ ! -d $JEMALLOC ]; then
        wget https://github.com/jemalloc/jemalloc/archive/4.0.4.tar.gz -O $JEMALLOC.tar.gz
        tar xf $JEMALLOC.tar.gz
    fi
    cd $JEMALLOC
    ./autogen.sh
    ./configure --prefix=$ROOT
    make build_lib_static -j 10
    make install install_lib_static
    cd $ROOT
}

if [ ! -f $ROOT/lib/libjemalloc.a ]; then
    build_jemalloc
fi

# Build z
function build_z() {
    Z=zlib-1.2.8
    if [ ! -d $Z ]; then
        wget http://zlib.net/zlib-1.2.8.tar.gz -O $Z.tar.gz
        tar xf $Z.tar.gz
    fi
    cd $Z
    ./configure --prefix=$ROOT
    make -j 10
    make install
    cd $ROOT
}

if [ ! -f $ROOT/lib/libz.a ]; then
    build_z
fi

# Build bz2
function build_bz2() {
    BZ2=bzip2-1.0.6
    if [ ! -d $BZ2 ]; then
        wget http://www.bzip.org/1.0.6/bzip2-1.0.6.tar.gz -O $BZ2.tar.gz
        tar xf $BZ2.tar.gz
    fi
    cd $BZ2
    make -j 10
    make install PREFIX=$ROOT
    cd $ROOT
}

if [ ! -f $ROOT/lib/libbz2.a ]; then
    build_bz2
fi

# Build lz4
function build_lz4() {
    LZ4=lz4-r131
    if [ ! -d $LZ4 ]; then
        wget https://github.com/Cyan4973/lz4/archive/r131.tar.gz -O $LZ4.tar.gz
        tar xf $LZ4.tar.gz
    fi
    cd $LZ4
    make -j 10 lib
    make install PREFIX=$ROOT
    cd $ROOT
}

if [ ! -f $ROOT/lib/liblz4.a ]; then
    build_lz4
fi

# Build snappy
function build_snappy() {
    SNAPPY=snappy-1.1.3
    if [ ! -d $SNAPPY ]; then
        wget https://github.com/google/snappy/archive/1.1.3.tar.gz -O $SNAPPY.tar.gz
        tar xf $SNAPPY.tar.gz
        if [ `uname` == "Darwin" ]; then
            sed -ie 's/libtoolize/glibtoolize/g' $SNAPPY/autogen.sh
        fi
        sed -ie '/AC_CONFIG_MACRO_DIR/ a\
        AC_CONFIG_AUX_DIR([.])\n' $SNAPPY/configure.ac
    fi
    cd $SNAPPY
    ./autogen.sh
    ./configure --prefix=$ROOT
    make -j 10
    make install
    cd $ROOT
}

if [ ! -f $ROOT/lib/libsnappy.a ]; then
    build_snappy
fi

# Build rocksdb
function build_rocksdb() {
    ROCKSDB=rocksdb-4.3.1
    if [ ! -d $ROCKSDB ]; then
        wget https://github.com/facebook/rocksdb/archive/v4.3.1.tar.gz -O $ROCKSDB.tar.gz
        tar xf $ROCKSDB.tar.gz
    fi
    cd $ROCKSDB
    make -j 10 static_lib EXTRA_CXXFLAGS="-I../include -DSNAPPY"
    make install INSTALL_PATH=$ROOT
    cd $ROOT
}

if [ ! -f $ROOT/lib/librocksdb.a ]; then
    build_rocksdb
fi

# Build protobuf
function build_protobuf() {
    PROTOBUF=protobuf-3.0.0-beta-2
    if [ ! -d $PROTOBUF ]; then
        wget https://github.com/google/protobuf/archive/v3.0.0-beta-2.tar.gz -O $PROTOBUF.tar.gz
        tar xf $PROTOBUF.tar.gz
        sed -ie '/AC_CONFIG_MACRO_DIR/ a\
        AC_CONFIG_AUX_DIR([.])\n' $PROTOBUF/configure.ac
    fi
    cd $PROTOBUF

    GMOCK=gmock-1.7.0
    if [ ! -d $GMOCK ]; then
        wget https://github.com/google/googlemock/archive/release-1.7.0.tar.gz -O $GMOCK.tar.gz
        mkdir gmock
        tar xf $GMOCK.tar.gz -C gmock
    fi

    ./autogen.sh
    ./configure --prefix=$ROOT
    make -j 10
    make install
    cd $ROOT
}

if [ ! -f $ROOT/lib/libprotobuf.a ]; then
    build_protobuf
fi

# Build hiredis
function build_hiredis() {
    HIREDIS=hiredis-0.13.3
    if [ ! -d $HIREDIS ]; then
        wget https://github.com/redis/hiredis/archive/v0.13.3.tar.gz -O $HIREDIS.tar.gz
        tar xf $HIREDIS.tar.gz
    fi
    cd $HIREDIS
    make -j 10
    make install PREFIX=$ROOT
    cd $ROOT
}

if [ ! -f $ROOT/lib/libhiredis.a ]; then
    build_hiredis
fi

# Build json
function build_json() {
    RAPIDJSON=rapidjson-1.0.2
    if [ ! -d $RAPIDJSON ]; then
        wget https://github.com/miloyip/rapidjson/archive/v1.0.2.tar.gz -O $RAPIDJSON.tar.gz
        tar xf $RAPIDJSON.tar.gz
    fi
    cp -r $RAPIDJSON/include/rapidjson $ROOT/include
}

if [ ! -d $ROOT/include/rapidjson ]; then
    build_json
fi

# Build curl
function build_curl() {
    CURL=curl-curl-7_48_0
    if [ ! -d $CURL ]; then
        wget https://github.com/curl/curl/archive/curl-7_48_0.tar.gz -O $CURL.tar.gz
        tar xf $CURL.tar.gz
    fi
    cd $CURL
    ./buildconf
    ./configure --prefix=$ROOT --without-ssl --without-libssh2 --without-librtmp --disable-crypto
    make -j 10
    make install
    cd $ROOT
}

if [ ! -f $ROOT/lib/libcurl.a ]; then
    build_curl
fi
