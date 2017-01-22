#!/usr/bin/env bash

# local maven repo path
export MAVEN_PATH=`mvn help:evaluate -Dexpression=settings.localRepository | grep -v '\[INFO\]'`
export VERSION=0.2.0

# local c++ lib env
export BOOST_INCLUDES=/usr/local/include
export BOOST_LIBS_DIR=/usr/local/lib

export CPLUS_INCLUDE_PATH=${CPLUS_INCLUDE_PATH}:${BOOST_INCLUDES}
export C_INCLUDE_PATH=${C_INCLUDE_PATH}:${BOOST_INCLUDES}
export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:${BOOST_LIBS_DIR}
export LIBRARY_PATH=${LIBRARY_PATH}:${BOOST_LIBS_DIR}

function check_exit_code {
    if [ ! "$1" = "0" ]; then
        exit $1
    fi
}

function print_exec {
    echo $1
    $1
}

if [ ! -d ${MAVEN_PATH} ]; then
    echo "Maven local repository path not exits: $MAVEN_PATH";
    exit 1
fi

echo "Maven local repository path: $MAVEN_PATH";
