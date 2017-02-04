#!/usr/bin/env bash
SCRIPT_DIR=$(cd $(dirname $0);echo $PWD)
cd ${SCRIPT_DIR}/..
ROOT_DIR=$(echo $PWD)
source ${ROOT_DIR}/script/env.sh

LIBDIR=${ROOT_DIR}/lib
mkdir -p ${LIBDIR}

# link lib into sub modules, convenient for testing.
rm -f ${ROOT_DIR}/indexr-segment/lib
rm -f ${ROOT_DIR}/indexr-server/lib

ln -s ${LIBDIR} ${ROOT_DIR}/indexr-segment/lib
ln -s ${LIBDIR} ${ROOT_DIR}/indexr-server/lib
