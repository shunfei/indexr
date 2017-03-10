#!/usr/bin/env bash
SCRIPT_DIR=$(cd $(dirname $0);echo $PWD)
cd ${SCRIPT_DIR}/..
ROOT_DIR=$(echo $PWD)

sh ${ROOT_DIR}/script/setup_lib.sh
sh ${ROOT_DIR}/script/setup_indexr-segment.sh

