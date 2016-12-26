#!/usr/bin/env bash
SCRIPT_DIR=$(cd $(dirname $0);echo $PWD)
cd ${SCRIPT_DIR}/..
ROOT_DIR=$(echo $PWD)

sh ${ROOT_DIR}/script/release_indexr-drill.sh
sh ${ROOT_DIR}/script/release_indexr-hive.sh
sh ${ROOT_DIR}/script/release_indexr-tool.sh
sh ${ROOT_DIR}/script/release_lib.sh
