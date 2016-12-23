#!/usr/bin/env bash
SCRIPT_DIR=$(cd $(dirname $0);echo $PWD)
ROOT_DIR=${SCRIPT_DIR}/..
source ${ROOT_DIR}/../script/env_setup.sh

java -cp ${ROOT_DIR}/sftest_indexr-query-opt-0.01-jar-with-dependencies.jar io.indexr.query.tool.RQLProfile "$@"
