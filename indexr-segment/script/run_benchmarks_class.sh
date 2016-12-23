#!/usr/bin/env bash
SCRIPT_DIR=$(cd $(dirname $0);echo $PWD)
ROOT_DIR=${SCRIPT_DIR}/..
#source ${ROOT_DIR}/../script/env_setup.sh

java -Djna.library.path=${ROOT_DIR}/lib -cp ${ROOT_DIR}/target/benchmarks.jar io.indexr.segment.pack.DPSegmentBenchmark "$@"