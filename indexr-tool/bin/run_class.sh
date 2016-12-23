#!/usr/bin/env bash
CUR_DIR=$(cd $(dirname $0); echo $PWD)
ROOT_DIR=$(cd ${CUR_DIR}/../;echo $PWD)

source ${ROOT_DIR}/conf/env.sh
export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:${ROOT_DIR}/lib

CLASS_PATH=${ROOT_DIR}/conf:${ROOT_DIR}/jars/*
java -Djna.library.path=${ROOT_DIR}/lib ${JVM_OPT} -cp ${CLASS_PATH} "$@"