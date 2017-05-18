#!/usr/bin/env bash
CUR_DIR=$(cd $(dirname $0); echo $PWD)
ROOT_DIR=$(cd ${CUR_DIR}/../;echo $PWD)

source ${ROOT_DIR}/conf/env.sh

for jar in ${ROOT_DIR}/jars/hadoop/*.jar
do
    LIBJARS=${LIBJARS},${jar}
done

export HADOOP_CLASSPATH=${ROOT_DIR}/conf:${ROOT_DIR}/jars/hadoop/*:${ROOT_DIR}/jars/*:${HADOOP_CLASSPATH}

${HADOOP_ROOT}/bin/hadoop jar ${ROOT_DIR}/jars/hadoop/indexr-server-${VERSION}.jar io.indexr.tool.UpdateColumnJob -libjars ${LIBJARS} "$@"
