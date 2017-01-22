#!/usr/bin/env bash
CUR_DIR=$(cd $(dirname $0); echo $PWD)
ROOT_DIR=$(cd ${CUR_DIR}/../;echo $PWD)
VERSION=0.2.0

source ${ROOT_DIR}/conf/env.sh

export LIBJARS=\
${ROOT_DIR}/jars/hadoop/antlr4-annotations-4.3.jar,\
${ROOT_DIR}/jars/hadoop/antlr4-runtime-4.3.jar,\
${ROOT_DIR}/jars/hadoop/args4j-2.32.jar,\
${ROOT_DIR}/jars/hadoop/hppc-0.7.1.jar,\
${ROOT_DIR}/jars/hadoop/indexr-common-${VERSION}.jar,\
${ROOT_DIR}/jars/hadoop/indexr-query-opt-${VERSION}.jar,\
${ROOT_DIR}/jars/hadoop/indexr-segment-${VERSION}.jar,\
${ROOT_DIR}/jars/hadoop/indexr-server-${VERSION}.jar,\
${ROOT_DIR}/jars/hadoop/jackson-annotations-2.6.0.jar,\
${ROOT_DIR}/jars/hadoop/jackson-core-2.6.2.jar,\
${ROOT_DIR}/jars/hadoop/jackson-databind-2.6.2.jar,\
${ROOT_DIR}/jars/hadoop/jna-4.2.1.jar,\
${ROOT_DIR}/jars/hadoop/jodd-core-3.6.7.jar,\
${ROOT_DIR}/jars/hadoop/kryo-2.21.jar,\
${ROOT_DIR}/jars/hadoop/org.abego.treelayout.core-1.0.1.jar,\
${ROOT_DIR}/jars/hadoop/spark-unsafe_2.10-1.6.0.jar

export HADOOP_CLASSPATH=${ROOT_DIR}/conf:${ROOT_DIR}/jars/hadoop/*:${ROOT_DIR}/jars/*:${HADOOP_CLASSPATH}

${HADOOP_ROOT}/bin/hadoop jar ${ROOT_DIR}/jars/hadoop/indexr-server-${VERSION}.jar io.indexr.tool.UpdateColumnJob -libjars ${LIBJARS} "$@"
