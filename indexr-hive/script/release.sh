#!/usr/bin/env bash
SCRIPT_DIR=$(cd $(dirname $0);echo $PWD)
MAVEN_PATH=~/.m2/repository

cd ${SCRIPT_DIR}/../../
mvn install -DskipTests=true -pl indexr-hadoop -am

cd ${SCRIPT_DIR}/../

mkdir -p distribution/hive-aux
rm -rf distribution/hive-aux/*.jar

cp ${MAVEN_PATH}/io/indexr/indexr-common/0.01/indexr-common-0.01.jar distribution/hive-aux/
cp ${MAVEN_PATH}/io/indexr/indexr-segment/0.01/indexr-segment-0.01.jar distribution/hive-aux/
cp ${MAVEN_PATH}/io/indexr/indexr-hadoop/0.01/indexr-hadoop-0.01.jar distribution/hive-aux/

cp ${MAVEN_PATH}/org/apache/spark/spark-unsafe_2.10/1.6.0/spark-unsafe_2.10-1.6.0.jar distribution/hive-aux/
cp ${MAVEN_PATH}/com/esotericsoftware/kryo/kryo/2.21/kryo-2.21.jar distribution/hive-aux/

cp ${MAVEN_PATH}/com/fasterxml/jackson/core/jackson-core/2.6.2/jackson-core-2.6.2.jar distribution/hive-aux/
cp ${MAVEN_PATH}/com/fasterxml/jackson/core/jackson-databind/2.6.2/jackson-databind-2.6.2.jar distribution/hive-aux/
cp ${MAVEN_PATH}/com/fasterxml/jackson/core/jackson-annotations/2.6.0/jackson-annotations-2.6.0.jar distribution/hive-aux/
cp ${MAVEN_PATH}/net/java/dev/jna/jna/4.2.1/jna-4.2.1.jar distribution/hive-aux/
cp ${MAVEN_PATH}/org/jodd/jodd-core/3.6.7/jodd-core-3.6.7.jar distribution/hive-aux/
