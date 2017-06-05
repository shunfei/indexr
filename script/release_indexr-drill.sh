#!/usr/bin/env bash
SCRIPT_DIR=$(cd $(dirname $0);echo $PWD)
cd ${SCRIPT_DIR}/..
ROOT_DIR=$(echo $PWD)
source ${ROOT_DIR}/script/env.sh

RELEASE_PATH=${ROOT_DIR}/distribution/indexr-${VERSION}

cd ${ROOT_DIR}

rm -rf ${RELEASE_PATH}/indexr-drill
mkdir -p ${RELEASE_PATH}/indexr-drill/jars/3rdparty
sh ${ROOT_DIR}/script/compile_indexr-server.sh

function cp_jar {
    if [ ! -f $1 ]; then
        echo "$1 not exists!"
        exit 1
    fi
    cp -f $1 ${RELEASE_PATH}/indexr-drill/jars/3rdparty/
}

# copy drill files
cp_jar ${MAVEN_PATH}/io/indexr/indexr-common/${VERSION}/indexr-common-${VERSION}.jar
cp_jar ${MAVEN_PATH}/io/indexr/indexr-segment/${VERSION}/indexr-segment-${VERSION}.jar
cp_jar ${MAVEN_PATH}/io/indexr/indexr-vlt-segment/${VERSION}/indexr-vlt-segment-${VERSION}.jar
cp_jar ${MAVEN_PATH}/io/indexr/indexr-server/${VERSION}/indexr-server-${VERSION}.jar
cp_jar ${MAVEN_PATH}/io/indexr/indexr-query-opt/${VERSION}/indexr-query-opt-${VERSION}.jar

# copy depenencies
cp_jar ${MAVEN_PATH}/org/apache/kafka/kafka_2.10/0.8.2.0/kafka_2.10-0.8.2.0.jar
cp_jar ${MAVEN_PATH}/org/apache/kafka/kafka-clients/0.8.2.0/kafka-clients-0.8.2.0.jar
cp_jar ${MAVEN_PATH}/com/101tec/zkclient/0.3/zkclient-0.3.jar
cp_jar ${MAVEN_PATH}/net/jpountz/lz4/lz4/1.2.0/lz4-1.2.0.jar
cp_jar ${MAVEN_PATH}/org/xerial/snappy/snappy-java/1.1.1.6/snappy-java-1.1.1.6.jar
cp_jar ${MAVEN_PATH}/com/yammer/metrics/metrics-core/2.2.0/metrics-core-2.2.0.jar
cp_jar ${MAVEN_PATH}/net/sf/jopt-simple/jopt-simple/3.2/jopt-simple-3.2.jar
cp_jar ${MAVEN_PATH}/org/scala-lang/scala-library/2.10.4/scala-library-2.10.4.jar
cp_jar ${MAVEN_PATH}/org/apache/spark/spark-unsafe_2.10/1.6.0/spark-unsafe_2.10-1.6.0.jar
cp_jar ${MAVEN_PATH}/com/esotericsoftware/kryo/kryo/2.21/kryo-2.21.jar
cp_jar ${MAVEN_PATH}/net/java/dev/jna/jna/4.2.1/jna-4.2.1.jar

mkdir -p ${RELEASE_PATH}/indexr-drill/conf
cp -f ${ROOT_DIR}/indexr-server/config/indexr.config.properties ${RELEASE_PATH}/indexr-drill/conf/

