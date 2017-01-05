#!/usr/bin/env bash
SCRIPT_DIR=$(cd $(dirname $0);echo $PWD)
ROOT_DIR=$(cd ${SCRIPT_DIR}/../;echo $PWD)
source ${ROOT_DIR}/script/env.sh

RELEASE_PATH=${ROOT_DIR}/distribution/indexr-${VERSION}

mkdir -p ${RELEASE_PATH}/indexr-tool
cp -r -f ${ROOT_DIR}/indexr-tool/* ${RELEASE_PATH}/indexr-tool/
cp -f ${ROOT_DIR}/lib/* ${RELEASE_PATH}/indexr-tool/lib/


cd ${ROOT_DIR}
mvn install -DskipTests=true -pl indexr-server -am
mvn assembly:single -DdescriptorId=jar-with-dependencies -DskipTests=true -pl indexr-server -am

check_exit_code $?

rm -rf ${RELEASE_PATH}/indexr-tool/jars
mkdir -p ${RELEASE_PATH}/indexr-tool/jars
mkdir -p ${RELEASE_PATH}/indexr-tool/jars/hadoop
cp -f ${ROOT_DIR}/indexr-server/target/indexr-server-${VERSION}-jar-with-dependencies.jar ${RELEASE_PATH}/indexr-tool/jars/


function cp_jar {
    if [ ! -f $1 ]; then
        echo "$1 not exists!"
        exit 1
    fi
    cp -f $1 ${RELEASE_PATH}/indexr-tool/jars/hadoop
}

cp_jar ${ROOT_DIR}/indexr-common/target/indexr-common-${VERSION}.jar
cp_jar ${ROOT_DIR}/indexr-segment/target/indexr-segment-${VERSION}.jar
cp_jar ${ROOT_DIR}/indexr-query-opt/target/indexr-query-opt-${VERSION}.jar
cp_jar ${ROOT_DIR}/indexr-server/target/indexr-server-${VERSION}.jar

# copy other depenencies

cp_jar ${MAVEN_PATH}/org/apache/spark/spark-unsafe_2.10/1.6.0/spark-unsafe_2.10-1.6.0.jar
cp_jar ${MAVEN_PATH}/com/esotericsoftware/kryo/kryo/2.21/kryo-2.21.jar

cp_jar ${MAVEN_PATH}/com/fasterxml/jackson/core/jackson-core/2.6.2/jackson-core-2.6.2.jar
cp_jar ${MAVEN_PATH}/com/fasterxml/jackson/core/jackson-databind/2.6.2/jackson-databind-2.6.2.jar
cp_jar ${MAVEN_PATH}/com/fasterxml/jackson/core/jackson-annotations/2.6.0/jackson-annotations-2.6.0.jar
cp_jar ${MAVEN_PATH}/net/java/dev/jna/jna/4.2.1/jna-4.2.1.jar
cp_jar ${MAVEN_PATH}/org/jodd/jodd-core/3.6.7/jodd-core-3.6.7.jar

cp_jar ${MAVEN_PATH}/args4j/args4j/2.32/args4j-2.32.jar
cp_jar ${MAVEN_PATH}/com/carrotsearch/hppc/0.7.1/hppc-0.7.1.jar
cp_jar ${MAVEN_PATH}/org/antlr/antlr4-runtime/4.3/antlr4-runtime-4.3.jar
cp_jar ${MAVEN_PATH}/org/antlr/antlr4-annotations/4.3/antlr4-annotations-4.3.jar
cp_jar ${MAVEN_PATH}/org/abego/treelayout/org.abego.treelayout.core/1.0.1/org.abego.treelayout.core-1.0.1.jar
