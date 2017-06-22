#!/usr/bin/env bash
SCRIPT_DIR=$(cd $(dirname $0);echo $PWD)
cd ${SCRIPT_DIR}/..
ROOT_DIR=$(echo $PWD)
source ${ROOT_DIR}/script/env.sh

RELEASE_PATH=${ROOT_DIR}/distribution/indexr-${VERSION}

cd ${ROOT_DIR}

sh ${ROOT_DIR}/script/compile_indexr-spark.sh

rm -rf ${RELEASE_PATH}/indexr-spark/jars
mkdir -p ${RELEASE_PATH}/indexr-spark/jars

function cp_jar {
    if [ ! -f $1 ]; then
        echo "$1 not exists!"
        exit 1
    fi
    cp -f $1 ${RELEASE_PATH}/indexr-spark/jars/
}

cp_jar ${MAVEN_PATH}/io/indexr/indexr-common/${VERSION}/indexr-common-${VERSION}.jar
cp_jar ${MAVEN_PATH}/io/indexr/indexr-segment/${VERSION}/indexr-segment-${VERSION}.jar
cp_jar ${MAVEN_PATH}/io/indexr/indexr-vlt-segment/${VERSION}/indexr-vlt-segment-${VERSION}.jar
cp_jar ${MAVEN_PATH}/io/indexr/indexr-spark/${VERSION}/indexr-spark-${VERSION}.jar

# copy depenencies

cp_jar ${MAVEN_PATH}/net/java/dev/jna/jna/4.2.1/jna-4.2.1.jar
cp_jar ${MAVEN_PATH}/it/unimi/dsi/fastutil/6.5.9/fastutil-6.5.9.jar
