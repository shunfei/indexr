#!/usr/bin/env bash
SCRIPT_DIR=$(cd $(dirname $0);echo $PWD)
cd ${SCRIPT_DIR}/..
ROOT_DIR=$(echo $PWD)

# Remove generated files by old ways.
rm -rf ${ROOT_DIR}/indexr-query-opt/src/main/java/io/indexr/query/parsers
rm -rf ${ROOT_DIR}/indexr-query-opt/src/main/java/*.tokens

cd ${ROOT_DIR}
mvn clean install -DskipTests=true -pl indexr-query-opt -am