#!/usr/bin/env bash
SCRIPT_DIR=$(cd $(dirname $0);echo $PWD)
cd ${SCRIPT_DIR}/..
ROOT_DIR=$(echo $PWD)

# Hack fix.
# Remove generated files by old ways.
rm -rf ${ROOT_DIR}/indexr-query-opt/src/main/java/io/indexr/query/parsers
rm -rf ${ROOT_DIR}/indexr-query-opt/src/main/java/*.tokens

bash ${ROOT_DIR}/script/release_indexr-drill.sh
bash ${ROOT_DIR}/script/release_indexr-hive.sh
bash ${ROOT_DIR}/script/release_indexr-spark.sh
bash ${ROOT_DIR}/script/release_indexr-tool.sh
bash ${ROOT_DIR}/script/release_lib.sh
