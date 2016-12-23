SCRIPT_DIR=$(cd $(dirname $0);echo $PWD)
cd ${SCRIPT_DIR}/..
ROOTDIR=$(echo $PWD)

cd ${ROOTDIR}/indexr-query-opt
mvn clean install