SCRIPT_DIR=$(cd $(dirname $0);echo $PWD)
cd ${SCRIPT_DIR}/..
ROOTDIR=$(echo $PWD)
source ${ROOTDIR}/script/env.sh

cd ${ROOTDIR}/indexr-segment
mvn jnaerator:generate