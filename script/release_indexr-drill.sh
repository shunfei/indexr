SCRIPT_DIR=$(cd $(dirname $0);echo $PWD)
cd ${SCRIPT_DIR}/..
ROOT_DIR=$(echo $PWD)
source ${ROOT_DIR}/script/env.sh

cd ${ROOT_DIR}

sh ${ROOT_DIR}/script/compile_indexr-server.sh

# copy drill files
mkdir -p ${ROOT_DIR}/distribution/indexr-drill/jars/3rdparty
cp -f ${ROOT_DIR}/indexr-common/target/indexr-common-${VERSION}.jar ${ROOT_DIR}/distribution/indexr-drill/jars/3rdparty/
cp -f ${ROOT_DIR}/indexr-segment/target/indexr-segment-${VERSION}.jar ${ROOT_DIR}/distribution/indexr-drill/jars/3rdparty/
cp -f ${ROOT_DIR}/indexr-server/target/indexr-server${VERSION}.jar ${ROOT_DIR}/distribution/indexr-drill/jars/3rdparty/

mkdir -p ${ROOT_DIR}/distribution/indexr-drill/conf
cp -f ${ROOT_DIR}/indexr-server/config/indexr.config.properties ${ROOT_DIR}/distribution/indexr-drill/conf/
