SCRIPT_DIR=$(cd $(dirname $0);echo $PWD)
cd ${SCRIPT_DIR}/..
ROOT_DIR=$(echo $PWD)
source ${ROOT_DIR}/script/env.sh

LIBDIR=${ROOT_DIR}/lib
mkdir -p ${LIBDIR}

# link lib into sub modules, convenient for testing.
if [ ! -d "${ROOT_DIR}/indexr-segment/lib" ]; then
    ln -s ${LIBDIR} ${ROOT_DIR}/indexr-segment/lib
fi
if [ ! -d "${ROOT_DIR}/indexr-server/lib" ]; then
    ln -s ${LIBDIR} ${ROOT_DIR}/indexr-server/lib
fi

mkdir -p ${ROOT_DIR}/distribution/lib
cp -f ${LIBDIR}/* ${ROOT_DIR}/distribution/lib/
