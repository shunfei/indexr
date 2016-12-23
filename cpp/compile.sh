SCRIPT_DIR=$(cd $(dirname $0);echo $PWD)
ROOT_DIR=${SCRIPT_DIR}

# local c++ lib env
export INCLUDES=/usr/local/include
export BOOST_LIBS_DIR=/usr/local/lib

export CPLUS_INCLUDE_PATH=${CPLUS_INCLUDE_PATH}:${INCLUDES}
export C_INCLUDE_PATH=${C_INCLUDE_PATH}:${INCLUDES}
export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:${BOOST_LIBS_DIR}
export LIBRARY_PATH=${LIBRARY_PATH}:${BOOST_LIBS_DIR}

BHROOT=${ROOT_DIR}/bh
LIBDIR=${ROOT_DIR}/lib
mkdir -p ${LIBDIR}
CPP_OUT_DIR=${ROOT_DIR}/cpp-out

#######################
# Compile compress lib

if [ ! -f "${BOOST_LIBS_DIR}/libboost_system.a" ]; then
    echo "lib not found: ${BOOST_LIBS_DIR}/libboost_system.a"
    exit 1
fi

mkdir -p ${CPP_OUT_DIR}
echo "cd ${CPP_OUT_DIR}"
cd ${CPP_OUT_DIR}

if [[ "${OSTYPE}" == "linux"* ]] || [[ "${OSTYPE}" == "cygwin"* ]]; then
    BHLIB=libbhcompress.so
elif [[ "${OSTYPE}" == "darwin"* ]]; then
    BHLIB=libbhcompress.dylib
else
    echo "Platform ${OSTYPE} not supported!"
    exit 1
fi

if [[ "x$1" == "x-j" ]]; then
    echo "Compiling ${BHROOT}/compress/JavaCompress.cpp"
    rm -f ${CPP_OUT_DIR}/JavaCompress.o
    g++ -O3 -c -fPIC -I"${BHROOT}" -I"${BHROOT}/community" ${BHROOT}/compress/JavaCompress.cpp
else
    echo "Compiling ${BHROOT}/compress/*.cpp"
    rm -f ${CPP_OUT_DIR}/*.o
    g++ -O3 -c -fPIC -I"${BHROOT}" -I"${BHROOT}/community" ${BHROOT}/compress/*.cpp
fi

rm -f ${LIBDIR}/libbhcompress.*
#g++ -shared -O3 -o ${LIBDIR}/${BHLIB} ${CPP_OUT_DIR}/*.o ${BOOST_LIBS_DIR}/libboost_system.a
g++ -shared -fPIC -O3 -o ${LIBDIR}/${BHLIB} ${CPP_OUT_DIR}/*.o ${BOOST_LIBS_DIR}/libboost_system.a

if [ ! "x$?" = "x0" ]; then
    exit $?
fi

echo "Generated ${LIBDIR}/${BHLIB}"

# Compilation done.
#######################

rm -rf ${CPP_OUT_DIR}
