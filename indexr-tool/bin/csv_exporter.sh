ROOT_DIR=$(cd $(dirname $0);echo $PWD)
export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/usr/local/lib

bash ${ROOT_DIR}/run_class.sh io.indexr.tool.CSVSegmentExporter "$@"