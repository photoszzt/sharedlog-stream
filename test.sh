#!/bin/bash

if [ "$1" = "" ]; then
    echo "should provide app name"
    exit 1
fi

if [ "$2" = "" ]; then
    echo "should provide test source"
    exit 1
fi

APP_NAME=$1
TEST_SRC=$(realpath $2)
GUA=${3:-alo}
DUMP_DIR=$(realpath ./dump/)
#DUMP_DIR=""
DUMP_DIR=${DUMP_DIR}/${APP_NAME}_$GUA
./bin/nexmark_client -app_name $APP_NAME -wconfig ./test_workload_config/${APP_NAME}.json \
    -guarantee $GUA -comm_every_niter 0 -duration 10 -comm_everyMS 5 -flushms 5 -tab_type mem -serde json \
    -stat_dir ./${APP_NAME}_stats -testsrc ${TEST_SRC} -dumpdir ${DUMP_DIR} -local true 
