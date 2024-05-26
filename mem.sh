#!/bin/bash

if [ "$1" = "" ]; then
    echo "should provide app name"
    exit 1
fi

if [ "$2" = "" ]; then
    echo "should provide guarantee name: <one of none, alo, 2pc, align_chkpt and epoch, remote_2pc>"
    exit
fi

APP_NAME=$1
GUA=${2:-alo}
CHKPT_MNGR_ADDR="127.0.0.1:6060" \
./bin/nexmark_client -app_name $APP_NAME -wconfig ./workload_config/4node/4_ins/${APP_NAME}.json \
    -guarantee $GUA -duration 30 -comm_everyMS 100 -src_flushms 100 -flushms 100 -serde msgp \
    -stat_dir ./${APP_NAME}_stats -tps 1000 -events_num 30000 -waitForLast=true \
    -snapshot_everyS=10 -dumpdir=$(realpath ./${APP_NAME}_out) -engine1=127.0.0.1:6061
