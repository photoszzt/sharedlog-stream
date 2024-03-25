#!/bin/bash

if [ "$1" = "" ]; then
    echo "should provide guarantee name: <one of none, alo, 2pc, align_chkpt and epoch>"
    exit
fi

GUA=${1:-alo}
./bin/nexmark_client -app_name fanout -wconfig ./workload_config/4node/4_ins/fanout_32.json \
    -guarantee $GUA -duration 60 -comm_everyMS 100 -src_flushms 10 -flushms 100 -serde msgp \
    -stat_dir ./${APP_NAME}_stats -tps 1000 -events_num 60000 -waitForLast=true -local=true \
    -snapshot_everyS=10
