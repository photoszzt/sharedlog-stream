#!/bin/bash

if [ "$1" = "" ]; then
    echo "should provide app name"
    exit 1
fi

if [ "$2" = "" ]; then
    echo "should provide guarantee name: <one of alo, 2pc and epoch>"
    exit
fi

APP_NAME=$1
GUA=${2:-alo}
./bin/nexmark_scale -app_name $APP_NAME -wconfig ./workload_config/4node/2_ins/${APP_NAME}.json \
    -scconfig ./scale_to_src_unchanged/2_to_4_ins/${APP_NAME}.json \
    -guarantee $GUA -durBF 5 -durAF 90 -comm_everyMS 100 -src_flushms 100 -flushms 100 -serde msgp \
    -stat_dir ./${APP_NAME}_stats -tps 1000 -events_num 95000 -waitForLast=true -local=true \
    -snapshot_everyS 10
