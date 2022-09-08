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
./bin/nexmark_scale -app_name $APP_NAME -wconfig ./workload_config/1_ins/${APP_NAME}.json \
    -scconfig ./scale_to_src_unchanged/1_to_2_ins/${APP_NAME}.json \
    -guarantee $GUA -durBF 30 -durAF 60 -comm_everyMS 100 -src_flushms 100 -flushms 100 -serde msgp \
    -stat_dir ./${APP_NAME}_stats -tps 1000 -waitForLast=true -local=true
