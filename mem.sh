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
./bin/nexmark_client -app_name $APP_NAME -wconfig ./workload_config/4_ins/${APP_NAME}.json \
    -guarantee $GUA -duration 60 -comm_everyMS 5 -flushms 5 -tab_type mem -serde json \
    -stat_dir ./${APP_NAME}_stats -tps 4000 -events_num 120000 -local true 
