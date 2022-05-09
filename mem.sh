#!/bin/bash

if [ "$1" = "" ]; then
    echo "should provide app name"
    exit 1
fi

APP_NAME=$1
TRAN=${2:-false}
if [ "$TRAN" = "true" ]; then
    ./bin/nexmark_client -app_name $APP_NAME -wconfig ./workload_config/${APP_NAME}.json \
        -tran -comm_every_niter 0 -duration 35 -comm_everyMS 100 -tab_type mem -serde msgp \
        -stat_dir ./${APP_NAME}_stats -tps 10000 -events_num 350000 -warmup_time 15 -warmup_events 150000 -local true
else
    ./bin/nexmark_client -app_name $APP_NAME -wconfig ./workload_config/${APP_NAME}.json \
        -duration 35 -tab_type mem -serde msgp -stat_dir ./${APP_NAME}_stats \
        -tps 10000 -events_num 350000 -warmup_time 15 -warmup_events 150000 -local true
fi
