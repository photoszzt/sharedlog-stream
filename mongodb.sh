#!/bin/bash

if [ "$1" = "" ]; then
    echo "should provide app name"
    exit 1
fi

APP_NAME=$1
TRAN=${2:-false}
if [ "$TRAN" = "true" ]; then
    ./bin/nexmark_client -app_name $APP_NAME -wconfig ./workload_config/${APP_NAME}.json \
        --tran -comm_every_niter 100 --duration 20  -comm_everyMS 0 -tab_type mongodb -serde msgp \
        -mongo_addr "mongodb://localhost:27017,localhost:27021,localhost:27022/?replicaSet=replicaset"
else
    ./bin/nexmark_client -app_name $APP_NAME -wconfig ./workload_config/${APP_NAME}.json \
        --duration 20 -tab_type mongodb -serde msgp \
        -mongo_addr "mongodb://localhost:27017,localhost:27021,localhost:27022/?replicaSet=replicaset"
fi
