#!/bin/bash
./bin/sharedlog_protocol_lat \
    -payload /mnt/data/src/stream_workspace/sharedlog-stream/data/payload-16Kb.data \
    -events_num 300 -duration 30 -tps 10 -comm_everyMS 10 -local=true -serde=msgp
