package main

import (
	"fmt"
	"os"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/pkg/stream/processor/store"
)

func NewQueryInput(serdeFormat uint8) *common.QueryInput {
	var table_type store.TABLE_TYPE
	if FLAGS_table_type == "mem" {
		table_type = store.IN_MEM
	} else if FLAGS_table_type == "mongodb" {
		table_type = store.MONGODB
		if FLAGS_mongo_addr == "" {
			panic("should specify mongodb address")
		}
	} else {
		panic(fmt.Sprintf("unrecognized table type: %s", FLAGS_app_name))
	}
	fmt.Fprintf(os.Stderr, "warmup: %d\n", FLAGS_warmup_time)
	return &common.QueryInput{
		Duration:          uint32(FLAGS_duration),
		EnableTransaction: FLAGS_tran,
		CommitEveryMs:     FLAGS_commit_everyMs,
		CommitEveryNIter:  uint32(FLAGS_commit_every_niter),
		ExitAfterNCommit:  uint32(FLAGS_exit_after_ncomm),
		SerdeFormat:       serdeFormat,
		AppId:             FLAGS_app_name,
		TableType:         uint8(table_type),
		MongoAddr:         FLAGS_mongo_addr,
		FlushMs:           uint32(FLAGS_flush_ms),
		WarmupS:           uint32(FLAGS_warmup_time),
	}
}
