package main

import "sharedlog-stream/benchmark/common"

func NewQueryInput(serdeFormat uint8) *common.QueryInput {
	return &common.QueryInput{
		Duration:          uint32(FLAGS_duration),
		EnableTransaction: FLAGS_tran,
		CommitEveryMs:     FLAGS_commit_every,
		SerdeFormat:       serdeFormat,
	}
}
