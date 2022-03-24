package main

import "sharedlog-stream/benchmark/common"

func NewQueryInput(serdeFormat uint8) *common.QueryInput {
	return &common.QueryInput{
		Duration:          uint32(FLAGS_duration),
		EnableTransaction: FLAGS_tran,
		CommitEveryMs:     FLAGS_commit_everyMs,
		CommitEveryNIter:  uint32(FLAGS_commit_every_niter),
		ExitAfterNCommit:  uint32(FLAGS_exit_after_ncomm),
		SerdeFormat:       serdeFormat,
	}
}
