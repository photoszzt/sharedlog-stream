package stream_task

import (
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/source_sink"
	"sharedlog-stream/pkg/store_restore"
	"time"

	"cs.utexas.edu/zjia/faas/types"
)

type StreamTaskArgs struct {
	procArgs interface{}
	env      types.Environment

	srcs                  []source_sink.Source
	sinks                 []source_sink.Sink
	windowStoreChangelogs []*store_restore.WindowStoreChangelog
	kvChangelogs          []*store_restore.KVStoreChangelog

	duration       time.Duration
	warmup         time.Duration
	flushEvery     time.Duration
	serdeFormat    commtypes.SerdeFormat
	parNum         uint8
	numInPartition uint8
}

func NewStreamTaskArgs(env types.Environment, procArgs interface{}, srcs []source_sink.Source, sinks []source_sink.Sink) *StreamTaskArgs {
	return &StreamTaskArgs{
		env:      env,
		procArgs: procArgs,
		srcs:     srcs,
		sinks:    sinks,
	}
}

func (args *StreamTaskArgs) WithWindowStoreChangelogs(wschangelogs []*store_restore.WindowStoreChangelog) *StreamTaskArgs {
	args.windowStoreChangelogs = wschangelogs
	return args
}

func (args *StreamTaskArgs) WithKVChangelogs(kvchangelogs []*store_restore.KVStoreChangelog) *StreamTaskArgs {
	args.kvChangelogs = kvchangelogs
	return args
}

func (args *StreamTaskArgs) WithDuration(duration time.Duration) *StreamTaskArgs {
	args.duration = duration
	return args
}

func (args *StreamTaskArgs) WithWarmup(warmup time.Duration) *StreamTaskArgs {
	args.warmup = warmup
	return args
}

func (args *StreamTaskArgs) WithFlushEvery(flushEvery time.Duration) *StreamTaskArgs {
	args.flushEvery = flushEvery
	return args
}

func (args *StreamTaskArgs) WithSerdeFormat(serdeFormat commtypes.SerdeFormat) *StreamTaskArgs {
	args.serdeFormat = serdeFormat
	return args
}

func (args *StreamTaskArgs) WithParNum(parNum uint8) *StreamTaskArgs {
	args.parNum = parNum
	return args
}

func (args *StreamTaskArgs) WithNumInPartition(numInPartition uint8) *StreamTaskArgs {
	args.numInPartition = numInPartition
	return args
}
