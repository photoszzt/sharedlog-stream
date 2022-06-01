package transaction

import (
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/source_sink"
	"time"

	"cs.utexas.edu/zjia/faas/types"
)

type StreamTaskArgs struct {
	procArgs interface{}
	env      types.Environment

	srcs                  []source_sink.Source
	sinks                 []source_sink.Sink
	windowStoreChangelogs []*WindowStoreChangelog
	kvChangelogs          []*KVStoreChangelog

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

func (args *StreamTaskArgs) WithWindowStoreChangelogs(wschangelogs []*WindowStoreChangelog) *StreamTaskArgs {
	args.windowStoreChangelogs = wschangelogs
	return args
}

func (args *StreamTaskArgs) WithKVChangelogs(kvchangelogs []*KVStoreChangelog) *StreamTaskArgs {
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
