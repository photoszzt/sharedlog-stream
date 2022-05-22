package transaction

import (
	"sharedlog-stream/pkg/source_sink"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/stream/processor/proc_interface"
	"time"

	"cs.utexas.edu/zjia/faas/types"
)

type StreamTaskArgsTransaction struct {
	procArgs        proc_interface.ProcArgs
	env             types.Environment
	srcs            []source_sink.Source
	sinks           []source_sink.Sink
	appId           string
	transactionalId string

	windowStoreChangelogs []*WindowStoreChangelog
	kvChangelogs          []*KVStoreChangelog

	warmup           time.Duration
	scaleEpoch       uint64
	commitEvery      time.Duration
	commitEveryNIter uint32
	exitAfterNCommit uint32
	duration         time.Duration
	serdeFormat      commtypes.SerdeFormat
	inParNum         uint8

	fixedOutParNum uint8
}

func NewStreamTaskArgsTransaction(env types.Environment, transactionalID string,
	procArgs proc_interface.ProcArgs, srcs []source_sink.Source, sinks []source_sink.Sink,
) *StreamTaskArgsTransaction {
	return &StreamTaskArgsTransaction{
		env:             env,
		procArgs:        procArgs,
		srcs:            srcs,
		sinks:           sinks,
		transactionalId: transactionalID,
	}
}

func (args *StreamTaskArgsTransaction) WithWindowStoreChangelogs(wschangelogs []*WindowStoreChangelog) *StreamTaskArgsTransaction {
	args.windowStoreChangelogs = wschangelogs
	return args
}

func (args *StreamTaskArgsTransaction) WithKVChangelogs(kvchangelogs []*KVStoreChangelog) *StreamTaskArgsTransaction {
	args.kvChangelogs = kvchangelogs
	return args
}

func (args *StreamTaskArgsTransaction) WithAppID(appId string) *StreamTaskArgsTransaction {
	args.appId = appId
	return args
}

func (args *StreamTaskArgsTransaction) WithFixedOutParNum(fixedOutParNum uint8) *StreamTaskArgsTransaction {
	args.fixedOutParNum = fixedOutParNum
	return args
}

func (args *StreamTaskArgsTransaction) WithWarmup(warmup time.Duration) *StreamTaskArgsTransaction {
	args.warmup = warmup
	return args
}

func (args *StreamTaskArgsTransaction) WithScaleEpoch(scaleEpoch uint64) *StreamTaskArgsTransaction {
	args.scaleEpoch = scaleEpoch
	return args
}

func (args *StreamTaskArgsTransaction) WithCommitEveryMs(commitEveryMs uint64) *StreamTaskArgsTransaction {
	args.commitEvery = time.Duration(commitEveryMs) * time.Millisecond
	return args
}
func (args *StreamTaskArgsTransaction) WithCommitEveryNIter(commitEveryNIter uint32) *StreamTaskArgsTransaction {
	args.commitEveryNIter = commitEveryNIter
	return args
}
func (args *StreamTaskArgsTransaction) WithExitAfterNCommit(exitAfterNCommit uint32) *StreamTaskArgsTransaction {
	args.exitAfterNCommit = exitAfterNCommit
	return args
}

func (args *StreamTaskArgsTransaction) WithDuration(duration uint32) *StreamTaskArgsTransaction {
	args.duration = time.Duration(duration) * time.Second
	return args
}
func (args *StreamTaskArgsTransaction) WithSerdeFormat(serdeFormat commtypes.SerdeFormat) *StreamTaskArgsTransaction {
	args.serdeFormat = serdeFormat
	return args
}
func (args *StreamTaskArgsTransaction) WithInParNum(inParNum uint8) *StreamTaskArgsTransaction {
	args.inParNum = inParNum
	return args
}
