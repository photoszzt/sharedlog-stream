package transaction

import (
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/proc_interface"
	"sharedlog-stream/pkg/source_sink"
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
	commitEvery      time.Duration
	commitEveryNIter uint32
	exitAfterNCommit uint32
	duration         time.Duration
	serdeFormat      commtypes.SerdeFormat
	fixedOutParNum   int16
}

type StreamTaskArgsTransactionBuilder struct {
	stArgs *StreamTaskArgsTransaction
}

func NewStreamTaskArgsTransactionBuilder() SetProcArgs {
	return &StreamTaskArgsTransactionBuilder{
		stArgs: &StreamTaskArgsTransaction{
			fixedOutParNum: -1,
		},
	}
}

type SetProcArgs interface {
	ProcArgs(procArgs proc_interface.ProcArgs) SetEnv
}

type SetEnv interface {
	Env(env types.Environment) SetSrcs
}

type SetSrcs interface {
	Srcs(srcs []source_sink.Source) SetSinks
}

type SetSinks interface {
	Sinks(sinks []source_sink.Sink) SetTransactionalID
}

type SetTransactionalID interface {
	TransactionalID(transactionalID string) SetAppID
}

type SetAppID interface {
	AppID(AppId string) SetWarmup
}

type SetWarmup interface {
	Warmup(time.Duration) SetCommitEvery
}

type SetCommitEvery interface {
	CommitEveryMs(uint64) SetCommitEveryNIter
}

type SetCommitEveryNIter interface {
	CommitEveryNIter(uint32) SetExitAfterNCommit
}

type SetExitAfterNCommit interface {
	ExitAfterNCommit(uint32) SetDuration
}

type SetDuration interface {
	Duration(uint32) SetSerdeFormat
}

type SetSerdeFormat interface {
	SerdeFormat(commtypes.SerdeFormat) BuildStreamTaskArgsTransaction
}

type BuildStreamTaskArgsTransaction interface {
	Build() *StreamTaskArgsTransaction
	WindowStoreChangelogs([]*WindowStoreChangelog) BuildStreamTaskArgsTransaction
	KVStoreChangelogs([]*KVStoreChangelog) BuildStreamTaskArgsTransaction
	FixedOutParNum(uint8) BuildStreamTaskArgsTransaction
}

/*
func NewStreamTaskArgsTransaction(
	env types.Environment,
	transactionalID string,
	procArgs proc_interface.ProcArgs,
	srcs []source_sink.Source,
	sinks []source_sink.Sink,
) *StreamTaskArgsTransaction {
	return &StreamTaskArgsTransaction{
		env:             env,
		procArgs:        procArgs,
		srcs:            srcs,
		sinks:           sinks,
		transactionalId: transactionalID,
		fixedOutParNum:  -1,
	}
}
*/

func (b *StreamTaskArgsTransactionBuilder) ProcArgs(procArgs proc_interface.ProcArgs) SetEnv {
	b.stArgs.procArgs = procArgs
	return b
}
func (b *StreamTaskArgsTransactionBuilder) Env(env types.Environment) SetSrcs {
	b.stArgs.env = env
	return b
}
func (b *StreamTaskArgsTransactionBuilder) Srcs(srcs []source_sink.Source) SetSinks {
	b.stArgs.srcs = srcs
	return b
}
func (b *StreamTaskArgsTransactionBuilder) Sinks(sinks []source_sink.Sink) SetTransactionalID {
	b.stArgs.sinks = sinks
	return b
}
func (b *StreamTaskArgsTransactionBuilder) TransactionalID(transactionalID string) SetAppID {
	b.stArgs.transactionalId = transactionalID
	return b
}

func (args *StreamTaskArgsTransactionBuilder) AppID(appId string) SetWarmup {
	args.stArgs.appId = appId
	return args
}

func (args *StreamTaskArgsTransactionBuilder) Warmup(warmup time.Duration) SetCommitEvery {
	args.stArgs.warmup = warmup
	return args
}

func (args *StreamTaskArgsTransactionBuilder) CommitEveryMs(commitEveryMs uint64) SetCommitEveryNIter {
	args.stArgs.commitEvery = time.Duration(commitEveryMs) * time.Millisecond
	return args
}
func (args *StreamTaskArgsTransactionBuilder) CommitEveryNIter(commitEveryNIter uint32) SetExitAfterNCommit {
	args.stArgs.commitEveryNIter = commitEveryNIter
	return args
}
func (args *StreamTaskArgsTransactionBuilder) ExitAfterNCommit(exitAfterNCommit uint32) SetDuration {
	args.stArgs.exitAfterNCommit = exitAfterNCommit
	return args
}

func (args *StreamTaskArgsTransactionBuilder) Duration(duration uint32) SetSerdeFormat {
	args.stArgs.duration = time.Duration(duration) * time.Second
	return args
}
func (args *StreamTaskArgsTransactionBuilder) SerdeFormat(serdeFormat commtypes.SerdeFormat) BuildStreamTaskArgsTransaction {
	args.stArgs.serdeFormat = serdeFormat
	return args
}

func (args *StreamTaskArgsTransactionBuilder) WindowStoreChangelogs(wschangelogs []*WindowStoreChangelog) BuildStreamTaskArgsTransaction {
	args.stArgs.windowStoreChangelogs = wschangelogs
	return args
}

func (args *StreamTaskArgsTransactionBuilder) KVStoreChangelogs(kvchangelogs []*KVStoreChangelog) BuildStreamTaskArgsTransaction {
	args.stArgs.kvChangelogs = kvchangelogs
	return args
}

func (args *StreamTaskArgsTransactionBuilder) FixedOutParNum(fixedOutParNum uint8) BuildStreamTaskArgsTransaction {
	args.stArgs.fixedOutParNum = int16(fixedOutParNum)
	return args
}

func (args *StreamTaskArgsTransactionBuilder) Build() *StreamTaskArgsTransaction {
	return args.stArgs
}
