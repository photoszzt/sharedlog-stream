package stream_task

import (
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/exactly_once_intr"
	"sharedlog-stream/pkg/processor"
	"sharedlog-stream/pkg/store"
	"time"

	"cs.utexas.edu/zjia/faas/types"
)

type StreamTaskArgs struct {
	ectx processor.ExecutionContext
	env  types.Environment

	appId           string
	transactionalId string

	windowStoreChangelogs []store.WindowStoreOpWithChangelog
	kvChangelogs          []store.KeyValueStoreOpWithChangelog

	warmup time.Duration

	// exactly once: commitEvery overwrites flushEvery
	commitEvery time.Duration

	// for at least once
	flushEvery               time.Duration
	trackEveryForAtLeastOnce time.Duration

	duration       time.Duration
	serdeFormat    commtypes.SerdeFormat
	fixedOutParNum int16
	guarantee      exactly_once_intr.GuaranteeMth
}

// func (s *StreamTaskArgs) LockProducerConsumer() {
// 	s.ectx.LockProducerConsumer()
// }
//
// func (s *StreamTaskArgs) UnlockProducerConsumer() {
// 	s.ectx.UnlockProducerConsumer()
// }
//
// func (s *StreamTaskArgs) LockProducer() {
// 	s.ectx.LockProducer()
// }
//
// func (s *StreamTaskArgs) UnlockProducer() {
// 	s.ectx.UnlockProducer()
// }

type StreamTaskArgsBuilder struct {
	stArgs *StreamTaskArgs
}

func NewStreamTaskArgsBuilder(env types.Environment,
	ectx processor.ExecutionContext,
	transactionalID string,
) SetGuarantee {
	return &StreamTaskArgsBuilder{
		stArgs: &StreamTaskArgs{
			ectx:                     ectx,
			env:                      env,
			transactionalId:          transactionalID,
			fixedOutParNum:           -1,
			guarantee:                exactly_once_intr.AT_LEAST_ONCE,
			trackEveryForAtLeastOnce: common.CommitDuration,
		},
	}
}

type SetGuarantee interface {
	Guarantee(gua exactly_once_intr.GuaranteeMth) SetAppID
}

type SetAppID interface {
	AppID(AppId string) SetWarmup
}

type SetWarmup interface {
	Warmup(time.Duration) SetCommitEvery
}

type SetCommitEvery interface {
	CommitEveryMs(uint64) SetFlushEveryMs
}

type SetFlushEveryMs interface {
	FlushEveryMs(uint32) SetDuration
}

type SetDuration interface {
	Duration(uint32) SetSerdeFormat
}

type SetSerdeFormat interface {
	SerdeFormat(commtypes.SerdeFormat) BuildStreamTaskArgs
}

type BuildStreamTaskArgs interface {
	Build() *StreamTaskArgs
	WindowStoreChangelogs([]store.WindowStoreOpWithChangelog) BuildStreamTaskArgs
	KVStoreChangelogs([]store.KeyValueStoreOpWithChangelog) BuildStreamTaskArgs
	FixedOutParNum(uint8) BuildStreamTaskArgs
}

func (args *StreamTaskArgsBuilder) Guarantee(gua exactly_once_intr.GuaranteeMth) SetAppID {
	args.stArgs.guarantee = gua
	return args
}

func (args *StreamTaskArgsBuilder) AppID(appId string) SetWarmup {
	args.stArgs.appId = appId
	return args
}

func (args *StreamTaskArgsBuilder) Warmup(warmup time.Duration) SetCommitEvery {
	args.stArgs.warmup = warmup
	return args
}

func (args *StreamTaskArgsBuilder) CommitEveryMs(commitEveryMs uint64) SetFlushEveryMs {
	args.stArgs.commitEvery = time.Duration(commitEveryMs) * time.Millisecond
	return args
}

func (args *StreamTaskArgsBuilder) FlushEveryMs(flushEveryMs uint32) SetDuration {
	args.stArgs.flushEvery = time.Duration(flushEveryMs) * time.Millisecond
	return args
}

func (args *StreamTaskArgsBuilder) Duration(duration uint32) SetSerdeFormat {
	args.stArgs.duration = time.Duration(duration) * time.Second
	return args
}
func (args *StreamTaskArgsBuilder) SerdeFormat(serdeFormat commtypes.SerdeFormat) BuildStreamTaskArgs {
	args.stArgs.serdeFormat = serdeFormat
	return args
}

func (args *StreamTaskArgsBuilder) WindowStoreChangelogs(wschangelogs []store.WindowStoreOpWithChangelog) BuildStreamTaskArgs {
	args.stArgs.windowStoreChangelogs = wschangelogs
	return args
}

func (args *StreamTaskArgsBuilder) KVStoreChangelogs(kvchangelogs []store.KeyValueStoreOpWithChangelog) BuildStreamTaskArgs {
	args.stArgs.kvChangelogs = kvchangelogs
	return args
}

func (args *StreamTaskArgsBuilder) FixedOutParNum(fixedOutParNum uint8) BuildStreamTaskArgs {
	args.stArgs.fixedOutParNum = int16(fixedOutParNum)
	return args
}

func (args *StreamTaskArgsBuilder) Build() *StreamTaskArgs {
	return args.stArgs
}
