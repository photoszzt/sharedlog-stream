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
	ectx                  processor.ExecutionContext
	env                   types.Environment
	windowStoreChangelogs map[string]store.WindowStoreOpWithChangelog
	kvChangelogs          map[string]store.KeyValueStoreOpWithChangelog
	testParams            map[string]commtypes.FailParam
	appId                 string
	transactionalId       string
	flushEvery            time.Duration
	// exactly once: commitEvery overwrites flushEvery if commitEvery < flushEvery
	commitEvery              time.Duration
	snapshotEvery            time.Duration
	trackEveryForAtLeastOnce time.Duration
	duration                 time.Duration
	warmup                   time.Duration
	bufMaxSize               uint32
	fixedOutParNum           int16
	serdeFormat              commtypes.SerdeFormat
	guarantee                exactly_once_intr.GuaranteeMth
	waitEndMark              bool
}

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

func (args *StreamTaskArgs) ExecutionContext() processor.ExecutionContext {
	return args.ectx
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
	SerdeFormat(commtypes.SerdeFormat) SetBufMaxSize
}

type SetBufMaxSize interface {
	BufMaxSize(uint32) BuildStreamTaskArgs
}

type BuildStreamTaskArgs interface {
	Build() *StreamTaskArgs
	WindowStoreChangelogs(map[string]store.WindowStoreOpWithChangelog) BuildStreamTaskArgs
	KVStoreChangelogs(map[string]store.KeyValueStoreOpWithChangelog) BuildStreamTaskArgs
	FixedOutParNum(uint8) BuildStreamTaskArgs
	WaitEndMark(bool) BuildStreamTaskArgs
	TestParams(map[string]commtypes.FailParam) BuildStreamTaskArgs
	SnapshotEveryS(uint32) BuildStreamTaskArgs
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

func (args *StreamTaskArgsBuilder) SnapshotEveryS(snapshotEveryS uint32) BuildStreamTaskArgs {
	args.stArgs.snapshotEvery = time.Duration(snapshotEveryS) * time.Second
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
func (args *StreamTaskArgsBuilder) SerdeFormat(serdeFormat commtypes.SerdeFormat) SetBufMaxSize {
	args.stArgs.serdeFormat = serdeFormat
	return args
}
func (args *StreamTaskArgsBuilder) BufMaxSize(bufMaxSize uint32) BuildStreamTaskArgs {
	args.stArgs.bufMaxSize = bufMaxSize
	return args
}

func (args *StreamTaskArgsBuilder) WindowStoreChangelogs(wschangelogs map[string]store.WindowStoreOpWithChangelog) BuildStreamTaskArgs {
	args.stArgs.windowStoreChangelogs = wschangelogs
	return args
}

func (args *StreamTaskArgsBuilder) KVStoreChangelogs(kvchangelogs map[string]store.KeyValueStoreOpWithChangelog) BuildStreamTaskArgs {
	args.stArgs.kvChangelogs = kvchangelogs
	return args
}

func (args *StreamTaskArgsBuilder) FixedOutParNum(fixedOutParNum uint8) BuildStreamTaskArgs {
	args.stArgs.fixedOutParNum = int16(fixedOutParNum)
	return args
}

func (args *StreamTaskArgsBuilder) WaitEndMark(waitEndMark bool) BuildStreamTaskArgs {
	args.stArgs.waitEndMark = waitEndMark
	return args
}

func (args *StreamTaskArgsBuilder) TestParams(testParams map[string]commtypes.FailParam) BuildStreamTaskArgs {
	args.stArgs.testParams = testParams
	return args
}

func (args *StreamTaskArgsBuilder) Build() *StreamTaskArgs {
	return args.stArgs
}
