package transaction

import (
	"context"
	"fmt"
	"os"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/errors"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/source_sink"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/stream/processor/proc_interface"
	"sharedlog-stream/pkg/stream/processor/store"
	"sharedlog-stream/pkg/transaction/tran_interface"
	"sharedlog-stream/pkg/txn_data"
	"sync"
	"time"

	"cs.utexas.edu/zjia/faas/types"
	"golang.org/x/xerrors"
)

type StreamTask struct {
	ProcessFunc               func(ctx context.Context, task *StreamTask, args interface{}) *common.FnOutput
	OffMu                     sync.Mutex
	CurrentOffset             map[string]uint64
	PauseFunc                 func() *common.FnOutput
	ResumeFunc                func(task *StreamTask)
	InitFunc                  func(progArgs interface{})
	CommitEveryForAtLeastOnce time.Duration
}

/*
func NewStreamTask(
	processFunc func(ctx context.Context, task *StreamTask, args interface{}) (map[string]uint64, *common.FnOutput),
	currentOffset map[string]uint64,
	commitEvery time.Duration,
	closeFunc func(),
	pauseFunc func(),
	resumeFunc func(),
	initFunc func(progArgs interface{}),
) *StreamTask {
	t := &StreamTask{
		ProcessFunc:   processFunc,
		CurrentOffset: currentOffset,
		CommitEvery:   commitEvery,
		CloseFunc:     closeFunc,
		PauseFunc:     pauseFunc,
		ResumeFunc:    resumeFunc,
		InitFunc:      initFunc,
	}
	t.runCond = sync.NewCond(t)
	return t
}
*/

func DefaultRecordPrevInstanceFinishFunc(ctx context.Context,
	appId string, instanceId uint8,
) error {
	return nil
}

// finding the last commited marker and gets the marker's seq number
// used in restore and in one thread
func createOffsetTopicAndGetOffset(ctx context.Context, tm *TransactionManager,
	topic string, numPartition uint8, parNum uint8,
) (uint64, error) {
	err := tm.createOffsetTopic(topic, numPartition)
	if err != nil {
		return 0, fmt.Errorf("create offset topic failed: %v", err)
	}
	debug.Fprintf(os.Stderr, "created offset topic\n")
	offset, err := tm.FindLastConsumedSeqNum(ctx, topic, parNum)
	if err != nil {
		if !errors.IsStreamEmptyError(err) {
			return 0, err
		}
	}
	return offset, nil
}

type RecordPrevInstanceFinishFunc func(ctx context.Context, appId string, instanceID uint8) error

func SetupManagersAndProcessTransactional(ctx context.Context,
	env types.Environment,
	streamTaskArgs *StreamTaskArgsTransaction,
	updateProcArgs func(procArgs interface{}, trackParFunc tran_interface.TrackKeySubStreamFunc,
		recordFinish RecordPrevInstanceFinishFunc),
	task *StreamTask,
) *common.FnOutput {
	tm, cmm, err := SetupManagers(ctx, env, streamTaskArgs, updateProcArgs, task)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	debug.Fprint(os.Stderr, "begin transaction processing\n")
	ret := task.ProcessWithTransaction(ctx, tm, cmm, streamTaskArgs)
	return ret
}

func SetupManagers(ctx context.Context, env types.Environment,
	streamTaskArgs *StreamTaskArgsTransaction,
	updateProcArgs func(procArgs interface{}, trackParFunc tran_interface.TrackKeySubStreamFunc,
		recordFinish RecordPrevInstanceFinishFunc),
	task *StreamTask,
) (*TransactionManager, *ControlChannelManager, error) {
	debug.Fprint(os.Stderr, "setup transaction and control manager\n")
	tm, err := SetupTransactionManager(ctx, streamTaskArgs)
	if err != nil {
		return nil, nil, err
	}
	cmm, err := NewControlChannelManager(env, streamTaskArgs.AppId,
		commtypes.SerdeFormat(streamTaskArgs.SerdeFormat), streamTaskArgs.ScaleEpoch)
	if err != nil {
		return nil, nil, err
	}
	debug.Fprint(os.Stderr, "start restore\n")
	offsetMap, err := getOffsetMap(ctx, tm, streamTaskArgs)
	if err != nil {
		return nil, nil, err
	}
	debug.Fprintf(os.Stderr, "got offset map: %v\n", offsetMap)
	err = restoreStateStore(ctx, tm, task, streamTaskArgs, offsetMap)
	if err != nil {
		return nil, nil, err
	}
	setOffsetOnStream(offsetMap, streamTaskArgs)
	debug.Fprintf(os.Stderr, "down restore\n")
	trackParFunc := func(ctx context.Context,
		key interface{},
		keySerde commtypes.Serde,
		topicName string,
		substreamId uint8,
	) error {
		err := tm.AddTopicPartition(ctx, topicName, []uint8{substreamId})
		if err != nil {
			return err
		}
		err = cmm.TrackAndAppendKeyMapping(ctx, key, keySerde, substreamId, topicName)
		return err
	}
	recordFinish := func(ctx context.Context, funcName string, instanceID uint8) error {
		return cmm.RecordPrevInstanceFinish(ctx, funcName, instanceID, cmm.currentEpoch)
	}
	updateProcArgs(streamTaskArgs.ProcArgs, trackParFunc, recordFinish)
	return tm, cmm, nil
}

func getOffsetMap(ctx context.Context, tm *TransactionManager, args *StreamTaskArgsTransaction) (map[string]uint64, error) {
	offsetMap := make(map[string]uint64)
	for _, src := range args.Srcs {
		inputTopicName := src.TopicName()
		offset, err := createOffsetTopicAndGetOffset(ctx, tm, inputTopicName,
			uint8(src.Stream().NumPartition()), args.InParNum)
		if err != nil {
			return nil, fmt.Errorf("createOffsetTopicAndGetOffset failed: %v", err)
		}
		offsetMap[inputTopicName] = offset
		debug.Fprintf(os.Stderr, "tp %s offset %d\n", inputTopicName, offset)
	}
	return offsetMap, nil
}

func setOffsetOnStream(offsetMap map[string]uint64, args *StreamTaskArgsTransaction) {
	for _, src := range args.Srcs {
		inputTopicName := src.TopicName()
		offset := offsetMap[inputTopicName]
		src.SetCursor(offset+1, args.InParNum)
	}
}

func restoreKVStore(ctx context.Context, tm *TransactionManager,
	t *StreamTask,
	args *StreamTaskArgsTransaction, offsetMap map[string]uint64,
) error {
	for _, kvchangelog := range args.KVChangelogs {
		if kvchangelog.KVStore.TableType() == store.IN_MEM {
			topic := kvchangelog.ChangelogManager.TopicName()
			// offset stream is input stream
			if offset, ok := offsetMap[topic]; ok && offset != 0 {
				err := RestoreChangelogKVStateStore(ctx,
					kvchangelog, offset)
				if err != nil {
					return fmt.Errorf("RestoreKVStateStore failed: %v", err)
				}
			} else {
				offset, err := createOffsetTopicAndGetOffset(ctx, tm, topic,
					kvchangelog.ChangelogManager.NumPartition(), kvchangelog.ParNum)
				if err != nil {
					return fmt.Errorf("createOffsetTopicAndGetOffset kv failed: %v", err)
				}
				if offset != 0 {
					err = RestoreChangelogKVStateStore(ctx, kvchangelog, offset)
					if err != nil {
						return fmt.Errorf("RestoreKVStateStore2 failed: %v", err)
					}
				}
			}
		} else if kvchangelog.KVStore.TableType() == store.MONGODB {
			if err := restoreMongoDBKVStore(ctx, tm, t, kvchangelog); err != nil {
				return err
			}
		}
	}
	return nil
}

func restoreChangelogBackedWindowStore(ctx context.Context, tm *TransactionManager, t *StreamTask, args *StreamTaskArgsTransaction, offsetMap map[string]uint64) error {
	for _, wschangelog := range args.WindowStoreChangelogs {
		if wschangelog.WinStore.TableType() == store.IN_MEM {
			topic := wschangelog.ChangelogManager.TopicName()
			// offset stream is input stream
			if offset, ok := offsetMap[topic]; ok && offset != 0 {
				err := RestoreChangelogWindowStateStore(ctx, wschangelog, offset)
				if err != nil {
					return fmt.Errorf("RestoreWindowStateStore failed: %v", err)
				}
			} else {
				offset, err := createOffsetTopicAndGetOffset(ctx, tm, topic,
					wschangelog.ChangelogManager.NumPartition(), wschangelog.ParNum)
				if err != nil {
					return fmt.Errorf("createOffsetTopicAndGetOffset win failed: %v", err)
				}
				if offset != 0 {
					err = RestoreChangelogWindowStateStore(ctx, wschangelog, offset)
					if err != nil {
						return fmt.Errorf("RestoreWindowStateStore2 failed: %v", err)
					}
				}
			}
		} else if wschangelog.WinStore.TableType() == store.MONGODB {
			if err := restoreMongoDBWinStore(ctx, tm, t, wschangelog); err != nil {
				return err
			}
		}
	}
	return nil
}

func restoreStateStore(ctx context.Context, tm *TransactionManager, t *StreamTask, args *StreamTaskArgsTransaction, offsetMap map[string]uint64) error {
	if args.KVChangelogs != nil {
		err := restoreKVStore(ctx, tm, t, args, offsetMap)
		if err != nil {
			return err
		}
	}
	if args.WindowStoreChangelogs != nil {
		err := restoreChangelogBackedWindowStore(ctx, tm, t, args, offsetMap)
		if err != nil {
			return err
		}
	}
	return nil
}

func restoreMongoDBKVStore(
	ctx context.Context,
	tm *TransactionManager,
	t *StreamTask,
	kvchangelog *KVStoreChangelog,
) error {
	storeTranID, found, err := kvchangelog.KVStore.GetTransactionID(ctx, kvchangelog.TabTranRepr)
	if err != nil {
		return err
	}
	if !found || tm.GetTransactionID() == storeTranID {
		return nil
	}

	seqNum, err := tm.FindConsumedSeqNumMatchesTransactionID(ctx, kvchangelog.InputStream.TopicName(), kvchangelog.ParNum, storeTranID)
	if err != nil {
		return err
	}
	kvchangelog.InputStream.SetCursor(seqNum, kvchangelog.ParNum)
	if err = kvchangelog.KVStore.StartTransaction(ctx); err != nil {
		return err
	}
	if err = kvchangelog.RestoreFunc(ctx, kvchangelog.RestoreArg); err != nil {
		return err
	}
	return kvchangelog.KVStore.CommitTransaction(ctx, tm.TransactionalId, tm.GetTransactionID())
}

func restoreMongoDBWinStore(
	ctx context.Context,
	tm *TransactionManager,
	t *StreamTask,
	kvchangelog *WindowStoreChangelog,
) error {
	storeTranID, found, err := kvchangelog.WinStore.GetTransactionID(ctx, tm.TransactionalId)
	if err != nil {
		return err
	}
	if !found || tm.GetTransactionID() == storeTranID {
		return nil
	}

	seqNum, err := tm.FindConsumedSeqNumMatchesTransactionID(ctx, kvchangelog.InputStream.TopicName(), kvchangelog.ParNum, storeTranID)
	if err != nil {
		return err
	}
	kvchangelog.InputStream.SetCursor(seqNum, kvchangelog.ParNum)
	if err = kvchangelog.WinStore.StartTransaction(ctx); err != nil {
		return err
	}
	if err = kvchangelog.RestoreFunc(ctx, kvchangelog.RestoreArg); err != nil {
		return err
	}
	return kvchangelog.WinStore.CommitTransaction(ctx, tm.TransactionalId, tm.GetTransactionID())
}

func SetupTransactionManager(
	ctx context.Context,
	args *StreamTaskArgsTransaction,
) (*TransactionManager, error) {
	tm, err := NewTransactionManager(ctx, args.Env, args.TransactionalId,
		commtypes.SerdeFormat(args.SerdeFormat))
	if err != nil {
		return nil, fmt.Errorf("NewTransactionManager failed: %v", err)
	}
	err = tm.InitTransaction(ctx)
	if err != nil {
		return nil, fmt.Errorf("InitTransaction failed: %v", err)
	}

	return tm, nil
}

func UpdateInputStreamCursor(
	args *StreamTaskArgsTransaction,
	offsetMap map[string]uint64,
) {
	for _, src := range args.Srcs {
		offset := offsetMap[src.TopicName()]
		src.SetCursor(offset+1, args.InParNum)
	}
}

func TrackOffsetAndCommit(ctx context.Context,
	consumedSeqNumConfigs []ConsumedSeqNumConfig,
	tm *TransactionManager,
	kvchangelogs []*KVStoreChangelog, winchangelogs []*WindowStoreChangelog,
	hasLiveTransaction *bool,
	trackConsumePar *bool,
	retc chan *common.FnOutput,
) {
	err := tm.AppendConsumedSeqNum(ctx, consumedSeqNumConfigs)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] append offset failed: %v\n", err)
		retc <- &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("append offset failed: %v\n", err),
		}
		return
	}
	err = tm.CommitTransaction(ctx, kvchangelogs, winchangelogs)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] commit failed: %v\n", err)
		retc <- &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("commit failed: %v\n", err),
		}
		return
	}
	*hasLiveTransaction = false
	*trackConsumePar = false
}

type StreamTaskArgs struct {
	procArgs interface{}
	env      types.Environment

	srcs                  []source_sink.Source
	sinks                 []source_sink.Sink
	windowStoreChangelogs []*WindowStoreChangelog
	kvChangelogs          []*KVStoreChangelog

	Duration       time.Duration
	Warmup         time.Duration
	FlushEvery     time.Duration
	SerdeFormat    commtypes.SerdeFormat
	ParNum         uint8
	NumInPartition uint8
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

type StreamTaskArgsTransaction struct {
	ProcArgs        proc_interface.ProcArgs
	Env             types.Environment
	Srcs            []source_sink.Source
	Sinks           []source_sink.Sink
	AppId           string
	TransactionalId string

	WindowStoreChangelogs []*WindowStoreChangelog
	KVChangelogs          []*KVStoreChangelog

	Warmup           time.Duration
	ScaleEpoch       uint64
	CommitEveryMs    uint64
	CommitEveryNIter uint32
	ExitAfterNCommit uint32
	Duration         uint32
	SerdeFormat      commtypes.SerdeFormat
	InParNum         uint8

	FixedOutParNum uint8
}

func (t *StreamTask) Process(ctx context.Context, args *StreamTaskArgs) *common.FnOutput {
	debug.Assert(len(args.srcs) >= 1, "Srcs should be filled")
	debug.Assert(args.env != nil, "env should be filled")
	debug.Assert(args.procArgs != nil, "program args should be filled")
	debug.Assert(args.NumInPartition != 0, "number of input partition should not be zero")
	latencies := make([]int, 0, 128)
	cm, err := NewConsumeSeqManager(args.SerdeFormat)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	debug.Fprint(os.Stderr, "start restore\n")
	for _, srcStream := range args.srcs {
		inputTopicName := srcStream.TopicName()
		err = cm.CreateOffsetTopic(args.env, inputTopicName, args.NumInPartition, args.SerdeFormat)
		if err != nil {
			return &common.FnOutput{Success: false, Message: err.Error()}
		}
		offset, err := cm.FindLastConsumedSeqNum(ctx, inputTopicName, args.ParNum)
		if err != nil {
			if !errors.IsStreamEmptyError(err) {
				return &common.FnOutput{Success: false, Message: err.Error()}
			}
		}
		debug.Fprintf(os.Stderr, "offset restores to %x\n", offset)
		if offset != 0 {
			srcStream.SetCursor(offset+1, args.ParNum)
		}
		cm.AddTopicTrackConsumedSeqs(ctx, inputTopicName, []uint8{args.ParNum})
	}
	debug.Fprint(os.Stderr, "done restore\n")
	if t.InitFunc != nil {
		t.InitFunc(args.procArgs)
	}
	hasUncommitted := false

	var afterWarmupStart time.Time
	afterWarmup := false
	commitTimer := time.Now()
	flushTimer := time.Now()
	debug.Fprintf(os.Stderr, "warmup time: %v\n", args.Warmup)
	startTime := time.Now()
	for {
		timeSinceLastCommit := time.Since(commitTimer)
		if timeSinceLastCommit >= t.CommitEveryForAtLeastOnce && t.CurrentOffset != nil {
			if ret_err := t.pauseCommitResume(ctx, cm, args.ParNum, &hasUncommitted, &commitTimer); ret_err != nil {
				return ret_err
			}
		}
		timeSinceLastFlush := time.Since(flushTimer)
		if timeSinceLastFlush >= args.FlushEvery {
			if err := t.flushStreams(ctx, args); err != nil {
				return &common.FnOutput{Success: false, Message: err.Error()}
			}
		}
		if !afterWarmup && args.Warmup != 0 && time.Since(startTime) >= args.Warmup {
			afterWarmup = true
			afterWarmupStart = time.Now()
		}
		if args.Duration != 0 && time.Since(startTime) >= args.Duration {
			break
		}
		procStart := time.Now()
		ret := t.ProcessFunc(ctx, t, args.procArgs)
		if ret != nil {
			if ret.Success {
				// elapsed := time.Since(procStart)
				// latencies = append(latencies, int(elapsed.Microseconds()))
				debug.Fprintf(os.Stderr, "consume timeout\n")
				if ret_err := t.pauseAndCommit(ctx, cm, args.ParNum, hasUncommitted); ret_err != nil {
					return ret_err
				}
				updateReturnMetric(ret, startTime, afterWarmupStart, args.Warmup, afterWarmup, latencies)
			}
			return ret
		}
		if !hasUncommitted {
			hasUncommitted = true
		}
		if args.Warmup == 0 || afterWarmup {
			elapsed := time.Since(procStart)
			latencies = append(latencies, int(elapsed.Microseconds()))
		}
	}
	t.pauseAndCommit(ctx, cm, args.ParNum, hasUncommitted)
	ret := &common.FnOutput{Success: true}
	updateReturnMetric(ret, startTime, afterWarmupStart, args.Warmup, afterWarmup, latencies)
	return ret
}

func (t *StreamTask) flushStreams(ctx context.Context, args *StreamTaskArgs) error {
	for _, sink := range args.sinks {
		if err := sink.Flush(ctx); err != nil {
			return err
		}
	}
	for _, kvchangelog := range args.kvChangelogs {
		if kvchangelog.ChangelogManager != nil {
			if err := kvchangelog.ChangelogManager.Flush(ctx); err != nil {
				return err
			}
		}
	}
	for _, wschangelog := range args.windowStoreChangelogs {
		if wschangelog.ChangelogManager != nil {
			if err := wschangelog.ChangelogManager.Flush(ctx); err != nil {
				return err
			}
		}
	}
	return nil
}

func (t *StreamTask) pauseCommitResume(ctx context.Context, cm *ConsumeSeqManager,
	parNum uint8, hasUncommitted *bool, commitTimer *time.Time,
) *common.FnOutput {
	if t.PauseFunc != nil {
		if ret := t.PauseFunc(); ret != nil {
			return ret
		}
	}
	err := t.commitOffset(ctx, cm, parNum)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	if t.ResumeFunc != nil {
		t.ResumeFunc(t)
	}
	*hasUncommitted = false
	*commitTimer = time.Now()
	return nil
}

func (t *StreamTask) pauseAndCommit(ctx context.Context, cm *ConsumeSeqManager, parNum uint8, hasUncommitted bool) *common.FnOutput {
	alreadyPaused := false
	if hasUncommitted {
		if t.PauseFunc != nil {
			if ret := t.PauseFunc(); ret != nil {
				return ret
			}
		}
		err := t.commitOffset(ctx, cm, parNum)
		if err != nil {
			return &common.FnOutput{Success: false, Message: err.Error()}
		}
		alreadyPaused = true
	}
	if !alreadyPaused && t.PauseFunc != nil {
		if ret := t.PauseFunc(); ret != nil {
			return ret
		}
	}
	return nil
}

func (t *StreamTask) commitOffset(ctx context.Context, cm *ConsumeSeqManager, parNum uint8) error {
	consumedSeqNumConfigs := make([]ConsumedSeqNumConfig, 0)
	t.OffMu.Lock()
	for topic, offset := range t.CurrentOffset {
		consumedSeqNumConfigs = append(consumedSeqNumConfigs, ConsumedSeqNumConfig{
			TopicToTrack:   topic,
			Partition:      parNum,
			ConsumedSeqNum: uint64(offset),
		})
	}
	t.OffMu.Unlock()
	err := cm.AppendConsumedSeqNum(ctx, consumedSeqNumConfigs)
	if err != nil {
		return err
	}
	err = cm.Commit(ctx)
	return err
}

/*
func (t *StreamTask) ProcessWithTransaction(
	ctx context.Context,
	tm *TransactionManager,
	cmm *ControlChannelManager,
	args *StreamTaskArgsTransaction,
) *common.FnOutput {
	debug.Assert(len(args.OutputStreams) >= 1, "OutputStreams should be filled")
	for _, ostream := range args.OutputStreams {
		tm.RecordTopicStreams(ostream.TopicName(), ostream)
		cmm.TrackStream(ostream.TopicName(), ostream)
	}

	monitorQuit := make(chan struct{})
	monitorErrc := make(chan error)
	controlErrc := make(chan error)
	controlQuit := make(chan struct{})
	meta := make(chan txn_data.ControlMetadata)

	for topic, src := range args.Srcs {
		cmm.TrackStream(topic, src.Stream().(*sharedlog_stream.ShardedSharedLogStream))
	}

	dctx, dcancel := context.WithCancel(ctx)
	go tm.MonitorTransactionLog(ctx, monitorQuit, monitorErrc, dcancel)
	go cmm.MonitorControlChannel(ctx, controlQuit, controlErrc, meta)

	retc := make(chan *common.FnOutput)
	runChan := make(chan struct{})
	go t.processWithTranLoop(dctx, tm, args, retc, runChan)
	for {
		debug.Fprintf(os.Stderr, "begin of loop\n")
		select {
		case m := <-meta:
			debug.Fprintf(os.Stderr, "finished prev task %s, funcName %s, meta epoch %d, input epoch %d\n",
				m.FinishedPrevTask, args.ProcArgs.FuncName(), m.Epoch, args.QueryInput.ScaleEpoch)
			if m.FinishedPrevTask == args.ProcArgs.FuncName() && m.Epoch+1 == args.QueryInput.ScaleEpoch {
				runChan <- struct{}{}
			}
		case ret := <-retc:
			debug.Fprintf(os.Stderr, "got tran func reply\n")
			monitorQuit <- struct{}{}
			debug.Fprintf(os.Stderr, "appended to monitor quit\n")
			controlQuit <- struct{}{}
			debug.Fprintf(os.Stderr, "appended to control quit\n")
			return ret
		case merr := <-monitorErrc:
			debug.Fprintf(os.Stderr, "got monitor error chan\n")
			monitorQuit <- struct{}{}
			controlQuit <- struct{}{}
			if merr != nil {
				debug.Fprintf(os.Stderr, "[ERROR] control channel manager: %v", merr)
				dcancel()
				return &common.FnOutput{Success: false, Message: fmt.Sprintf("monitor failed: %v", merr)}
			}
		case cerr := <-controlErrc:
			debug.Fprintf(os.Stderr, "got control error chan\n")
			monitorQuit <- struct{}{}
			controlQuit <- struct{}{}
			if cerr != nil {
				debug.Fprintf(os.Stderr, "[ERROR] control channel manager: %v", cerr)
				dcancel()
				return &common.FnOutput{
					Success: false,
					Message: fmt.Sprintf("control channel manager failed: %v", cerr),
				}
			}
		case <-time.After(time.Duration(1) * time.Millisecond):
		}
		debug.Fprintf(os.Stderr, "end of loop\n")
	}
}
*/

func (t *StreamTask) ProcessWithTransaction(
	ctx context.Context,
	tm *TransactionManager,
	cmm *ControlChannelManager,
	args *StreamTaskArgsTransaction,
) *common.FnOutput {
	debug.Assert(len(args.Srcs) >= 1, "Srcs should be filled")
	debug.Assert(args.Env != nil, "env should be filled")
	debug.Assert(args.ProcArgs != nil, "program args should be filled")
	for _, src := range args.Srcs {
		if !src.IsInitialSource() {
			src.InTransaction(commtypes.SerdeFormat(args.SerdeFormat))
		}
		cmm.TrackStream(src.TopicName(), src.Stream().(*sharedlog_stream.ShardedSharedLogStream))
	}
	for _, sink := range args.Sinks {
		tm.RecordTopicStreams(sink.TopicName(), sink.Stream())
		cmm.TrackStream(sink.TopicName(), sink.Stream())
	}

	for _, kvchangelog := range args.KVChangelogs {
		if kvchangelog.ChangelogManager != nil {
			err := kvchangelog.ChangelogManager.InTransaction(tm)
			if err != nil {
				return &common.FnOutput{Success: false, Message: err.Error()}
			}
			tm.RecordTopicStreams(kvchangelog.ChangelogManager.TopicName(), kvchangelog.ChangelogManager.Stream())
			cmm.TrackStream(kvchangelog.ChangelogManager.TopicName(), kvchangelog.ChangelogManager.Stream())
		}
	}
	for _, winchangelog := range args.WindowStoreChangelogs {
		if winchangelog.ChangelogManager != nil {
			err := winchangelog.ChangelogManager.InTransaction(tm)
			if err != nil {
				return &common.FnOutput{Success: false, Message: err.Error()}
			}
			tm.RecordTopicStreams(winchangelog.ChangelogManager.TopicName(), winchangelog.ChangelogManager.Stream())
			cmm.TrackStream(winchangelog.ChangelogManager.TopicName(), winchangelog.ChangelogManager.Stream())
		}
	}

	monitorQuit := make(chan struct{})
	monitorErrc := make(chan error, 1)
	controlErrc := make(chan error, 1)
	controlQuit := make(chan struct{})
	meta := make(chan txn_data.ControlMetadata)

	dctx, dcancel := context.WithCancel(ctx)
	go tm.MonitorTransactionLog(dctx, monitorQuit, monitorErrc, dcancel)
	go cmm.MonitorControlChannel(dctx, controlQuit, controlErrc, meta)

	run := false
	// retc := make(chan *common.FnOutput)
	// runChan := make(chan struct{})
	// go t.processWithTranLoop(dctx, tm, args, retc, runChan)

	latencies := make([]int, 0, 128)
	hasLiveTransaction := false
	trackConsumePar := false
	commitTimer := time.Now()
	commitEvery := time.Duration(args.CommitEveryMs) * time.Millisecond
	duration := time.Duration(args.Duration) * time.Second

	idx := 0
	numCommit := 0
	debug.Fprintf(os.Stderr, "commit every(ms): %d, commit everyIter: %d, exitAfterNComm: %d\n",
		commitEvery, args.CommitEveryNIter, args.ExitAfterNCommit)
	init := false
	startTimeInit := false
	var afterWarmupStart time.Time
	afterWarmup := false
	warmupDuration := args.Warmup
	var startTime time.Time
	for {
		select {
		case <-dctx.Done():
			return &common.FnOutput{Success: true, Message: "exit due to ctx cancel"}
		case m := <-meta:
			debug.Fprintf(os.Stderr, "finished prev task %s, funcName %s, meta epoch %d, input epoch %d\n",
				m.FinishedPrevTask, args.ProcArgs.FuncName(), m.Epoch, args.ScaleEpoch)
			if m.FinishedPrevTask == args.ProcArgs.FuncName() && m.Epoch+1 == args.ScaleEpoch {
				run = true
			}
		case merr := <-monitorErrc:
			debug.Fprintf(os.Stderr, "got monitor error chan\n")
			monitorQuit <- struct{}{}
			controlQuit <- struct{}{}
			if merr != nil {
				debug.Fprintf(os.Stderr, "[ERROR] control channel manager: %v", merr)
				dcancel()
				return &common.FnOutput{Success: false, Message: fmt.Sprintf("monitor failed: %v", merr)}
			}
		case cerr := <-controlErrc:
			debug.Fprintf(os.Stderr, "got control error chan\n")
			monitorQuit <- struct{}{}
			controlQuit <- struct{}{}
			if cerr != nil {
				debug.Fprintf(os.Stderr, "[ERROR] control channel manager: %v", cerr)
				dcancel()
				return &common.FnOutput{
					Success: false,
					Message: fmt.Sprintf("control channel manager failed: %v", cerr),
				}
			}
		default:
		}
		if run {
			procStart := time.Now()
			if !startTimeInit {
				startTime = time.Now()
				startTimeInit = true
			}
			if !afterWarmup && warmupDuration != 0 && time.Since(startTime) >= warmupDuration {
				debug.Fprintf(os.Stderr, "after warmup\n")
				afterWarmup = true
				afterWarmupStart = time.Now()
			}
			timeSinceTranStart := time.Since(commitTimer)
			cur_elapsed := time.Since(startTime)
			timeout := duration != 0 && cur_elapsed >= duration
			shouldCommitByIter := args.CommitEveryNIter != 0 &&
				uint32(idx)%args.CommitEveryNIter == 0 && idx != 0
			// debug.Fprintf(os.Stderr, "iter: %d, shouldCommitByIter: %v, timeSinceTranStart: %v, cur_elapsed: %v, duration: %v\n",
			// 	idx, shouldCommitByIter, timeSinceTranStart, cur_elapsed, duration)

			// should commit
			if ((commitEvery != 0 && timeSinceTranStart > commitEvery) || timeout || shouldCommitByIter) && hasLiveTransaction {
				err_out := t.commitTransaction(ctx, tm, args, &hasLiveTransaction, &trackConsumePar)
				if err_out != nil {
					return err_out
				}
				debug.Assert(!hasLiveTransaction, "after commit. there should be no live transaction\n")
				numCommit += 1
				debug.Fprintf(os.Stderr, "transaction committed\n")
			}

			// Exit routine
			cur_elapsed = time.Since(startTime)
			timeout = duration != 0 && cur_elapsed >= duration
			if timeout || (args.ExitAfterNCommit != 0 && numCommit == int(args.ExitAfterNCommit)) {
				if err := tm.Close(); err != nil {
					debug.Fprintf(os.Stderr, "[ERROR] close transaction manager: %v\n", err)
					return &common.FnOutput{Success: false, Message: fmt.Sprintf("close transaction manager: %v\n", err)}
				}
				if hasLiveTransaction {
					err_out := t.commitTransaction(ctx, tm, args, &hasLiveTransaction, &trackConsumePar)
					if err_out != nil {
						return err_out
					}
				}
				elapsed := time.Since(procStart)
				latencies = append(latencies, int(elapsed.Microseconds()))
				ret := &common.FnOutput{
					Success: true,
				}
				updateReturnMetric(ret, startTime, afterWarmupStart, warmupDuration, afterWarmup, latencies)
				return ret
			}

			// begin new transaction
			err := t.startNewTransaction(ctx, args, tm, &hasLiveTransaction, &trackConsumePar, &init, &commitTimer)
			if err != nil {
				return &common.FnOutput{Success: false, Message: err.Error()}
			}

			ret := t.ProcessFunc(ctx, t, args.ProcArgs)
			if ret != nil {
				if ret.Success {
					if hasLiveTransaction {
						err_out := t.commitTransaction(ctx, tm, args, &hasLiveTransaction, &trackConsumePar)
						if err_out != nil {
							return err_out
						}
					}
					// elapsed := time.Since(procStart)
					// latencies = append(latencies, int(elapsed.Microseconds()))
					updateReturnMetric(ret, startTime, afterWarmupStart, warmupDuration, afterWarmup, latencies)
				} else {
					if hasLiveTransaction {
						if err := tm.AbortTransaction(ctx, false, args.KVChangelogs, args.WindowStoreChangelogs); err != nil {
							return &common.FnOutput{Success: false, Message: fmt.Sprintf("abort failed: %v\n", err)}
						}
					}
				}
				return ret
			}
			if warmupDuration == 0 || afterWarmup {
				elapsed := time.Since(procStart)
				latencies = append(latencies, int(elapsed.Microseconds()))
			}
			idx += 1
		}
	}
}

func (t *StreamTask) startNewTransaction(ctx context.Context, args *StreamTaskArgsTransaction, tm *TransactionManager,
	hasLiveTransaction *bool, trackConsumePar *bool, init *bool, commitTimer *time.Time,
) error {
	if !*hasLiveTransaction {
		if err := tm.BeginTransaction(ctx, args.KVChangelogs, args.WindowStoreChangelogs); err != nil {
			debug.Fprintf(os.Stderr, "[ERROR] transaction begin failed: %v", err)
			return fmt.Errorf("transaction begin failed: %v\n", err)
		}
		*hasLiveTransaction = true
		*commitTimer = time.Now()
		if args.FixedOutParNum != 0 {
			debug.Assert(len(args.Sinks) == 1, "fixed out param is only usable when there's only one output stream")
			if err := tm.AddTopicPartition(ctx, args.Sinks[0].TopicName(), []uint8{args.FixedOutParNum}); err != nil {
				debug.Fprintf(os.Stderr, "[ERROR] track topic partition failed: %v\n", err)
				return fmt.Errorf("track topic partition failed: %v\n", err)
			}
		}
		if *init && t.ResumeFunc != nil {
			t.ResumeFunc(t)
		}
		if !*init && t.InitFunc != nil {
			t.InitFunc(args.ProcArgs)
			*init = true
		}
	}
	if !*trackConsumePar {
		for _, src := range args.Srcs {
			if err := tm.AddTopicTrackConsumedSeqs(ctx, src.TopicName(), []uint8{args.InParNum}); err != nil {
				return fmt.Errorf("add offsets failed: %v\n", err)
			}
		}
		*trackConsumePar = true
	}
	return nil
}

func updateReturnMetric(ret *common.FnOutput,
	startTime time.Time, afterWarmupStart time.Time, warmupDuration time.Duration, afterWarmup bool,
	latencies []int,
) {
	ret.Latencies = map[string][]int{"e2e": latencies}
	ret.Consumed = make(map[string]uint64)
	e2eTime := time.Since(startTime).Seconds()
	if warmupDuration != 0 && afterWarmup {
		e2eTime = time.Since(afterWarmupStart).Seconds()
	}
	ret.Duration = e2eTime
}

/*
func (t *StreamTask) processWithTranLoop(ctx context.Context,
	tm *TransactionManager, args *StreamTaskArgsTransaction,
	retc chan *common.FnOutput, runChan chan struct{},
) {
	latencies := make([]int, 0, 128)
	hasLiveTransaction := false
	trackConsumePar := false
	var currentOffset map[string]uint64
	commitTimer := time.Now()
	commitEvery := time.Duration(args.QueryInput.CommitEveryMs) * time.Millisecond
	duration := time.Duration(args.QueryInput.Duration) * time.Second

	idx := 0
	numCommit := 0
	debug.Fprintf(os.Stderr, "commit every(ms): %d, commit everyIter: %d, exitAfterNComm: %d\n",
		commitEvery, args.QueryInput.CommitEveryNIter, args.QueryInput.ExitAfterNCommit)

	<-runChan

	init := false
	var afterWarmupStart time.Time
	afterWarmup := false
	warmupDuration := time.Duration(args.QueryInput.WarmupS) * time.Second
	debug.Fprintf(os.Stderr, "warmup time: %v\n", warmupDuration)
	startTime := time.Now()
L:
	for {
		select {
		case <-ctx.Done():
			debug.Fprintf(os.Stderr, "out of loop because of ctx")
			break L

		default:
		}
		if !afterWarmup && warmupDuration != 0 && time.Since(startTime) >= warmupDuration {
			debug.Fprintf(os.Stderr, "after warmup\n")
			afterWarmup = true
			afterWarmupStart = time.Now()
		}
		procStart := time.Now()
		timeSinceTranStart := time.Since(commitTimer)
		cur_elapsed := time.Since(startTime)
		timeout := duration != 0 && cur_elapsed >= duration
		shouldCommitByIter := args.QueryInput.CommitEveryNIter != 0 &&
			uint32(idx)%args.QueryInput.CommitEveryNIter == 0 && idx != 0
		// debug.Fprintf(os.Stderr, "iter: %d, shouldCommitByIter: %v, timeSinceTranStart: %v, cur_elapsed: %v, duration: %v\n",
		// 	idx, shouldCommitByIter, timeSinceTranStart, cur_elapsed, duration)
		if ((commitEvery != 0 && timeSinceTranStart > commitEvery) || timeout || shouldCommitByIter) && hasLiveTransaction {
			t.commitTransaction(ctx, currentOffset, tm, args, retc, &hasLiveTransaction, &trackConsumePar)

			debug.Assert(!hasLiveTransaction, "after commit. there should be no live transaction\n")
			numCommit += 1
			debug.Fprintf(os.Stderr, "transaction committed\n")
		}
		cur_elapsed = time.Since(startTime)
		timeout = duration != 0 && cur_elapsed >= duration
		if timeout || (args.QueryInput.ExitAfterNCommit != 0 && numCommit == int(args.QueryInput.ExitAfterNCommit)) {
			if err := tm.Close(); err != nil {
				debug.Fprintf(os.Stderr, "[ERROR] close transaction manager: %v\n", err)
				retc <- &common.FnOutput{Success: false, Message: fmt.Sprintf("close transaction manager: %v\n", err)}
				return
			}
			break L
		}
		if !hasLiveTransaction {
			if err := tm.BeginTransaction(ctx, args.KVChangelogs, args.WindowStoreChangelogs); err != nil {
				debug.Fprintf(os.Stderr, "[ERROR] transaction begin failed: %v", err)
				retc <- &common.FnOutput{Success: false, Message: fmt.Sprintf("transaction begin failed: %v\n", err)}
				return
			}
			hasLiveTransaction = true
			commitTimer = time.Now()
			if args.FixedOutParNum != 0 {
				debug.Assert(len(args.QueryInput.OutputTopicNames) == 1, "fixed out param is only usable when there's only one output stream")
				if err := tm.AddTopicPartition(ctx, args.QueryInput.OutputTopicNames[0], []uint8{args.FixedOutParNum}); err != nil {
					debug.Fprintf(os.Stderr, "[ERROR] track topic partition failed: %v\n", err)
					retc <- &common.FnOutput{Success: false, Message: fmt.Sprintf("track topic partition failed: %v\n", err)}
					return
				}
			}
			if init && t.ResumeFunc != nil {
				t.ResumeFunc()
			}
			if !init && t.InitFunc != nil {
				t.InitFunc(args.ProcArgs)
				init = true
			}
		}
		if !trackConsumePar {
			for _, inputTopicName := range args.QueryInput.InputTopicNames {
				if err := tm.AddTopicTrackConsumedSeqs(ctx, inputTopicName, []uint8{args.QueryInput.ParNum}); err != nil {
					retc <- &common.FnOutput{Success: false, Message: fmt.Sprintf("add offsets failed: %v\n", err)}
					return
				}
			}
			trackConsumePar = true
		}

		off, ret := t.ProcessFunc(ctx, t, args.ProcArgs)
		if ret != nil {
			if ret.Success {
				if hasLiveTransaction {
					t.commitTransaction(ctx, currentOffset, tm, args, retc, &hasLiveTransaction, &trackConsumePar)
				}
				if t.CloseFunc != nil {
					debug.Fprintf(os.Stderr, "waiting for goroutines to close 1\n")
					t.CloseFunc()
				}
				// elapsed := time.Since(procStart)
				// latencies = append(latencies, int(elapsed.Microseconds()))
				ret.Latencies = map[string][]int{"e2e": latencies}
				ret.Consumed = make(map[string]uint64)
				e2eTime := time.Since(startTime).Seconds()
				if warmupDuration != 0 && afterWarmup {
					e2eTime = time.Since(afterWarmupStart).Seconds()
				}
				ret.Duration = e2eTime
			} else {
				if hasLiveTransaction {
					if err := tm.AbortTransaction(ctx, false, args.KVChangelogs, args.WindowStoreChangelogs); err != nil {
						retc <- &common.FnOutput{Success: false, Message: fmt.Sprintf("abort failed: %v\n", err)}
						return
					}
				}
			}
			debug.Fprintf(os.Stderr, "return final out to chan 1\n")
			retc <- ret
			debug.Fprintf(os.Stderr, "return done\n")
			return
		}
		currentOffset = off
		if warmupDuration == 0 || afterWarmup {
			elapsed := time.Since(procStart)
			latencies = append(latencies, int(elapsed.Microseconds()))
		}
		idx += 1
	}
	if hasLiveTransaction {
		t.commitTransaction(ctx, currentOffset, tm, args, retc, &hasLiveTransaction, &trackConsumePar)
	}
	if t.CloseFunc != nil {
		debug.Fprintf(os.Stderr, "waiting for goroutines to close 2\n")
		t.CloseFunc()
	}
	e2eTime := time.Since(startTime).Seconds()
	if warmupDuration != 0 && afterWarmup {
		e2eTime = time.Since(afterWarmupStart).Seconds()
	}
	debug.Fprintf(os.Stderr, "return final out to chan 2\n")
	retc <- &common.FnOutput{
		Success:   true,
		Duration:  e2eTime,
		Latencies: map[string][]int{"e2e": latencies},
		Consumed:  make(map[string]uint64),
	}
	debug.Fprintf(os.Stderr, "return done\n")
}
*/

/*
func (t *StreamTask) commitTransaction(ctx context.Context,
	currentOffset map[string]uint64,
	tm *TransactionManager,
	args *StreamTaskArgsTransaction,
	retc chan *common.FnOutput,
	hasLiveTransaction *bool,
	trackConsumePar *bool,
) {
	debug.Fprintf(os.Stderr, "about to flush\n")
	if t.PauseFunc != nil {
		t.PauseFunc()
	}
	debug.Fprintf(os.Stderr, "after flush\n")
	consumedSeqNumConfigs := make([]ConsumedSeqNumConfig, 0)
	for topic, offset := range currentOffset {
		consumedSeqNumConfigs = append(consumedSeqNumConfigs, ConsumedSeqNumConfig{
			TopicToTrack:   topic,
			TaskId:         tm.CurrentTaskId,
			TaskEpoch:      tm.CurrentEpoch,
			Partition:      args.QueryInput.ParNum,
			ConsumedSeqNum: uint64(offset),
		})
	}
	debug.Fprintf(os.Stderr, "about to commit transaction\n")
	err := tm.AppendConsumedSeqNum(ctx, consumedSeqNumConfigs)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] append offset failed: %v\n", err)
		retc <- &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("append offset failed: %v\n", err),
		}
		return
	}
	err = tm.CommitTransaction(ctx, args.KVChangelogs, args.WindowStoreChangelogs)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] commit failed: %v\n", err)
		retc <- &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("commit failed: %v\n", err),
		}
		return
	}
	*hasLiveTransaction = false
	*trackConsumePar = false
}
*/

func (t *StreamTask) commitTransaction(ctx context.Context,
	tm *TransactionManager,
	args *StreamTaskArgsTransaction,
	hasLiveTransaction *bool,
	trackConsumePar *bool,
) *common.FnOutput {
	// debug.Fprintf(os.Stderr, "about to pause\n")
	if t.PauseFunc != nil {
		if ret := t.PauseFunc(); ret != nil {
			return ret
		}
	}
	// debug.Fprintf(os.Stderr, "after pause\n")
	consumedSeqNumConfigs := make([]ConsumedSeqNumConfig, 0)
	t.OffMu.Lock()
	for topic, offset := range t.CurrentOffset {
		consumedSeqNumConfigs = append(consumedSeqNumConfigs, ConsumedSeqNumConfig{
			TopicToTrack:   topic,
			TaskId:         tm.GetCurrentTaskId(),
			TaskEpoch:      tm.GetCurrentEpoch(),
			Partition:      args.InParNum,
			ConsumedSeqNum: uint64(offset),
		})
	}
	t.OffMu.Unlock()
	// debug.Fprintf(os.Stderr, "about to commit transaction\n")
	err := tm.AppendConsumedSeqNum(ctx, consumedSeqNumConfigs)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] append offset failed: %v\n", err)
		return &common.FnOutput{Success: false, Message: fmt.Sprintf("append offset failed: %v\n", err)}
	}
	err = tm.CommitTransaction(ctx, args.KVChangelogs, args.WindowStoreChangelogs)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] commit failed: %v\n", err)
		return &common.FnOutput{Success: false, Message: fmt.Sprintf("commit failed: %v\n", err)}
	}
	*hasLiveTransaction = false
	*trackConsumePar = false
	return nil
}

func CommonProcess(ctx context.Context, t *StreamTask, args proc_interface.ProcArgsWithSrcSink,
	proc func(t *StreamTask, msg commtypes.MsgAndSeq) error,
) *common.FnOutput {
	select {
	case err := <-args.ErrChan():
		return &common.FnOutput{Success: false, Message: err.Error()}
	default:
	}
	gotMsgs, err := args.Source().Consume(ctx, args.ParNum())
	if err != nil {
		if xerrors.Is(err, errors.ErrStreamSourceTimeout) {
			return &common.FnOutput{Success: true, Message: err.Error()}
		}
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	for _, msg := range gotMsgs.Msgs {
		if msg.MsgArr == nil && msg.Msg.Value == nil {
			continue
		}
		if msg.IsControl {
			v := msg.Msg.Value.(source_sink.ScaleEpochAndBytes)
			err := args.PushToAllSinks(ctx, commtypes.Message{Key: commtypes.SCALE_FENCE_KEY,
				Value: v.Payload}, args.ParNum(), true)
			if err != nil {
				return &common.FnOutput{Success: false, Message: err.Error()}
			}
			if args.CurEpoch() < v.ScaleEpoch {
				err = args.RecordFinishFunc()(ctx, args.FuncName(), args.ParNum())
				if err != nil {
					return &common.FnOutput{Success: false, Message: err.Error()}
				}
				return &common.FnOutput{
					Success: true,
					Message: fmt.Sprintf("%s-%d epoch %d exit", args.FuncName(), args.ParNum(), args.CurEpoch()),
					Err:     errors.ErrShouldExitForScale,
				}
			}
			continue
		}
		err = proc(t, msg)
		if err != nil {
			return &common.FnOutput{Success: false, Message: err.Error()}
		}
	}
	return nil
}
