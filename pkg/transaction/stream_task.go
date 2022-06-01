package transaction

import (
	"context"
	"fmt"
	"os"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/pkg/control_channel"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/errors"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/store"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/txn_data"
	"sync"
	"time"

	"cs.utexas.edu/zjia/faas/types"
)

type StreamTask struct {
	ProcessFunc               func(ctx context.Context, task *StreamTask, args interface{}) *common.FnOutput
	OffMu                     sync.Mutex
	CurrentOffset             map[string]uint64
	PauseFunc                 func() *common.FnOutput
	ResumeFunc                func(task *StreamTask)
	InitFunc                  func(progArgs interface{})
	HandleErrFunc             func() error
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

func SetupManagersAndProcessTransactional(ctx context.Context,
	env types.Environment,
	streamTaskArgs *StreamTaskArgsTransaction,
	task *StreamTask,
) *common.FnOutput {
	tm, cmm, err := SetupManagers(ctx, env, streamTaskArgs, task)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	debug.Fprint(os.Stderr, "begin transaction processing\n")
	ret := task.ProcessWithTransaction(ctx, tm, cmm, streamTaskArgs)
	return ret
}

func SetupManagers(ctx context.Context, env types.Environment,
	streamTaskArgs *StreamTaskArgsTransaction,
	task *StreamTask,
) (*TransactionManager, *control_channel.ControlChannelManager, error) {
	debug.Fprint(os.Stderr, "setup transaction and control manager\n")
	tm, err := SetupTransactionManager(ctx, streamTaskArgs)
	if err != nil {
		return nil, nil, err
	}
	cmm, err := control_channel.NewControlChannelManager(env, streamTaskArgs.appId,
		commtypes.SerdeFormat(streamTaskArgs.serdeFormat), streamTaskArgs.scaleEpoch)
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
		return cmm.RecordPrevInstanceFinish(ctx, funcName, instanceID, cmm.CurrentEpoch())
	}
	streamTaskArgs.procArgs.SetTrackParFunc(trackParFunc)
	streamTaskArgs.procArgs.SetRecordFinishFunc(recordFinish)
	for _, kvchangelog := range streamTaskArgs.kvChangelogs {
		kvchangelog.kvStore.SetTrackParFunc(trackParFunc)
	}
	for _, wschangelog := range streamTaskArgs.windowStoreChangelogs {
		wschangelog.winStore.SetTrackParFunc(trackParFunc)
	}
	return tm, cmm, nil
}

func getOffsetMap(ctx context.Context, tm *TransactionManager, args *StreamTaskArgsTransaction) (map[string]uint64, error) {
	offsetMap := make(map[string]uint64)
	for _, src := range args.srcs {
		inputTopicName := src.TopicName()
		offset, err := createOffsetTopicAndGetOffset(ctx, tm, inputTopicName,
			uint8(src.Stream().NumPartition()), args.inParNum)
		if err != nil {
			return nil, fmt.Errorf("createOffsetTopicAndGetOffset failed: %v", err)
		}
		offsetMap[inputTopicName] = offset
		debug.Fprintf(os.Stderr, "tp %s offset %d\n", inputTopicName, offset)
	}
	return offsetMap, nil
}

func setOffsetOnStream(offsetMap map[string]uint64, args *StreamTaskArgsTransaction) {
	for _, src := range args.srcs {
		inputTopicName := src.TopicName()
		offset := offsetMap[inputTopicName]
		src.SetCursor(offset+1, args.inParNum)
	}
}

func restoreKVStore(ctx context.Context, tm *TransactionManager,
	t *StreamTask,
	args *StreamTaskArgsTransaction, offsetMap map[string]uint64,
) error {
	for _, kvchangelog := range args.kvChangelogs {
		if kvchangelog.kvStore.TableType() == store.IN_MEM {
			topic := kvchangelog.changelogManager.TopicName()
			// offset stream is input stream
			if offset, ok := offsetMap[topic]; ok && offset != 0 {
				err := RestoreChangelogKVStateStore(ctx,
					kvchangelog, offset)
				if err != nil {
					return fmt.Errorf("RestoreKVStateStore failed: %v", err)
				}
			} else {
				offset, err := createOffsetTopicAndGetOffset(ctx, tm, topic,
					kvchangelog.changelogManager.NumPartition(), kvchangelog.parNum)
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
		} else if kvchangelog.kvStore.TableType() == store.MONGODB {
			if err := restoreMongoDBKVStore(ctx, tm, t, kvchangelog); err != nil {
				return err
			}
		}
	}
	return nil
}

func restoreChangelogBackedWindowStore(ctx context.Context, tm *TransactionManager, t *StreamTask, args *StreamTaskArgsTransaction, offsetMap map[string]uint64) error {
	for _, wschangelog := range args.windowStoreChangelogs {
		if wschangelog.winStore.TableType() == store.IN_MEM {
			topic := wschangelog.changelogManager.TopicName()
			// offset stream is input stream
			if offset, ok := offsetMap[topic]; ok && offset != 0 {
				err := RestoreChangelogWindowStateStore(ctx, wschangelog, offset)
				if err != nil {
					return fmt.Errorf("RestoreWindowStateStore failed: %v", err)
				}
			} else {
				offset, err := createOffsetTopicAndGetOffset(ctx, tm, topic,
					wschangelog.changelogManager.NumPartition(), wschangelog.parNum)
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
		} else if wschangelog.winStore.TableType() == store.MONGODB {
			if err := restoreMongoDBWinStore(ctx, tm, t, wschangelog); err != nil {
				return err
			}
		}
	}
	return nil
}

func restoreStateStore(ctx context.Context, tm *TransactionManager, t *StreamTask, args *StreamTaskArgsTransaction, offsetMap map[string]uint64) error {
	if args.kvChangelogs != nil {
		err := restoreKVStore(ctx, tm, t, args, offsetMap)
		if err != nil {
			return err
		}
	}
	if args.windowStoreChangelogs != nil {
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
	storeTranID, found, err := kvchangelog.kvStore.GetTransactionID(ctx, kvchangelog.tabTranRepr)
	if err != nil {
		return err
	}
	if !found || tm.GetTransactionID() == storeTranID {
		return nil
	}

	seqNum, err := tm.FindConsumedSeqNumMatchesTransactionID(ctx, kvchangelog.inputStream.TopicName(), kvchangelog.parNum, storeTranID)
	if err != nil {
		return err
	}
	kvchangelog.inputStream.SetCursor(seqNum, kvchangelog.parNum)
	if err = kvchangelog.kvStore.StartTransaction(ctx); err != nil {
		return err
	}
	if err = kvchangelog.restoreFunc(ctx, kvchangelog.restoreArg); err != nil {
		return err
	}
	return kvchangelog.kvStore.CommitTransaction(ctx, tm.TransactionalId, tm.GetTransactionID())
}

func restoreMongoDBWinStore(
	ctx context.Context,
	tm *TransactionManager,
	t *StreamTask,
	kvchangelog *WindowStoreChangelog,
) error {
	storeTranID, found, err := kvchangelog.winStore.GetTransactionID(ctx, tm.TransactionalId)
	if err != nil {
		return err
	}
	if !found || tm.GetTransactionID() == storeTranID {
		return nil
	}

	seqNum, err := tm.FindConsumedSeqNumMatchesTransactionID(ctx, kvchangelog.inputStream.TopicName(), kvchangelog.parNum, storeTranID)
	if err != nil {
		return err
	}
	kvchangelog.inputStream.SetCursor(seqNum, kvchangelog.parNum)
	if err = kvchangelog.winStore.StartTransaction(ctx); err != nil {
		return err
	}
	if err = kvchangelog.restoreFunc(ctx, kvchangelog.restoreArg); err != nil {
		return err
	}
	return kvchangelog.winStore.CommitTransaction(ctx, tm.TransactionalId, tm.GetTransactionID())
}

func SetupTransactionManager(
	ctx context.Context,
	args *StreamTaskArgsTransaction,
) (*TransactionManager, error) {
	tm, err := NewTransactionManager(ctx, args.env, args.transactionalId,
		commtypes.SerdeFormat(args.serdeFormat))
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
	for _, src := range args.srcs {
		offset := offsetMap[src.TopicName()]
		src.SetCursor(offset+1, args.inParNum)
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

func (t *StreamTask) Process(ctx context.Context, args *StreamTaskArgs) *common.FnOutput {
	debug.Assert(len(args.srcs) >= 1, "Srcs should be filled")
	debug.Assert(args.env != nil, "env should be filled")
	debug.Assert(args.procArgs != nil, "program args should be filled")
	debug.Assert(args.numInPartition != 0, "number of input partition should not be zero")
	latencies := make([]int, 0, 128)
	cm, err := NewConsumeSeqManager(args.serdeFormat)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	debug.Fprint(os.Stderr, "start restore\n")
	for _, srcStream := range args.srcs {
		inputTopicName := srcStream.TopicName()
		err = cm.CreateOffsetTopic(args.env, inputTopicName, args.numInPartition, args.serdeFormat)
		if err != nil {
			return &common.FnOutput{Success: false, Message: err.Error()}
		}
		offset, err := cm.FindLastConsumedSeqNum(ctx, inputTopicName, args.parNum)
		if err != nil {
			if !errors.IsStreamEmptyError(err) {
				return &common.FnOutput{Success: false, Message: err.Error()}
			}
		}
		debug.Fprintf(os.Stderr, "offset restores to %x\n", offset)
		if offset != 0 {
			srcStream.SetCursor(offset+1, args.parNum)
		}
		cm.AddTopicTrackConsumedSeqs(ctx, inputTopicName, []uint8{args.parNum})
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
	debug.Fprintf(os.Stderr, "warmup time: %v\n", args.warmup)
	startTime := time.Now()
	for {
		timeSinceLastCommit := time.Since(commitTimer)
		if timeSinceLastCommit >= t.CommitEveryForAtLeastOnce && t.CurrentOffset != nil {
			if ret_err := t.pauseCommitFlushResume(ctx, args, cm, &hasUncommitted, &commitTimer, &flushTimer); ret_err != nil {
				return ret_err
			}
		}
		timeSinceLastFlush := time.Since(flushTimer)
		if timeSinceLastFlush >= args.flushEvery {
			if ret_err := t.pauseFlushResume(ctx, args, &flushTimer); err != nil {
				return ret_err
			}
		}
		if !afterWarmup && args.warmup != 0 && time.Since(startTime) >= args.warmup {
			afterWarmup = true
			afterWarmupStart = time.Now()
		}
		if args.duration != 0 && time.Since(startTime) >= args.duration {
			break
		}
		procStart := time.Now()
		ret := t.ProcessFunc(ctx, t, args.procArgs)
		if ret != nil {
			if ret.Success {
				// elapsed := time.Since(procStart)
				// latencies = append(latencies, int(elapsed.Microseconds()))
				debug.Fprintf(os.Stderr, "consume timeout\n")
				if ret_err := t.pauseCommitFlush(ctx, args, cm, hasUncommitted); ret_err != nil {
					return ret_err
				}
				updateReturnMetric(ret, startTime, afterWarmupStart, args.warmup, afterWarmup, latencies)
			}
			return ret
		}
		if !hasUncommitted {
			hasUncommitted = true
		}
		if args.warmup == 0 || afterWarmup {
			elapsed := time.Since(procStart)
			latencies = append(latencies, int(elapsed.Microseconds()))
		}
	}
	t.pauseCommitFlush(ctx, args, cm, hasUncommitted)
	ret := &common.FnOutput{Success: true}
	updateReturnMetric(ret, startTime, afterWarmupStart, args.warmup, afterWarmup, latencies)
	return ret
}

func (t *StreamTask) flushStreams(ctx context.Context, args *StreamTaskArgs) error {
	for _, sink := range args.sinks {
		if err := sink.Flush(ctx); err != nil {
			return err
		}
	}
	for _, kvchangelog := range args.kvChangelogs {
		if kvchangelog.changelogManager != nil {
			if err := kvchangelog.changelogManager.Flush(ctx); err != nil {
				return err
			}
		}
	}
	for _, wschangelog := range args.windowStoreChangelogs {
		if wschangelog.changelogManager != nil {
			if err := wschangelog.changelogManager.Flush(ctx); err != nil {
				return err
			}
		}
	}
	return nil
}

func (t *StreamTask) pauseFlushResume(ctx context.Context, args *StreamTaskArgs, flushTimer *time.Time) *common.FnOutput {
	if t.PauseFunc != nil {
		if ret := t.PauseFunc(); ret != nil {
			return ret
		}
	}
	err := t.flushStreams(ctx, args)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	if t.ResumeFunc != nil {
		t.ResumeFunc(t)
	}
	*flushTimer = time.Now()
	return nil
}

func (t *StreamTask) pauseCommitFlushResume(ctx context.Context, args *StreamTaskArgs, cm *ConsumeSeqManager,
	hasUncommitted *bool, commitTimer *time.Time, flushTimer *time.Time,
) *common.FnOutput {
	if t.PauseFunc != nil {
		if ret := t.PauseFunc(); ret != nil {
			return ret
		}
	}
	err := t.commitOffset(ctx, cm, args.parNum)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	err = t.flushStreams(ctx, args)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	if t.ResumeFunc != nil {
		t.ResumeFunc(t)
	}
	*hasUncommitted = false
	*commitTimer = time.Now()
	*flushTimer = time.Now()
	return nil
}

func (t *StreamTask) pauseCommitFlush(ctx context.Context, args *StreamTaskArgs, cm *ConsumeSeqManager, hasUncommitted bool) *common.FnOutput {
	alreadyPaused := false
	if hasUncommitted {
		if t.PauseFunc != nil {
			if ret := t.PauseFunc(); ret != nil {
				return ret
			}
		}
		err := t.commitOffset(ctx, cm, args.parNum)
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
	err := t.flushStreams(ctx, args)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
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

func (t *StreamTask) ProcessWithTransaction(
	ctx context.Context,
	tm *TransactionManager,
	cmm *control_channel.ControlChannelManager,
	args *StreamTaskArgsTransaction,
) *common.FnOutput {
	debug.Assert(len(args.srcs) >= 1, "Srcs should be filled")
	debug.Assert(args.env != nil, "env should be filled")
	debug.Assert(args.procArgs != nil, "program args should be filled")
	for _, src := range args.srcs {
		if !src.IsInitialSource() {
			src.InTransaction(commtypes.SerdeFormat(args.serdeFormat))
		}
		cmm.TrackStream(src.TopicName(), src.Stream().(*sharedlog_stream.ShardedSharedLogStream))
	}
	for _, sink := range args.sinks {
		sink.InTransaction(tm)
		tm.RecordTopicStreams(sink.TopicName(), sink.Stream())
		cmm.TrackStream(sink.TopicName(), sink.Stream())
	}

	for _, kvchangelog := range args.kvChangelogs {
		if kvchangelog.changelogManager != nil {
			err := kvchangelog.changelogManager.InTransaction(tm)
			if err != nil {
				return &common.FnOutput{Success: false, Message: err.Error()}
			}
			tm.RecordTopicStreams(kvchangelog.changelogManager.TopicName(), kvchangelog.changelogManager.Stream())
			cmm.TrackStream(kvchangelog.changelogManager.TopicName(), kvchangelog.changelogManager.Stream())
		}
	}
	for _, winchangelog := range args.windowStoreChangelogs {
		if winchangelog.changelogManager != nil {
			err := winchangelog.changelogManager.InTransaction(tm)
			if err != nil {
				return &common.FnOutput{Success: false, Message: err.Error()}
			}
			tm.RecordTopicStreams(winchangelog.changelogManager.TopicName(), winchangelog.changelogManager.Stream())
			cmm.TrackStream(winchangelog.changelogManager.TopicName(), winchangelog.changelogManager.Stream())
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

	latencies := make([]int, 0, 128)
	hasLiveTransaction := false
	trackConsumePar := false
	commitTimer := time.Now()

	idx := 0
	numCommit := 0
	debug.Fprintf(os.Stderr, "commit every(ms): %d, commit everyIter: %d, exitAfterNComm: %d\n",
		args.commitEvery, args.commitEveryNIter, args.exitAfterNCommit)
	init := false
	startTimeInit := false
	var afterWarmupStart time.Time
	afterWarmup := false
	warmupDuration := args.warmup
	var startTime time.Time
	for {
		select {
		case <-dctx.Done():
			return &common.FnOutput{Success: true, Message: "exit due to ctx cancel"}
		case m := <-meta:
			debug.Fprintf(os.Stderr, "finished prev task %s, funcName %s, meta epoch %d, input epoch %d\n",
				m.FinishedPrevTask, args.procArgs.FuncName(), m.Epoch, args.scaleEpoch)
			if m.FinishedPrevTask == args.procArgs.FuncName() && m.Epoch+1 == args.scaleEpoch {
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
			timeout := args.duration != 0 && cur_elapsed >= args.duration
			shouldCommitByIter := args.commitEveryNIter != 0 &&
				uint32(idx)%args.commitEveryNIter == 0 && idx != 0
			// debug.Fprintf(os.Stderr, "iter: %d, shouldCommitByIter: %v, timeSinceTranStart: %v, cur_elapsed: %v, duration: %v\n",
			// 	idx, shouldCommitByIter, timeSinceTranStart, cur_elapsed, duration)

			// should commit
			if ((args.commitEvery != 0 && timeSinceTranStart > args.commitEvery) || timeout || shouldCommitByIter) && hasLiveTransaction {
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
			timeout = args.duration != 0 && cur_elapsed >= args.duration
			if timeout || (args.exitAfterNCommit != 0 && numCommit == int(args.exitAfterNCommit)) {
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

			ret := t.ProcessFunc(ctx, t, args.procArgs)
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
						if err := tm.AbortTransaction(ctx, false, args.kvChangelogs, args.windowStoreChangelogs); err != nil {
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
		if err := tm.BeginTransaction(ctx, args.kvChangelogs, args.windowStoreChangelogs); err != nil {
			debug.Fprintf(os.Stderr, "[ERROR] transaction begin failed: %v", err)
			return fmt.Errorf("transaction begin failed: %v\n", err)
		}
		*hasLiveTransaction = true
		*commitTimer = time.Now()
		if args.fixedOutParNum != 0 {
			debug.Assert(len(args.sinks) == 1, "fixed out param is only usable when there's only one output stream")
			if err := tm.AddTopicPartition(ctx, args.sinks[0].TopicName(), []uint8{args.fixedOutParNum}); err != nil {
				debug.Fprintf(os.Stderr, "[ERROR] track topic partition failed: %v\n", err)
				return fmt.Errorf("track topic partition failed: %v\n", err)
			}
		}
		if *init && t.ResumeFunc != nil {
			t.ResumeFunc(t)
		}
		if !*init && t.InitFunc != nil {
			t.InitFunc(args.procArgs)
			*init = true
		}
	}
	if !*trackConsumePar {
		for _, src := range args.srcs {
			if err := tm.AddTopicTrackConsumedSeqs(ctx, src.TopicName(), []uint8{args.inParNum}); err != nil {
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
	ret.Counts = make(map[string]uint64)
	e2eTime := time.Since(startTime).Seconds()
	if warmupDuration != 0 && afterWarmup {
		e2eTime = time.Since(afterWarmupStart).Seconds()
	}
	ret.Duration = e2eTime
}

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
			Partition:      args.inParNum,
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
	err = tm.CommitTransaction(ctx, args.kvChangelogs, args.windowStoreChangelogs)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] commit failed: %v\n", err)
		return &common.FnOutput{Success: false, Message: fmt.Sprintf("commit failed: %v\n", err)}
	}
	*hasLiveTransaction = false
	*trackConsumePar = false
	return nil
}
