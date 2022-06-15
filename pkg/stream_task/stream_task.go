package stream_task

import (
	"context"
	"fmt"
	"os"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/consume_seq_num_manager/con_types"
	"sharedlog-stream/pkg/control_channel"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/proc_interface"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/store"
	"sharedlog-stream/pkg/store_restore"
	"sharedlog-stream/pkg/transaction"
	"sharedlog-stream/pkg/txn_data"
	"sync"
	"time"
)

type ProcessFunc func(ctx context.Context, task *StreamTask, args interface{}) *common.FnOutput

type StreamTask struct {
	appProcessFunc           ProcessFunc
	OffMu                    sync.Mutex
	CurrentOffset            map[string]uint64
	pauseFunc                func() *common.FnOutput
	resumeFunc               func(task *StreamTask)
	initFunc                 func(progArgs interface{})
	HandleErrFunc            func() error
	trackEveryForAtLeastOnce time.Duration
}

func updateSrcSinkCount(ret *common.FnOutput, srcsSinks proc_interface.SourcesSinks) {
	for _, src := range srcsSinks.Sources() {
		ret.Counts[src.Name()] = src.GetCount()
	}
	for _, sink := range srcsSinks.Sinks() {
		ret.Counts[sink.Name()] = sink.GetCount()
	}
}

func (t *StreamTask) ExecuteApp(ctx context.Context,
	streamTaskArgs *StreamTaskArgs,
	transaction bool,
	update_stats func(ret *common.FnOutput),
) *common.FnOutput {
	var ret *common.FnOutput
	if transaction {
		ret = t.SetupManagersAndProcessTransactional(ctx, streamTaskArgs)
	} else {
		ret = t.Process(ctx, streamTaskArgs)
	}
	if ret != nil && ret.Success {
		updateSrcSinkCount(ret, streamTaskArgs.procArgs)
		update_stats(ret)
	}
	return ret
}

func (t *StreamTask) SetupManagersAndProcessTransactional(ctx context.Context,
	streamTaskArgs *StreamTaskArgs,
) *common.FnOutput {
	tm, cmm, err := t.SetupManagers(ctx, streamTaskArgs)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	debug.Fprint(os.Stderr, "begin transaction processing\n")
	ret := t.ProcessWithTransaction(ctx, tm, cmm, streamTaskArgs)
	return ret
}

func (t *StreamTask) SetupManagers(ctx context.Context, streamTaskArgs *StreamTaskArgs,
) (*transaction.TransactionManager, *control_channel.ControlChannelManager, error) {
	debug.Fprint(os.Stderr, "setup transaction and control manager\n")
	tm, err := SetupTransactionManager(ctx, streamTaskArgs)
	if err != nil {
		return nil, nil, err
	}
	cmm, err := control_channel.NewControlChannelManager(streamTaskArgs.env, streamTaskArgs.appId,
		commtypes.SerdeFormat(streamTaskArgs.serdeFormat), streamTaskArgs.procArgs.CurEpoch())
	if err != nil {
		return nil, nil, err
	}
	debug.Fprint(os.Stderr, "start restore\n")
	offsetMap, err := getOffsetMap(ctx, tm, streamTaskArgs)
	if err != nil {
		return nil, nil, err
	}
	debug.Fprintf(os.Stderr, "got offset map: %v\n", offsetMap)
	err = restoreStateStore(ctx, tm, streamTaskArgs, offsetMap)
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
		err := tm.AddTopicSubstream(ctx, topicName, substreamId)
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
		kvchangelog.SetTrackParFunc(trackParFunc)
	}
	for _, wschangelog := range streamTaskArgs.windowStoreChangelogs {
		wschangelog.SetTrackParFunc(trackParFunc)
	}
	return tm, cmm, nil
}

func getOffsetMap(ctx context.Context, tm *transaction.TransactionManager, args *StreamTaskArgs) (map[string]uint64, error) {
	offsetMap := make(map[string]uint64)
	for _, src := range args.srcs {
		inputTopicName := src.TopicName()
		offset, err := transaction.CreateOffsetTopicAndGetOffset(ctx, tm, inputTopicName,
			uint8(src.Stream().NumPartition()), args.procArgs.SubstreamNum())
		if err != nil {
			return nil, fmt.Errorf("createOffsetTopicAndGetOffset failed: %v", err)
		}
		offsetMap[inputTopicName] = offset
		debug.Fprintf(os.Stderr, "tp %s offset %d\n", inputTopicName, offset)
	}
	return offsetMap, nil
}

func setOffsetOnStream(offsetMap map[string]uint64, args *StreamTaskArgs) {
	for _, src := range args.srcs {
		inputTopicName := src.TopicName()
		offset := offsetMap[inputTopicName]
		src.SetCursor(offset+1, args.procArgs.SubstreamNum())
	}
}

func restoreKVStore(ctx context.Context, tm *transaction.TransactionManager,
	args *StreamTaskArgs, offsetMap map[string]uint64,
) error {
	for _, kvchangelog := range args.kvChangelogs {
		if kvchangelog.TableType() == store.IN_MEM {
			topic := kvchangelog.ChangelogManager().TopicName()
			// offset stream is input stream
			if offset, ok := offsetMap[topic]; ok && offset != 0 {
				err := store_restore.RestoreChangelogKVStateStore(ctx,
					kvchangelog, offset)
				if err != nil {
					return fmt.Errorf("RestoreKVStateStore failed: %v", err)
				}
			} else {
				offset, err := transaction.CreateOffsetTopicAndGetOffset(ctx, tm, topic,
					kvchangelog.ChangelogManager().NumPartition(), kvchangelog.ParNum())
				if err != nil {
					return fmt.Errorf("createOffsetTopicAndGetOffset kv failed: %v", err)
				}
				if offset != 0 {
					err = store_restore.RestoreChangelogKVStateStore(ctx, kvchangelog, offset)
					if err != nil {
						return fmt.Errorf("RestoreKVStateStore2 failed: %v", err)
					}
				}
			}
		} else if kvchangelog.TableType() == store.MONGODB {
			if err := restoreMongoDBKVStore(ctx, tm, kvchangelog); err != nil {
				return err
			}
		}
	}
	return nil
}

func restoreChangelogBackedWindowStore(ctx context.Context, tm *transaction.TransactionManager,
	args *StreamTaskArgs, offsetMap map[string]uint64) error {
	for _, wschangelog := range args.windowStoreChangelogs {
		if wschangelog.TableType() == store.IN_MEM {
			topic := wschangelog.ChangelogManager().TopicName()
			// offset stream is input stream
			if offset, ok := offsetMap[topic]; ok && offset != 0 {
				err := store_restore.RestoreChangelogWindowStateStore(ctx, wschangelog, offset)
				if err != nil {
					return fmt.Errorf("RestoreWindowStateStore failed: %v", err)
				}
			} else {
				offset, err := transaction.CreateOffsetTopicAndGetOffset(ctx, tm, topic,
					wschangelog.ChangelogManager().NumPartition(), wschangelog.ParNum())
				if err != nil {
					return fmt.Errorf("createOffsetTopicAndGetOffset win failed: %v", err)
				}
				if offset != 0 {
					err = store_restore.RestoreChangelogWindowStateStore(ctx, wschangelog, offset)
					if err != nil {
						return fmt.Errorf("RestoreWindowStateStore2 failed: %v", err)
					}
				}
			}
		} else if wschangelog.TableType() == store.MONGODB {
			if err := restoreMongoDBWinStore(ctx, tm, wschangelog); err != nil {
				return err
			}
		}
	}
	return nil
}

func restoreStateStore(ctx context.Context, tm *transaction.TransactionManager, args *StreamTaskArgs, offsetMap map[string]uint64) error {
	if args.kvChangelogs != nil {
		err := restoreKVStore(ctx, tm, args, offsetMap)
		if err != nil {
			return err
		}
	}
	if args.windowStoreChangelogs != nil {
		err := restoreChangelogBackedWindowStore(ctx, tm, args, offsetMap)
		if err != nil {
			return err
		}
	}
	return nil
}

func restoreMongoDBKVStore(
	ctx context.Context,
	tm *transaction.TransactionManager,
	kvchangelog *store_restore.KVStoreChangelog,
) error {
	storeTranID, found, err := kvchangelog.KVExternalStore().GetTransactionID(ctx, kvchangelog.TabTranRepr())
	if err != nil {
		return err
	}
	if !found || tm.GetTransactionID() == storeTranID {
		return nil
	}

	seqNum, err := tm.FindConsumedSeqNumMatchesTransactionID(ctx,
		kvchangelog.InputStream().TopicName(), kvchangelog.ParNum(), storeTranID)
	if err != nil {
		return err
	}
	kvchangelog.InputStream().SetCursor(seqNum, kvchangelog.ParNum())
	if err = kvchangelog.KVExternalStore().StartTransaction(ctx); err != nil {
		return err
	}
	if err = kvchangelog.ExecuteRestoreFunc(ctx); err != nil {
		return err
	}
	return kvchangelog.KVExternalStore().CommitTransaction(ctx, tm.TransactionalId, tm.GetTransactionID())
}

func restoreMongoDBWinStore(
	ctx context.Context,
	tm *transaction.TransactionManager,
	kvchangelog *store_restore.WindowStoreChangelog,
) error {
	storeTranID, found, err := kvchangelog.WinExternalStore().GetTransactionID(ctx, tm.TransactionalId)
	if err != nil {
		return err
	}
	if !found || tm.GetTransactionID() == storeTranID {
		return nil
	}

	seqNum, err := tm.FindConsumedSeqNumMatchesTransactionID(ctx, kvchangelog.InputStream().TopicName(),
		kvchangelog.ParNum(), storeTranID)
	if err != nil {
		return err
	}
	kvchangelog.InputStream().SetCursor(seqNum, kvchangelog.ParNum())
	if err = kvchangelog.WinExternalStore().StartTransaction(ctx); err != nil {
		return err
	}
	if err = kvchangelog.ExecuteRestoreFunc(ctx); err != nil {
		return err
	}
	return kvchangelog.WinExternalStore().CommitTransaction(ctx, tm.TransactionalId, tm.GetTransactionID())
}

func SetupTransactionManager(
	ctx context.Context,
	args *StreamTaskArgs,
) (*transaction.TransactionManager, error) {
	tm, err := transaction.NewTransactionManager(ctx, args.env, args.transactionalId,
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
	args *StreamTaskArgs,
	offsetMap map[string]uint64,
) {
	for _, src := range args.srcs {
		offset := offsetMap[src.TopicName()]
		src.SetCursor(offset+1, args.procArgs.SubstreamNum())
	}
}

func TrackOffsetAndCommit(ctx context.Context,
	consumedSeqNumConfigs []con_types.ConsumedSeqNumConfig,
	tm *transaction.TransactionManager,
	kvchangelogs []*store_restore.KVStoreChangelog, winchangelogs []*store_restore.WindowStoreChangelog,
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

func (t *StreamTask) flushStreams(ctx context.Context, args *StreamTaskArgs) error {
	for _, sink := range args.sinks {
		if err := sink.Flush(ctx); err != nil {
			return err
		}
	}
	for _, kvchangelog := range args.kvChangelogs {
		if kvchangelog.ChangelogManager() != nil {
			if err := kvchangelog.ChangelogManager().Flush(ctx); err != nil {
				return err
			}
		}
	}
	for _, wschangelog := range args.windowStoreChangelogs {
		if wschangelog.ChangelogManager() != nil {
			if err := wschangelog.ChangelogManager().Flush(ctx); err != nil {
				return err
			}
		}
	}
	return nil
}

func (t *StreamTask) ProcessWithTransaction(
	ctx context.Context,
	tm *transaction.TransactionManager,
	cmm *control_channel.ControlChannelManager,
	args *StreamTaskArgs,
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
		if kvchangelog.ChangelogManager() != nil {
			err := kvchangelog.ChangelogManager().InTransaction(tm)
			if err != nil {
				return &common.FnOutput{Success: false, Message: err.Error()}
			}
			tm.RecordTopicStreams(kvchangelog.ChangelogManager().TopicName(), kvchangelog.ChangelogManager().Stream())
			cmm.TrackStream(kvchangelog.ChangelogManager().TopicName(), kvchangelog.ChangelogManager().Stream())
		}
	}
	for _, winchangelog := range args.windowStoreChangelogs {
		if winchangelog.ChangelogManager() != nil {
			err := winchangelog.ChangelogManager().InTransaction(tm)
			if err != nil {
				return &common.FnOutput{Success: false, Message: err.Error()}
			}
			tm.RecordTopicStreams(winchangelog.ChangelogManager().TopicName(), winchangelog.ChangelogManager().Stream())
			cmm.TrackStream(winchangelog.ChangelogManager().TopicName(), winchangelog.ChangelogManager().Stream())
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
				m.FinishedPrevTask, args.procArgs.FuncName(), m.Epoch, args.procArgs.CurEpoch())
			if m.FinishedPrevTask == args.procArgs.FuncName() && m.Epoch+1 == args.procArgs.CurEpoch() {
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

			ret := t.appProcessFunc(ctx, t, args.procArgs)
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

func (t *StreamTask) startNewTransaction(ctx context.Context, args *StreamTaskArgs, tm *transaction.TransactionManager,
	hasLiveTransaction *bool, trackConsumePar *bool, init *bool, commitTimer *time.Time,
) error {
	if !*hasLiveTransaction {
		if err := tm.BeginTransaction(ctx, args.kvChangelogs, args.windowStoreChangelogs); err != nil {
			debug.Fprintf(os.Stderr, "[ERROR] transaction begin failed: %v", err)
			return fmt.Errorf("transaction begin failed: %v\n", err)
		}
		*hasLiveTransaction = true
		*commitTimer = time.Now()
		debug.Fprintf(os.Stderr, "fixedOutParNum: %d\n", args.fixedOutParNum)
		if args.fixedOutParNum != -1 {
			debug.Assert(len(args.sinks) == 1, "fixed out param is only usable when there's only one output stream")
			debug.Fprintf(os.Stderr, "%s tracking substream %d\n", args.sinks[0].TopicName(), args.fixedOutParNum)
			if err := tm.AddTopicSubstream(ctx, args.sinks[0].TopicName(), uint8(args.fixedOutParNum)); err != nil {
				debug.Fprintf(os.Stderr, "[ERROR] track topic partition failed: %v\n", err)
				return fmt.Errorf("track topic partition failed: %v\n", err)
			}
		}
		if *init && t.resumeFunc != nil {
			t.resumeFunc(t)
		}
		if !*init {
			args.procArgs.StartWarmup()
			if t.initFunc != nil {
				t.initFunc(args.procArgs)
			}
			*init = true
		}
	}
	if !*trackConsumePar {
		for _, src := range args.srcs {
			if err := tm.AddTopicTrackConsumedSeqs(ctx, src.TopicName(), args.procArgs.SubstreamNum()); err != nil {
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
	tm *transaction.TransactionManager,
	args *StreamTaskArgs,
	hasLiveTransaction *bool,
	trackConsumePar *bool,
) *common.FnOutput {
	// debug.Fprintf(os.Stderr, "about to pause\n")
	if t.pauseFunc != nil {
		if ret := t.pauseFunc(); ret != nil {
			return ret
		}
	}
	// debug.Fprintf(os.Stderr, "after pause\n")
	consumedSeqNumConfigs := make([]con_types.ConsumedSeqNumConfig, 0)
	t.OffMu.Lock()
	for topic, offset := range t.CurrentOffset {
		consumedSeqNumConfigs = append(consumedSeqNumConfigs, con_types.ConsumedSeqNumConfig{
			TopicToTrack:   topic,
			TaskId:         tm.GetCurrentTaskId(),
			TaskEpoch:      tm.GetCurrentEpoch(),
			Partition:      args.procArgs.SubstreamNum(),
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
