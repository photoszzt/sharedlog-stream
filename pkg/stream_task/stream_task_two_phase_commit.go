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
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/transaction"
	"sharedlog-stream/pkg/txn_data"
	"time"
)

func (t *StreamTask) setupManagersFor2pc(ctx context.Context, streamTaskArgs *StreamTaskArgs,
) (*transaction.TransactionManager, *control_channel.ControlChannelManager, error) {
	debug.Fprint(os.Stderr, "setup transaction and control manager\n")
	tm, err := SetupTransactionManager(ctx, streamTaskArgs)
	if err != nil {
		return nil, nil, err
	}
	cmm, err := control_channel.NewControlChannelManager(streamTaskArgs.env, streamTaskArgs.appId,
		commtypes.SerdeFormat(streamTaskArgs.serdeFormat), streamTaskArgs.ectx.CurEpoch())
	if err != nil {
		return nil, nil, err
	}
	debug.Fprint(os.Stderr, "start restore\n")
	offsetMap, err := getOffsetMap(ctx, tm, streamTaskArgs)
	if err != nil {
		return nil, nil, err
	}
	debug.Fprintf(os.Stderr, "got offset map: %v\n", offsetMap)
	err = configChangelogExactlyOnce(tm, streamTaskArgs)
	if err != nil {
		return nil, nil, err
	}
	err = restoreStateStore(ctx, streamTaskArgs, offsetMap)
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
	updateFuncs(streamTaskArgs, trackParFunc, recordFinish)
	return tm, cmm, nil
}

/*
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
*/

func SetupTransactionManager(
	ctx context.Context,
	args *StreamTaskArgs,
) (*transaction.TransactionManager, error) {
	tm, err := transaction.NewTransactionManager(ctx, args.env,
		args.transactionalId, args.serdeFormat)
	if err != nil {
		return nil, fmt.Errorf("NewTransactionManager failed: %v", err)
	}
	err = tm.InitTransaction(ctx)
	if err != nil {
		return nil, fmt.Errorf("InitTransaction failed: %v", err)
	}

	return tm, nil
}

func (t *StreamTask) processWithTransaction(
	ctx context.Context,
	tm *transaction.TransactionManager,
	cmm *control_channel.ControlChannelManager,
	args *StreamTaskArgs,
) *common.FnOutput {
	debug.Assert(len(args.ectx.Consumers()) >= 1, "Srcs should be filled")
	debug.Assert(args.env != nil, "env should be filled")
	debug.Assert(args.ectx != nil, "program args should be filled")
	for _, src := range args.ectx.Consumers() {
		if !src.IsInitialSource() {
			src.ConfigExactlyOnce(args.serdeFormat, args.guarantee)
		}
		cmm.TrackStream(src.TopicName(), src.Stream().(*sharedlog_stream.ShardedSharedLogStream))
	}
	for _, sink := range args.ectx.Producers() {
		sink.ConfigExactlyOnce(tm, args.guarantee)
		tm.RecordTopicStreams(sink.TopicName(), sink.Stream())
		cmm.TrackStream(sink.TopicName(), sink.Stream())
	}

	for _, kvchangelog := range args.kvChangelogs {
		if kvchangelog.ChangelogManager() != nil {
			tm.RecordTopicStreams(kvchangelog.ChangelogManager().TopicName(), kvchangelog.ChangelogManager().Stream())
			cmm.TrackStream(kvchangelog.ChangelogManager().TopicName(),
				kvchangelog.ChangelogManager().Stream())
		}
	}
	for _, winchangelog := range args.windowStoreChangelogs {
		if winchangelog.ChangelogManager() != nil {
			tm.RecordTopicStreams(winchangelog.ChangelogManager().TopicName(), winchangelog.ChangelogManager().Stream())
			cmm.TrackStream(winchangelog.ChangelogManager().TopicName(),
				winchangelog.ChangelogManager().Stream())
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
				m.FinishedPrevTask, args.ectx.FuncName(), m.Epoch, args.ectx.CurEpoch())
			if m.FinishedPrevTask == args.ectx.FuncName() && m.Epoch+1 == args.ectx.CurEpoch() {
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

			ret := t.appProcessFunc(ctx, t, args.ectx)
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
			sinks := args.ectx.Producers()
			debug.Assert(len(sinks) == 1, "fixed out param is only usable when there's only one output stream")
			debug.Fprintf(os.Stderr, "%s tracking substream %d\n", sinks[0].TopicName(), args.fixedOutParNum)
			if err := tm.AddTopicSubstream(ctx, sinks[0].TopicName(), uint8(args.fixedOutParNum)); err != nil {
				debug.Fprintf(os.Stderr, "[ERROR] track topic partition failed: %v\n", err)
				return fmt.Errorf("track topic partition failed: %v\n", err)
			}
		}
		if *init && t.resumeFunc != nil {
			t.resumeFunc(t)
		}
		if !*init {
			args.ectx.StartWarmup()
			if t.initFunc != nil {
				t.initFunc(args.ectx)
			}
			*init = true
		}
	}
	if !*trackConsumePar {
		for _, src := range args.ectx.Consumers() {
			if err := tm.AddTopicTrackConsumedSeqs(ctx, src.TopicName(), args.ectx.SubstreamNum()); err != nil {
				return fmt.Errorf("add offsets failed: %v\n", err)
			}
		}
		*trackConsumePar = true
	}
	return nil
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
	for topic, offset := range t.CurrentConsumeOffset {
		consumedSeqNumConfigs = append(consumedSeqNumConfigs, con_types.ConsumedSeqNumConfig{
			TopicToTrack:   topic,
			TaskId:         tm.GetCurrentTaskId(),
			TaskEpoch:      tm.GetCurrentEpoch(),
			Partition:      args.ectx.SubstreamNum(),
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
