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
	"sharedlog-stream/pkg/stats"
	"sharedlog-stream/pkg/transaction"
	"sync"
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
	trackStreamAndConfigureExactlyOnce(args, tm,
		func(name string, stream *sharedlog_stream.ShardedSharedLogStream) {
			tm.RecordTopicStreams(name, stream)
			cmm.TrackStream(name, stream)
		})

	dctx, dcancel := context.WithCancel(ctx)
	tm.StartMonitorLog(dctx, dcancel)
	cmm.StartMonitorControlChannel(dctx)

	run := false

	latencies := stats.NewInt64Collector("latPerIter", stats.DEFAULT_COLLECT_DURATION)
	hasLiveTransaction := false
	trackConsumePar := false
	commitTimer := time.Now()

	idx := 0
	numCommit := 0
	debug.Fprintf(os.Stderr, "commit every(ms): %d, commit everyIter: %d, exitAfterNComm: %d\n",
		args.commitEvery, args.commitEveryNIter, args.exitAfterNCommit)
	init := false
	var once sync.Once
	warmupCheck := stats.NewWarmupChecker(args.warmup)
	for {
		ret := checkMonitorReturns(dctx, dcancel, args, cmm, tm, &run)
		if ret != nil {
			return ret
		}
		if run {
			procStart := time.Now()
			once.Do(func() {
				warmupCheck.StartWarmup()
			})
			warmupCheck.Check()
			timeSinceTranStart := time.Since(commitTimer)
			cur_elapsed := warmupCheck.ElapsedSinceInitial()
			timeout := args.duration != 0 && cur_elapsed >= args.duration
			shouldCommitByIter := args.commitEveryNIter != 0 &&
				uint32(idx)%args.commitEveryNIter == 0 && idx != 0
			shouldCommitByTime := (args.commitEvery != 0 && timeSinceTranStart > args.commitEvery)
			// debug.Fprintf(os.Stderr, "iter: %d, shouldCommitByIter: %v, timeSinceTranStart: %v, cur_elapsed: %v, duration: %v\n",
			// 	idx, shouldCommitByIter, timeSinceTranStart, cur_elapsed, duration)

			// should commit
			if (shouldCommitByTime || timeout || shouldCommitByIter) && hasLiveTransaction {
				err_out := t.commitTransaction(ctx, tm, args, &hasLiveTransaction, &trackConsumePar)
				if err_out != nil {
					return err_out
				}
				debug.Assert(!hasLiveTransaction, "after commit. there should be no live transaction\n")
				numCommit += 1
				debug.Fprintf(os.Stderr, "transaction committed\n")
			}

			// Exit routine
			cur_elapsed = warmupCheck.ElapsedSinceInitial()
			timeout = args.duration != 0 && cur_elapsed >= args.duration
			shouldExitAfterNCommit := (args.exitAfterNCommit != 0 && numCommit == int(args.exitAfterNCommit))
			if timeout || shouldExitAfterNCommit {
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
				latencies.AddSample(elapsed.Microseconds())
				ret := &common.FnOutput{
					Success: true,
				}
				updateReturnMetric(ret, &warmupCheck)
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
					updateReturnMetric(ret, &warmupCheck)
				} else {
					if hasLiveTransaction {
						if err := tm.AbortTransaction(ctx, false, args.kvChangelogs, args.windowStoreChangelogs); err != nil {
							return &common.FnOutput{Success: false, Message: fmt.Sprintf("abort failed: %v\n", err)}
						}
					}
				}
				return ret
			}
			if warmupCheck.AfterWarmup() {
				elapsed := time.Since(procStart)
				latencies.AddSample(elapsed.Microseconds())
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
		err := t.initAfterMarkOrCommit(ctx, args, tm, init)
		if err != nil {
			return err
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
