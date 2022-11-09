package stream_task

import (
	"context"
	"fmt"
	"os"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/control_channel"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/epoch_manager"
	"sharedlog-stream/pkg/exactly_once_intr"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/snapshot_store"
	"sharedlog-stream/pkg/stats"
	"sharedlog-stream/pkg/store"
	"sharedlog-stream/pkg/txn_data"
	"sync"
	"time"

	"cs.utexas.edu/zjia/faas/types"
)

var (
	CREATE_SNAPSHOT = checkCreateSnapshot()
)

func checkCreateSnapshot() bool {
	createSnapshotStr := os.Getenv("CREATE_SNAPSHOT")
	createSnapshot := false
	if createSnapshotStr == "true" || createSnapshotStr == "1" {
		createSnapshot = true
	}
	fmt.Fprintf(os.Stderr, "env str: %s, create snapshot: %v\n", createSnapshotStr, createSnapshot)
	return createSnapshot
}

func SetKVStoreSnapshot[K, V any](ctx context.Context, env types.Environment,
	em *epoch_manager.EpochManager, rs *snapshot_store.RedisSnapshotStore,
	kvstore store.KeyValueStoreBackedByChangelogG[K, V], payloadSerde commtypes.SerdeG[commtypes.PayloadArr],
) {
	kvstore.SetSnapshotCallback(ctx, func(ctx context.Context, logOff uint64, snapshot []commtypes.KeyValuePair[K, V]) error {
		kvPairSerdeG := kvstore.GetKVSerde()
		outBin := make([][]byte, 0, len(snapshot))
		for _, kv := range snapshot {
			bin, err := kvPairSerdeG.Encode(kv)
			if err != nil {
				return err
			}
			outBin = append(outBin, bin)
		}
		out, err := payloadSerde.Encode(commtypes.PayloadArr{
			Payloads: outBin,
		})
		if err != nil {
			return err
		}
		fmt.Fprintf(os.Stderr, "snapshot size: %d\n", len(out))
		return rs.StoreSnapshot(ctx, env, out, kvstore.ChangelogTopicName(), logOff)
	})
}

func SetWinStoreSnapshot[K, V any](ctx context.Context, env types.Environment,
	em *epoch_manager.EpochManager, rs *snapshot_store.RedisSnapshotStore,
	winStore store.WindowStoreBackedByChangelogG[K, V], payloadSerde commtypes.SerdeG[commtypes.PayloadArr],
) {
	winStore.SetWinSnapshotCallback(ctx, func(ctx context.Context, logOff uint64, snapshot []commtypes.KeyValuePair[commtypes.KeyAndWindowStartTsG[K], V]) error {
		kvPairSerdeG := winStore.GetKVSerde()
		outBin := make([][]byte, 0, len(snapshot))
		for _, kv := range snapshot {
			bin, err := kvPairSerdeG.Encode(kv)
			if err != nil {
				return err
			}
			outBin = append(outBin, bin)
		}
		out, err := payloadSerde.Encode(commtypes.PayloadArr{
			Payloads: outBin,
		})
		if err != nil {
			return err
		}
		fmt.Fprintf(os.Stderr, "snapshot size: %d\n", len(out))
		return rs.StoreSnapshot(ctx, env, out, winStore.ChangelogTopicName(), logOff)
	})
}

func SetupManagersForEpoch(ctx context.Context,
	args *StreamTaskArgs, rs *snapshot_store.RedisSnapshotStore, setupSnapshotCallback SetupSnapshotCallbackFunc,
) (*epoch_manager.EpochManager, *control_channel.ControlChannelManager, error) {
	em, err := epoch_manager.NewEpochManager(args.env, args.transactionalId, args.serdeFormat)
	if err != nil {
		return nil, nil, err
	}
	initEmStart := time.Now()
	recentMeta, rawMetaMsg, err := em.Init(ctx)
	if err != nil {
		return nil, nil, err
	}
	initEmElapsed := time.Since(initEmStart)
	fmt.Fprintf(os.Stderr, "Init EpochManager took %v\n", initEmElapsed)
	err = configChangelogExactlyOnce(em, args)
	if err != nil {
		return nil, nil, err
	}
	err = setupSnapshotCallback(ctx, args.env, args.serdeFormat, em, rs)
	if err != nil {
		return nil, nil, err
	}
	if recentMeta != nil {
		fmt.Fprint(os.Stderr, "start restore\n")
		restoreBeg := time.Now()
		offsetMap := recentMeta.ConSeqNums
		auxData := rawMetaMsg.AuxData
		auxMetaSeq := rawMetaMsg.LogSeqNum
		if len(offsetMap) == 0 {
			// the last commit doesn't consume anything
			var meta *commtypes.EpochMarker
			var rawMsg *commtypes.RawMsg
			meta, rawMsg, err = em.FindLastEpochMetaThatHasConsumed(ctx, rawMetaMsg.LogSeqNum)
			if err != nil {
				return nil, nil, err
			}
			if len(auxData) == 0 && len(rawMsg.AuxData) != 0 {
				auxData = rawMsg.AuxData
				auxMetaSeq = rawMsg.LogSeqNum
			}
			if meta == nil {
				panic("consumed sequnce number must be recorded in epoch meta")
			} else {
				offsetMap = meta.ConSeqNums
			}
		}
		if CREATE_SNAPSHOT {
			loadSnapBeg := time.Now()
			if len(auxData) == 0 {
				debug.Fprintf(os.Stderr, "read back for snapshot from 0x%x\n", rawMetaMsg.LogSeqNum)
				auxData, auxMetaSeq, err = em.FindLastEpochMetaWithAuxData(ctx, rawMetaMsg.LogSeqNum)
				if err != nil {
					return nil, nil, fmt.Errorf("[ERR] FindLastEpochMetaWithAuxData: %v", err)
				}
			}
			err = loadSnapshot(ctx, args, auxData, auxMetaSeq, rs)
			if err != nil {
				return nil, nil, err
			}
			loadSnapElapsed := time.Since(loadSnapBeg)
			fmt.Fprintf(os.Stderr, "load snapshot took %v\n", loadSnapElapsed)
		}
		restoreSsBeg := time.Now()
		err = restoreStateStore(ctx, args, offsetMap)
		if err != nil {
			return nil, nil, err
		}
		restoreStateStoreElapsed := time.Since(restoreSsBeg)
		fmt.Fprintf(os.Stderr, "restore state store took %v\n", restoreStateStoreElapsed)
		setOffsetOnStream(offsetMap, args)
		restoreElapsed := time.Since(restoreBeg)
		fmt.Fprintf(os.Stderr, "down restore, elapsed: %v\n", restoreElapsed)
	}
	cmm, err := control_channel.NewControlChannelManager(args.env, args.appId,
		args.serdeFormat, args.ectx.CurEpoch(), args.ectx.SubstreamNum())
	if err != nil {
		return nil, nil, err
	}
	trackParFunc := exactly_once_intr.TrackProdSubStreamFunc(
		func(ctx context.Context, kBytes []byte,
			topicName string, substreamId uint8,
		) error {
			_ = em.AddTopicSubstream(ctx, topicName, substreamId)
			// control_channel.TrackAndAppendKeyMapping(ctx, cmm, kBytes, substreamId, topicName)
			return nil
		})
	recordFinish := func(ctx context.Context, funcName string, instanceID uint8) error {
		return cmm.RecordPrevInstanceFinish(ctx, funcName, instanceID, args.ectx.CurEpoch())
	}
	updateFuncs(args, trackParFunc, recordFinish)
	return em, cmm, nil
}

func loadSnapshot(ctx context.Context,
	args *StreamTaskArgs, auxData []byte, auxMetaSeq uint64, rs *snapshot_store.RedisSnapshotStore,
) error {
	if len(auxData) > 0 {
		uint16Serde := commtypes.Uint16SerdeG{}
		ret, err := uint16Serde.Decode(auxData)
		if err != nil {
			return fmt.Errorf("[ERR] Decode: %v", err)
		}
		if ret == 1 {
			payloadSerde, err := commtypes.GetPayloadArrSerdeG(args.serdeFormat)
			if err != nil {
				return err
			}
			for kvchTp, kvchangelog := range args.kvChangelogs {
				snapArr, err := rs.GetSnapshot(ctx, kvchTp, auxMetaSeq)
				if err != nil {
					return err
				}
				if len(snapArr) > 0 {
					payloadArr, err := payloadSerde.Decode(snapArr)
					if err != nil {
						return err
					}
					err = kvchangelog.RestoreFromSnapshot(payloadArr.Payloads)
					if err != nil {
						return fmt.Errorf("[ERR] restore kv from snapshot: %v", err)
					}
					kvchangelog.Stream().SetCursor(auxMetaSeq+1, kvchangelog.SubstreamNum())
				}
			}
			for wscTp, wsc := range args.windowStoreChangelogs {
				snapArr, err := rs.GetSnapshot(ctx, wscTp, auxMetaSeq)
				if err != nil {
					return err
				}
				if snapArr != nil {
					payloadArr, err := payloadSerde.Decode(snapArr)
					if err != nil {
						return err
					}
					err = wsc.RestoreFromSnapshot(ctx, payloadArr.Payloads)
					if err != nil {
						return fmt.Errorf("[ERR] restore window table from snapshot: %v", err)
					}
					wsc.Stream().SetCursor(auxMetaSeq+1, wsc.SubstreamNum())
				}
			}
		}
	} else {
		fmt.Fprintf(os.Stderr, "no snapshot found\n")
	}
	return nil
}

func processInEpoch(
	ctx context.Context,
	t *StreamTask,
	em *epoch_manager.EpochManager,
	cmm *control_channel.ControlChannelManager,
	args *StreamTaskArgs,
	rs *snapshot_store.RedisSnapshotStore,
) *common.FnOutput {
	err := trackStreamAndConfigureExactlyOnce(args, em,
		func(name string, stream *sharedlog_stream.ShardedSharedLogStream) {
		})
	if err != nil {
		return common.GenErrFnOutput(fmt.Errorf("trackStreamAndConfigExactlyOnce: %v", err))
	}

	dctx, dcancel := context.WithCancel(ctx)
	defer dcancel()
	// em.StartMonitorLog(dctx, dcancel)
	debug.Fprintf(os.Stderr, "start restore mapping")
	err = cmm.RestoreMappingAndWaitForPrevTask(
		dctx, args.ectx.FuncName(), CREATE_SNAPSHOT, args.serdeFormat,
		args.kvChangelogs, args.windowStoreChangelogs, rs)
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	execIntrMs := stats.NewPrintLogStatsCollector[int64]("execIntrMs")
	thisAndLastCmtMs := stats.NewPrintLogStatsCollector[int64]("thisAndLastCmtMs")
	markPartUs := stats.NewPrintLogStatsCollector[int64]("markPartUs")
	snapshotTime := make([]int64, 0, 8)
	// run := false
	hasProcessData := false
	init := false
	paused := false
	// latencies := stats.NewInt64Collector("latPerIter", stats.DEFAULT_COLLECT_DURATION)
	markTimer := time.Now()
	snapshotTimer := time.Now()
	var once sync.Once
	warmupCheck := stats.NewWarmupChecker(args.warmup)
	fmt.Fprintf(os.Stderr, "commit every(ms): %v, waitEndMark: %v, fixed output parNum: %d, snapshot every(s): %v\n",
		args.commitEvery, args.waitEndMark, args.fixedOutParNum, args.snapshotEvery)
	testForFail := false
	var failAfter time.Duration
	failParam, ok := args.testParams[args.ectx.FuncName()]
	if ok && failParam.InstanceId == args.ectx.SubstreamNum() {
		testForFail = true
		failAfter = time.Duration(failParam.FailAfterS) * time.Second
		fmt.Fprintf(os.Stderr, "%s(%d) will fail after %v\n",
			args.ectx.FuncName(), args.ectx.SubstreamNum(), failAfter)
	}
	epochMarkTime := make([]int64, 0, 1024)
	for {
		select {
		case <-dctx.Done():
			return &common.FnOutput{Success: true, Message: "exit due to ctx cancel"}
		default:
		}
		// procStart := time.Now()
		once.Do(func() {
			warmupCheck.StartWarmup()
		})
		warmupCheck.Check()
		timeSinceLastMark := time.Since(markTimer)
		shouldMarkByTime := args.commitEvery != 0 && timeSinceLastMark >= args.commitEvery
		if shouldMarkByTime && hasProcessData {
			execIntrMs.AddSample(timeSinceLastMark.Milliseconds())
			markBegin := time.Now()
			if t.pauseFunc != nil {
				if ret := t.pauseFunc(); ret != nil {
					return ret
				}
				paused = true
			}

			flushAllStart := stats.TimerBegin()
			f, err := flushStreams(dctx, args)
			if err != nil {
				return common.GenErrFnOutput(err)
			}
			flushTime := stats.Elapsed(flushAllStart).Microseconds()

			mPartBeg := time.Now()
			logOff, shouldExit, err := markEpoch(dctx, em, t, args)
			if err != nil {
				return common.GenErrFnOutput(err)
			}

			if CREATE_SNAPSHOT && args.snapshotEvery != 0 && time.Since(snapshotTimer) > args.snapshotEvery {
				snStart := time.Now()
				createSnapshot(args, logOff)
				elapsed := time.Since(snStart)
				snapshotTime = append(snapshotTime, elapsed.Microseconds())
				snapshotTimer = time.Now()
			}
			markElapsed := time.Since(markBegin)
			mPartElapsed := time.Since(mPartBeg)
			epochMarkTime = append(epochMarkTime, markElapsed.Microseconds())
			t.flushStageTime.AddSample(flushTime)
			markPartUs.AddSample(mPartElapsed.Microseconds())
			if f > 0 {
				t.flushAtLeastOne.AddSample(flushTime)
			}
			t.epochMarkTimes += 1
			if shouldExit {
				ret := &common.FnOutput{Success: true}
				if testForFail {
					ret = &common.FnOutput{Success: true, Message: common_errors.ErrReturnDueToTest.Error()}
				}
				fmt.Fprintf(os.Stderr, "{epoch mark time: %v}\n", epochMarkTime)
				fmt.Fprintf(os.Stderr, "epoch_mark_times: %d\n", t.epochMarkTimes)
				t.flushStageTime.PrintRemainingStats()
				execIntrMs.PrintRemainingStats()
				thisAndLastCmtMs.PrintRemainingStats()
				updateReturnMetric(ret, &warmupCheck,
					false, t.GetEndDuration(), args.ectx.SubstreamNum())
				return ret
			}
			hasProcessData = false
		}
		// Exit routine
		cur_elapsed := warmupCheck.ElapsedSinceInitial()
		timeout := args.duration != 0 && cur_elapsed >= args.duration
		if (!args.waitEndMark && timeout) || (testForFail && cur_elapsed >= failAfter) {
			r := finalMark(dctx, t, args, em, cmm, snapshotTime, epochMarkTime, &markPartUs)
			if r != nil {
				return r
			}
			ret := &common.FnOutput{Success: true}
			if testForFail {
				ret = &common.FnOutput{Success: true, Message: common_errors.ErrReturnDueToTest.Error()}
			}
			markPartUs.PrintRemainingStats()
			execIntrMs.PrintRemainingStats()
			thisAndLastCmtMs.PrintRemainingStats()
			updateReturnMetric(ret, &warmupCheck,
				false, t.GetEndDuration(), args.ectx.SubstreamNum())
			return ret
		}

		// init
		if !hasProcessData {
			err := initAfterMarkOrCommit(dctx, t, args, em, &init, &paused)
			if err != nil {
				return common.GenErrFnOutput(err)
			}
			btwThisLastCmt := time.Since(markTimer)
			thisAndLastCmtMs.AddSample(btwThisLastCmt.Milliseconds())
			markTimer = time.Now()
		}
		app_ret, ctrlRawMsgOp := t.appProcessFunc(dctx, t, args.ectx)
		if app_ret != nil {
			if app_ret.Success {
				debug.Assert(ctrlRawMsgOp.IsNone(), "when timeout, ctrlMsg should not be returned")
				// consume timeout but not sure whether there's more data is coming; continue to process
				continue
			}
			return app_ret
		}
		if !hasProcessData {
			hasProcessData = true
		}
		ctrlRawMsg, ok := ctrlRawMsgOp.Take()
		if ok {
			fmt.Fprintf(os.Stderr, "exit due to ctrlMsg\n")
			r := finalMark(dctx, t, args, em, cmm, snapshotTime, epochMarkTime, &markPartUs)
			if r != nil {
				return r
			}
			markPartUs.PrintRemainingStats()
			execIntrMs.PrintRemainingStats()
			thisAndLastCmtMs.PrintRemainingStats()
			return handleCtrlMsg(dctx, ctrlRawMsg, t, args, &warmupCheck)
		}
	}
}

func finalMark(dctx context.Context, t *StreamTask, args *StreamTaskArgs,
	em *epoch_manager.EpochManager, cmm *control_channel.ControlChannelManager,
	snapshotTime []int64, epochMarkTime []int64, markPartUs *stats.PrintLogStatsCollector[int64],
) *common.FnOutput {
	markBegin := time.Now()
	if t.pauseFunc != nil {
		if ret := t.pauseFunc(); ret != nil {
			return ret
		}
	}
	flushAllStart := stats.TimerBegin()
	f, err := flushStreams(dctx, args)
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	err = getKeyMappingAndFlush(dctx, args, cmm)
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	flushTime := stats.Elapsed(flushAllStart).Microseconds()
	mPartBeg := time.Now()
	logOff, _, err := markEpoch(dctx, em, t, args)
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	if CREATE_SNAPSHOT && args.snapshotEvery != 0 {
		snStart := time.Now()
		createSnapshot(args, logOff)
		elapsed := time.Since(snStart)
		snapshotTime = append(snapshotTime, elapsed.Microseconds())
		for _, kv := range args.kvChangelogs {
			err = kv.WaitForAllSnapshot()
			if err != nil {
				return common.GenErrFnOutput(err)
			}
		}
		for _, wsc := range args.windowStoreChangelogs {
			err = wsc.WaitForAllSnapshot()
			if err != nil {
				return common.GenErrFnOutput(err)
			}
		}
		fmt.Fprintf(os.Stderr, "snapshot time: %v\n", snapshotTime)
	}
	markElapsed := time.Since(markBegin)
	mPartElapsed := time.Since(mPartBeg)
	epochMarkTime = append(epochMarkTime, markElapsed.Microseconds())
	markPartUs.AddSample(mPartElapsed.Microseconds())
	t.flushStageTime.AddSample(flushTime)
	if f > 0 {
		t.flushAtLeastOne.AddSample(flushTime)
	}
	fmt.Fprintf(os.Stderr, "{epoch mark time: %v}\n", epochMarkTime)
	fmt.Fprintf(os.Stderr, "epoch_mark_times: %d\n", t.epochMarkTimes)
	t.flushStageTime.PrintRemainingStats()
	t.flushAtLeastOne.PrintRemainingStats()
	return nil
}

func getKeyMappingAndFlush(ctx context.Context, args *StreamTaskArgs, cmm *control_channel.ControlChannelManager) error {
	kms := make(map[string][]txn_data.KeyMaping)
	for _, kv := range args.kvChangelogs {
		kv.BuildKeyMeta(ctx, kms)
	}
	for _, wsc := range args.windowStoreChangelogs {
		wsc.BuildKeyMeta(kms)
	}
	if len(kms) > 0 {
		return cmm.OutputKeyMapping(ctx, kms)
	}
	return nil
}

func markEpoch(ctx context.Context, em *epoch_manager.EpochManager,
	t *StreamTask, args *StreamTaskArgs,
) (logOff uint64, shouldExit bool, err error) {
	rawMsgs, err := em.SyncToRecent(ctx)
	if err != nil {
		return 0, false, err
	}
	for _, rawMsg := range rawMsgs {
		if rawMsg.Mark == commtypes.FENCE {
			if rawMsg.ProdId.TaskId == em.TaskId && rawMsg.ProdId.TaskEpoch > em.GetCurrentEpoch() {
				return 0, true, nil
			}
		}
	}
	// prepareStart := stats.TimerBegin()
	epochMarker, err := epoch_manager.GenEpochMarker(ctx, em, args.ectx.Consumers(), args.ectx.Producers(),
		args.kvChangelogs, args.windowStoreChangelogs)
	if err != nil {
		return 0, false, err
	}
	epochMarkerTags, epochMarkerTopics := em.GenTagsAndTopicsForEpochMarker()
	epoch_manager.CleanupState(em, args.ectx.Producers(), args.kvChangelogs, args.windowStoreChangelogs)
	// prepareTime := stats.Elapsed(prepareStart).Microseconds()

	// mStart := stats.TimerBegin()
	logOff, err = em.MarkEpoch(ctx, epochMarker, epochMarkerTags, epochMarkerTopics)
	if err != nil {
		return 0, false, err
	}
	// mElapsed := stats.Elapsed(mStart).Microseconds()
	// t.markEpochTime.AddSample(mElapsed)
	// t.markEpochPrepare.AddSample(prepareTime)
	return logOff, false, nil
}
