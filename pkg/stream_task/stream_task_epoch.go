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
	"sharedlog-stream/pkg/env_config"
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

func SetKVStoreWithChangelogSnapshot[K, V any](
	ctx context.Context,
	env types.Environment,
	rs *snapshot_store.RedisSnapshotStore,
	kvstore store.KeyValueStoreBackedByChangelogG[K, V],
	payloadSerde commtypes.SerdeG[commtypes.PayloadArr],
) {
	kvstore.SetSnapshotCallback(ctx,
		func(ctx context.Context,
			tpLogoff []commtypes.TpLogOff,
			unprocessed []commtypes.ChkptMetaData,
			snapshot []commtypes.KeyValuePair[K, V],
		) error {
			out, err := encodeKVSnapshot[K, V](kvstore, snapshot, payloadSerde)
			if err != nil {
				return err
			}
			fmt.Fprintf(os.Stderr, "kv snapshot size: %d, store snapshot at %x\n", len(out), tpLogoff[0].LogOff)
			return rs.StoreSnapshot(ctx, env, out, kvstore.ChangelogTopicName(), tpLogoff[0].LogOff)
		})
}

func SetWinStoreWithChangelogSnapshot[K, V any](
	ctx context.Context,
	env types.Environment,
	rs *snapshot_store.RedisSnapshotStore,
	winStore store.WindowStoreBackedByChangelogG[K, V],
	payloadSerde commtypes.SerdeG[commtypes.PayloadArr],
) {
	winStore.SetWinSnapshotCallback(ctx,
		func(ctx context.Context, tpLogOff []commtypes.TpLogOff,
			chkptMeta []commtypes.ChkptMetaData,
			snapshot []commtypes.KeyValuePair[commtypes.KeyAndWindowStartTsG[K], V],
		) error {
			out, err := encodeWinSnapshot[K, V](winStore, snapshot, payloadSerde)
			if err != nil {
				return err
			}
			fmt.Fprintf(os.Stderr, "win snapshot size: %d, store snapshot at %x\n", len(out), tpLogOff[0].LogOff)
			return rs.StoreSnapshot(ctx, env, out, winStore.ChangelogTopicName(), tpLogOff[0].LogOff)
		})
}

func SetupManagersForEpoch(ctx context.Context,
	args *StreamTaskArgs, rs *snapshot_store.RedisSnapshotStore,
	setupSnapshotCallback SetupSnapshotCallbackFunc,
) (*epoch_manager.EpochManager, *control_channel.ControlChannelManager, error) {
	em, err := epoch_manager.NewEpochManager(args.env, args.transactionalId, args.serdeFormat)
	if err != nil {
		return nil, nil, err
	}
	cmm, err := control_channel.NewControlChannelManager(args.env, args.appId,
		args.serdeFormat, args.bufMaxSize, args.ectx.CurEpoch(), args.ectx.SubstreamNum())
	if err != nil {
		return nil, nil, err
	}
	debug.Fprintf(os.Stderr, "[%d] start restore potential mapping\n", args.ectx.SubstreamNum())
	err = cmm.RestoreMappingAndWaitForPrevTask(
		ctx, args.ectx.FuncName(), env_config.CREATE_SNAPSHOT, args.serdeFormat,
		args.kvChangelogs, args.windowStoreChangelogs, rs)
	if err != nil {
		return nil, nil, fmt.Errorf("RestoreMappingAndWaitForPrevTask: %v", err)
	}
	fmt.Fprintf(os.Stderr, "[%d] restore potential mapping done\n", args.ectx.SubstreamNum())
	initEmStart := time.Now()
	recentMeta, rawMetaMsg, err := em.Init(ctx)
	if err != nil {
		return nil, nil, err
	}
	initEmElapsed := time.Since(initEmStart)
	fmt.Fprintf(os.Stderr, "[%d] Init EpochManager took %v, task epoch %#x, task id %#x\n",
		args.ectx.SubstreamNum(), initEmElapsed, em.GetCurrentEpoch(), em.GetCurrentTaskId())
	configChangelogExactlyOnce(em, args)
	err = setupSnapshotCallback(ctx, args.env, args.serdeFormat, rs)
	if err != nil {
		return nil, nil, err
	}
	lastMark := uint64(0)
	if recentMeta != nil {
		restoreBeg := time.Now()
		offsetMap := recentMeta.ConSeqNums
		auxData := rawMetaMsg.AuxData
		auxMetaSeq := rawMetaMsg.LogSeqNum
		fmt.Fprintf(os.Stderr, "[%d] start restore, recent AuxData: %v, recent auxMetaSeq: %#x\n",
			args.ectx.SubstreamNum(), auxData, auxMetaSeq)
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
		if env_config.CREATE_SNAPSHOT {
			loadSnapBeg := time.Now()
			if len(auxData) == 0 {
				fmt.Fprintf(os.Stderr, "[%d] read back for snapshot from 0x%x\n", args.ectx.SubstreamNum(), rawMetaMsg.LogSeqNum)
				auxData, auxMetaSeq, err = em.FindLastEpochMetaWithAuxData(ctx, rawMetaMsg.LogSeqNum)
				if err != nil {
					return nil, nil, fmt.Errorf("[ERR] FindLastEpochMetaWithAuxData: %v", err)
				}
			}
			fmt.Fprintf(os.Stderr, "[%d] auxData: %v, auxMetaSeq: %#x\n", args.ectx.SubstreamNum(), auxData, auxMetaSeq)
			err = loadSnapshot(ctx, args, auxData, auxMetaSeq, rs)
			if err != nil {
				return nil, nil, err
			}
			loadSnapElapsed := time.Since(loadSnapBeg)
			fmt.Fprintf(os.Stderr, "[%d] load snapshot took %v\n", args.ectx.SubstreamNum(), loadSnapElapsed)
		}
		lastMark = auxMetaSeq
		restoreSsBeg := time.Now()
		err = restoreStateStore(ctx, args, offsetMap)
		if err != nil {
			return nil, nil, err
		}
		restoreStateStoreElapsed := time.Since(restoreSsBeg)
		setOffsetOnStream(offsetMap, args)
		restoreElapsed := time.Since(restoreBeg)
		fmt.Fprintf(os.Stderr, "restore state store took %v, restore elapsed: %v\n",
			restoreStateStoreElapsed, restoreElapsed)
	}
	fmt.Fprintf(os.Stderr, "[%d] %s down restore, epoch: %d ts: %d\n",
		args.ectx.SubstreamNum(), args.ectx.FuncName(), args.ectx.CurEpoch(), time.Now().UnixMilli())
	setLastMarkSeq(lastMark, args)
	trackParFunc := exactly_once_intr.TrackProdSubStreamFunc(
		func(ctx context.Context,
			topicName string, substreamId uint8,
		) error {
			return em.AddTopicSubstream(ctx, topicName, substreamId)
		})
	recordFinish := func(ctx context.Context, funcName string, instanceID uint8) error {
		return cmm.RecordPrevInstanceFinish(ctx, funcName, instanceID, args.ectx.CurEpoch())
	}
	updateFuncs(args, trackParFunc, recordFinish, func(ctx context.Context) error { return nil })
	return em, cmm, nil
}

func setLastMarkSeq(logOff uint64, args *StreamTaskArgs) {
	for _, p := range args.ectx.Producers() {
		p.SetLastMarkerSeq(logOff)
	}
	for _, kvs := range args.kvChangelogs {
		kvs.SetLastMarkerSeq(logOff)
	}
	for _, ws := range args.windowStoreChangelogs {
		ws.SetLastMarkerSeq(logOff)
	}
}

func processInEpoch(
	ctx context.Context,
	t *StreamTask,
	em *epoch_manager.EpochManager,
	cmm *control_channel.ControlChannelManager,
	args *StreamTaskArgs,
) *common.FnOutput {
	trackStreamAndConfigureExactlyOnce(args, em,
		func(name string, stream *sharedlog_stream.ShardedSharedLogStream) {})

	// execIntrMs := stats.NewPrintLogStatsCollector[int64]("execIntrMs")
	// thisAndLastCmtMs := stats.NewPrintLogStatsCollector[int64]("thisAndLastCmtMs")
	// markPartUs := stats.NewPrintLogStatsCollector[int64]("markPartUs")
	snapshotTime := make([]int64, 0, 8)
	hasProcessData := false
	init := false
	paused := false
	// latencies := stats.NewInt64Collector("latPerIter", stats.DEFAULT_COLLECT_DURATION)
	markTimer := time.Now()
	snapshotTimer := time.Now()
	var once sync.Once
	warmupCheck := stats.NewWarmupChecker(args.warmup)
	fmt.Fprintf(os.Stderr, "commit every(ms): %v, waitEndMark: %v, fixed output parNum: %d, snapshot every(s): %v, sink buf max: %v\n",
		args.commitEvery, args.waitEndMark, args.fixedOutParNum, args.snapshotEvery, args.bufMaxSize)
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
	var timeoutPrintOnce sync.Once
	for {
		// procStart := time.Now()
		once.Do(func() {
			warmupCheck.StartWarmup()
		})
		warmupCheck.Check()
		timeSinceLastMark := time.Since(markTimer)
		shouldMarkByTime := args.commitEvery != 0 && timeSinceLastMark >= args.commitEvery
		if shouldMarkByTime && hasProcessData {
			// execIntrMs.AddSample(timeSinceLastMark.Milliseconds())
			markBegin := time.Now()
			if t.pauseFunc != nil {
				if ret := t.pauseFunc(); ret != nil {
					return ret
				}
				paused = true
			}

			flushAllStart := stats.TimerBegin()
			f, err := flushStreams(ctx, args)
			if err != nil {
				return common.GenErrFnOutput(fmt.Errorf("flushStreams: %v", err))
			}
			flushTime := stats.Elapsed(flushAllStart).Microseconds()

			// mPartBeg := time.Now()
			logOff, shouldExit, err := markEpoch(ctx, em, t, args)
			if err != nil {
				return common.GenErrFnOutput(fmt.Errorf("markEpoch: %v", err))
			}
			setLastMarkSeq(logOff, args)
			if env_config.CREATE_SNAPSHOT && args.snapshotEvery != 0 && time.Since(snapshotTimer) > args.snapshotEvery {
				snStart := time.Now()
				createSnapshot(args, []commtypes.TpLogOff{{LogOff: logOff}})
				elapsed := time.Since(snStart)
				snapshotTime = append(snapshotTime, elapsed.Microseconds())
				snapshotTimer = time.Now()
			}
			markElapsed := time.Since(markBegin)
			// mPartElapsed := time.Since(mPartBeg)
			epochMarkTime = append(epochMarkTime, markElapsed.Microseconds())
			t.flushStageTime.AddSample(flushTime)
			// markPartUs.AddSample(mPartElapsed.Microseconds())
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
				// execIntrMs.PrintRemainingStats()
				// thisAndLastCmtMs.PrintRemainingStats()
				updateReturnMetric(ret, &warmupCheck,
					false, t.GetEndDuration(), args.ectx.SubstreamNum())
				return ret
			}
			hasProcessData = false
			// btwThisLastCmt := time.Since(markTimer)
			// thisAndLastCmtMs.AddSample(btwThisLastCmt.Milliseconds())
			markTimer = time.Now()
		}
		// Exit routine
		cur_elapsed := warmupCheck.ElapsedSinceInitial()
		timeout := args.duration != 0 && cur_elapsed >= args.duration
		lastTimeoutCheck := time.Now()
		if timeout {
			timeoutPrintOnce.Do(func() {
				fmt.Fprintf(os.Stderr, "timeout after %v\n", cur_elapsed)
				for _, src := range args.ectx.Consumers() {
					fmt.Fprintf(os.Stderr, "%s msgCnt %d, ctrlCnt %d, epochCnt %d, logEntry %d\n",
						src.Name(), src.GetCount(), src.NumCtrlMsg(), src.NumEpoch(), src.NumLogEntry())
				}
			})
			if time.Since(lastTimeoutCheck) > 20*time.Second {
				fmt.Fprintf(os.Stderr, "timeout after %v\n", cur_elapsed)
				for _, src := range args.ectx.Consumers() {
					fmt.Fprintf(os.Stderr, "%s msgCnt %d, ctrlCnt %d, epochCnt %d, logEntry %d\n",
						src.Name(), src.GetCount(), src.NumCtrlMsg(), src.NumEpoch(), src.NumLogEntry())
				}
			}
		}
		exitDueToFailTest := testForFail && cur_elapsed >= failAfter
		if (!args.waitEndMark && timeout) || exitDueToFailTest {
			r := finalMark(ctx, t, args, em, cmm, snapshotTime, epochMarkTime, exitDueToFailTest)
			if r != nil {
				return r
			}
			ret := &common.FnOutput{Success: true}
			if exitDueToFailTest {
				ret = &common.FnOutput{Success: true, Message: common_errors.ErrReturnDueToTest.Error()}
			}
			// markPartUs.PrintRemainingStats()
			// execIntrMs.PrintRemainingStats()
			// thisAndLastCmtMs.PrintRemainingStats()
			updateReturnMetric(ret, &warmupCheck,
				false, t.GetEndDuration(), args.ectx.SubstreamNum())
			return ret
		}

		// init
		if !hasProcessData && (!init || paused) {
			err := initAfterMarkOrCommit(ctx, t, args, em, &init, &paused)
			if err != nil {
				return common.GenErrFnOutput(fmt.Errorf("initAfterMarkOrCommit: %v", err))
			}
		}
		app_ret, ctrlRawMsgArr := t.appProcessFunc(ctx, t, args.ectx)
		if app_ret != nil {
			if app_ret.Success {
				debug.Assert(ctrlRawMsgArr == nil, "when timeout, ctrlMsg should not be returned")
				// consume timeout but not sure whether there's more data is coming; continue to process
				continue
			}
			return app_ret
		}
		if !hasProcessData {
			hasProcessData = true
		}
		if ctrlRawMsgArr != nil {
			ctrlRawMsg := ctrlRawMsgArr[0]
			fmt.Fprintf(os.Stderr, "exit due to ctrlMsg\n")
			r := finalMark(ctx, t, args, em, cmm, snapshotTime, epochMarkTime, false)
			if r != nil {
				return r
			}
			// markPartUs.PrintRemainingStats()
			// execIntrMs.PrintRemainingStats()
			// thisAndLastCmtMs.PrintRemainingStats()
			return handleCtrlMsg(ctx, ctrlRawMsg, t, args, &warmupCheck)
		}
	}
}

func finalMark(dctx context.Context, t *StreamTask, args *StreamTaskArgs,
	em *epoch_manager.EpochManager, cmm *control_channel.ControlChannelManager,
	snapshotTime []int64, epochMarkTime []int64, exitDueToFailTest bool,
	// markPartUs *stats.PrintLogStatsCollector[int64],
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
		return common.GenErrFnOutput(fmt.Errorf("flushStreams failed: %v", err))
	}
	flushTime := stats.Elapsed(flushAllStart).Microseconds()
	kms, err := getKeyMapping(dctx, args)
	if err != nil {
		return common.GenErrFnOutput(fmt.Errorf("getKeyMapping failed: %v", err))
	}
	// mPartBeg := time.Now()
	logOff, _, err := markEpoch(dctx, em, t, args)
	if err != nil {
		return common.GenErrFnOutput(fmt.Errorf("markEpoch failed: %v", err))
	}
	fmt.Fprintf(os.Stderr, "[%d] final mark seqNum: %#x\n", args.ectx.SubstreamNum(), logOff)
	if env_config.CREATE_SNAPSHOT && args.snapshotEvery != 0 && !exitDueToFailTest {
		snStart := time.Now()
		createSnapshot(args, []commtypes.TpLogOff{{LogOff: logOff}})
		for _, kv := range args.kvChangelogs {
			err = kv.WaitForAllSnapshot()
			if err != nil {
				return common.GenErrFnOutput(fmt.Errorf("KV WaitForAllSnapshot failed: %v", err))
			}
		}
		for _, wsc := range args.windowStoreChangelogs {
			err = wsc.WaitForAllSnapshot()
			if err != nil {
				return common.GenErrFnOutput(fmt.Errorf("Win WaitForAllSnapshot failed: %v", err))
			}
		}
		elapsed := time.Since(snStart)
		snapshotTime = append(snapshotTime, elapsed.Microseconds())
		fmt.Fprintf(os.Stderr, "final snapshot time: %v\n", snapshotTime)
	}
	err = cmm.OutputKeyMapping(dctx, kms)
	if err != nil {
		return common.GenErrFnOutput(fmt.Errorf("OutputKeyMapping failed: %v", err))
	}
	fmt.Fprintf(os.Stderr, "[%d] %s final mark, epoch %d, ts %d\n",
		args.ectx.SubstreamNum(), args.ectx.FuncName(), cmm.CurrentEpoch(), time.Now().UnixMilli())
	markElapsed := time.Since(markBegin)
	// mPartElapsed := time.Since(mPartBeg)
	epochMarkTime = append(epochMarkTime, markElapsed.Microseconds())
	// markPartUs.AddSample(mPartElapsed.Microseconds())
	t.flushStageTime.AddSample(flushTime)
	if f > 0 {
		t.flushAtLeastOne.AddSample(flushTime)
	}
	fmt.Fprintf(os.Stderr, "[%d] {epoch mark time: %v}\n", args.ectx.SubstreamNum(), epochMarkTime)
	fmt.Fprintf(os.Stderr, "[%d] epoch_mark_times: %d\n", args.ectx.SubstreamNum(), t.epochMarkTimes)
	t.flushStageTime.PrintRemainingStats()
	t.flushAtLeastOne.PrintRemainingStats()
	return nil
}

func getKeyMapping(ctx context.Context, args *StreamTaskArgs) (map[string][]txn_data.KeyMaping, error) {
	kms := make(map[string][]txn_data.KeyMaping)
	for _, kv := range args.kvChangelogs {
		err := kv.BuildKeyMeta(ctx, kms)
		if err != nil {
			return nil, err
		}
	}
	for _, wsc := range args.windowStoreChangelogs {
		err := wsc.BuildKeyMeta(kms)
		if err != nil {
			return nil, err
		}
	}
	return kms, nil
}

func markEpoch(ctx context.Context, em *epoch_manager.EpochManager,
	t *StreamTask, args *StreamTaskArgs,
) (logOff uint64, shouldExit bool, err error) {
	rawMsgs, err := em.SyncToRecent(ctx)
	if err != nil {
		return 0, false, fmt.Errorf("SyncToRecent: %v", err)
	}
	for _, rawMsg := range rawMsgs {
		if rawMsg.Mark == commtypes.FENCE {
			if (rawMsg.ProdId.TaskId == em.GetCurrentTaskId() && rawMsg.ProdId.TaskEpoch > em.GetCurrentEpoch()) ||
				rawMsg.ProdId.TaskId != em.GetCurrentTaskId() {
				return 0, true, nil
			}
		}
	}
	// prepareStart := stats.TimerBegin()
	epochMarker, err := epoch_manager.GenEpochMarker(ctx, em, args.ectx.Consumers(), args.ectx.Producers(),
		args.kvChangelogs, args.windowStoreChangelogs, args.ectx.SubstreamNum())
	if err != nil {
		return 0, false, fmt.Errorf("GenEpochMarker: %v", err)
	}
	epochMarkerTags, epochMarkerTopics := em.GenTagsAndTopicsForEpochMarker()
	// prepareTime := stats.Elapsed(prepareStart).Microseconds()

	// mStart := stats.TimerBegin()
	logOff, err = em.MarkEpoch(ctx, epochMarker, epochMarkerTags, epochMarkerTopics)
	if err != nil {
		return 0, false, fmt.Errorf("MarkEpoch: %v", err)
	}
	epoch_manager.CleanupState(em, args.ectx.Producers(), args.kvChangelogs, args.windowStoreChangelogs)
	// mElapsed := stats.Elapsed(mStart).Microseconds()
	// t.markEpochTime.AddSample(mElapsed)
	// t.markEpochPrepare.AddSample(prepareTime)
	return logOff, false, nil
}
