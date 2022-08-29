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
	"sharedlog-stream/pkg/exactly_once_intr"
	"sharedlog-stream/pkg/optional"
	"sharedlog-stream/pkg/processor"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/stats"
	"sharedlog-stream/pkg/store_restore"
	"sharedlog-stream/pkg/transaction"
	"time"
)

type ProcessFunc func(ctx context.Context, task *StreamTask,
	args processor.ExecutionContext) (*common.FnOutput, optional.Option[commtypes.RawMsgAndSeq])

type StreamTask struct {
	appProcessFunc ProcessFunc

	// in case the task consumes multiple streams, the task consumes from the same substream number
	// and the substreams must have the same number of substreams.

	pauseFunc     func() *common.FnOutput
	resumeFunc    func(task *StreamTask)
	initFunc      func(task *StreamTask)
	HandleErrFunc func() error

	flushForALO stats.StatsCollector[int64]
	// 2pc stat
	commitTrTime stats.StatsCollector[int64]
	beginTrTime  stats.StatsCollector[int64]

	// epoch stat
	markEpochTime    stats.StatsCollector[int64]
	markEpochPrepare stats.StatsCollector[int64]

	flushAllTime stats.StatsCollector[int64]
	isFinalStage bool
	endDuration  time.Duration
}

func (t *StreamTask) SetEndDuration(startTimeMs int64) {
	t.endDuration = time.Since(time.UnixMilli(startTimeMs))
}

func (t *StreamTask) GetEndDuration() time.Duration {
	return t.endDuration
}

func ExecuteApp(ctx context.Context,
	t *StreamTask,
	streamTaskArgs *StreamTaskArgs,
) *common.FnOutput {
	var ret *common.FnOutput
	if streamTaskArgs.guarantee == exactly_once_intr.TWO_PHASE_COMMIT {
		tm, cmm, err := setupManagersFor2pc(ctx, t, streamTaskArgs)
		if err != nil {
			return &common.FnOutput{Success: false, Message: err.Error()}
		}
		debug.Fprint(os.Stderr, "begin transaction processing\n")
		ret = processWithTransaction(ctx, t, tm, cmm, streamTaskArgs)
	} else if streamTaskArgs.guarantee == exactly_once_intr.EPOCH_MARK {
		em, cmm, err := SetupManagersForEpoch(ctx, streamTaskArgs)
		if err != nil {
			return &common.FnOutput{Success: false, Message: err.Error()}
		}
		debug.Fprint(os.Stderr, "begin epoch processing\n")
		ret = processInEpoch(ctx, t, em, cmm, streamTaskArgs)
		fmt.Fprintf(os.Stderr, "epoch ret: %v\n", ret)
	} else {
		ret = process(ctx, t, streamTaskArgs)
	}
	if ret != nil && ret.Success {
		for _, src := range streamTaskArgs.ectx.Consumers() {
			ret.Counts[src.Name()] = src.GetCount()
			ret.Counts[src.Name()+"_ctrl"] = uint64(src.NumCtrlMsg())
			ret.Counts[src.Name()+"_data"] = src.GetCount() - uint64(src.NumCtrlMsg())
			fmt.Fprintf(os.Stderr, "%s msgCnt %d, ctrlCnt %d\n",
				src.Name(), src.GetCount(), src.NumCtrlMsg())
		}
		for _, sink := range streamTaskArgs.ectx.Producers() {
			sink.PrintRemainingStats()
			ret.Counts[sink.Name()] = sink.GetCount()
			ret.Counts[sink.Name()+"_ctrl"] = uint64(sink.NumCtrlMsg())
			ret.Counts[sink.Name()+"_data"] = sink.GetCount() - uint64(sink.NumCtrlMsg())
			fmt.Fprintf(os.Stderr, "%s msgCnt %d, ctrlCnt %d\n",
				sink.Name(), sink.GetCount(), sink.NumCtrlMsg())
			// if sink.IsFinalOutput() {
			// ret.Latencies["eventTimeLatency_"+sink.Name()] = sink.GetEventTimeLatency()
			// }
		}
	}
	return ret
}

func handleCtrlMsg(ctx context.Context, ctrlRawMsg commtypes.RawMsgAndSeq,
	t *StreamTask, args *StreamTaskArgs, warmupCheck *stats.Warmup,
) *common.FnOutput {
	if ctrlRawMsg.Mark == commtypes.SCALE_FENCE {
		ret := HandleScaleEpochAndBytes(ctx, ctrlRawMsg, args.ectx)
		if ret.Success {
			updateReturnMetric(ret, warmupCheck,
				args.waitEndMark, t.GetEndDuration(), args.ectx.SubstreamNum())
		}
		return ret
	} else if ctrlRawMsg.Mark == commtypes.STREAM_END {
		epochMarkerSerde, err := commtypes.GetEpochMarkerSerdeG(args.serdeFormat)
		if err != nil {
			return common.GenErrFnOutput(err)
		}
		epochMarker := commtypes.EpochMarker{
			StartTime: ctrlRawMsg.StartTime,
			Mark:      commtypes.STREAM_END,
			ProdIndex: args.ectx.SubstreamNum(),
		}
		encoded, err := epochMarkerSerde.Encode(epochMarker)
		if err != nil {
			return common.GenErrFnOutput(err)
		}
		ctrlRawMsg.Payload = encoded
		for _, sink := range args.ectx.Producers() {
			if args.fixedOutParNum >= 0 {
				// debug.Fprintf(os.Stderr, "produce stream end mark to %s %d\n",
				// 	sink.Stream().TopicName(), ectx.SubstreamNum())
				_, err := sink.ProduceCtrlMsg(ctx, ctrlRawMsg, []uint8{args.ectx.SubstreamNum()})
				if err != nil {
					return &common.FnOutput{Success: false, Message: err.Error()}
				}
			} else {
				parNums := make([]uint8, 0, sink.Stream().NumPartition())
				for par := uint8(0); par < sink.Stream().NumPartition(); par++ {
					parNums = append(parNums, par)
				}
				_, err := sink.ProduceCtrlMsg(ctx, ctrlRawMsg, parNums)
				if err != nil {
					return &common.FnOutput{Success: false, Message: err.Error()}
				}
			}
		}
		t.SetEndDuration(ctrlRawMsg.StartTime)
		ret := &common.FnOutput{Success: true}
		updateReturnMetric(ret, warmupCheck,
			args.waitEndMark, t.GetEndDuration(), args.ectx.SubstreamNum())
		return ret
	} else {
		return &common.FnOutput{Success: false, Message: "unexpected ctrl msg"}
	}
}

func flushStreams(ctx context.Context, t *StreamTask,
	args *StreamTaskArgs,
) error {
	pStart := stats.TimerBegin()
	for _, kvchangelog := range args.kvChangelogs {
		if err := kvchangelog.Flush(ctx); err != nil {
			return err
		}
	}
	for _, wschangelog := range args.windowStoreChangelogs {
		if err := wschangelog.Flush(ctx); err != nil {
			return err
		}
	}
	for _, sink := range args.ectx.Producers() {
		if err := sink.Flush(ctx); err != nil {
			return err
		}
	}
	elapsed := stats.Elapsed(pStart)
	t.flushForALO.AddSample(elapsed.Microseconds())
	return nil
}

func updateReturnMetric(ret *common.FnOutput, warmupChecker *stats.Warmup,
	waitForEndMark bool, endDuration time.Duration, instanceID uint8,
) {
	ret.Latencies = make(map[string][]int)
	ret.Counts = make(map[string]uint64)
	if waitForEndMark {
		ret.Duration = endDuration.Seconds()
	} else {
		ret.Duration = warmupChecker.ElapsedAfterWarmup().Seconds()
	}
	fmt.Fprintf(os.Stderr, "duration: %f s\n", ret.Duration)
}

// for key value table, it's possible that the changelog is the source stream of the task
// and restore should make sure that it restores to the previous offset and don't read over
func restoreKVStore(ctx context.Context,
	args *StreamTaskArgs, offsetMap map[string]uint64,
) error {
	for _, kvchangelog := range args.kvChangelogs {
		topic := kvchangelog.ChangelogTopicName()
		offset := uint64(0)
		ok := false
		if kvchangelog.ChangelogIsSrc() {
			offset, ok = offsetMap[topic]
			if !ok {
				continue
			}
		}
		err := store_restore.RestoreChangelogKVStateStore(ctx, kvchangelog,
			offset, args.ectx.SubstreamNum())
		if err != nil {
			return fmt.Errorf("RestoreKVStateStore failed: %v", err)
		}
	}
	return nil
}

func restoreChangelogBackedWindowStore(ctx context.Context,
	args *StreamTaskArgs,
) error {
	for _, wschangelog := range args.windowStoreChangelogs {
		err := store_restore.RestoreChangelogWindowStateStore(ctx, wschangelog, args.ectx.SubstreamNum())
		if err != nil {
			return fmt.Errorf("RestoreWindowStateStore failed: %v", err)
		}
	}
	return nil
}

func restoreStateStore(ctx context.Context, args *StreamTaskArgs, offsetMap map[string]uint64) error {
	if args.kvChangelogs != nil {
		err := restoreKVStore(ctx, args, offsetMap)
		if err != nil {
			return err
		}
	}
	if args.windowStoreChangelogs != nil {
		err := restoreChangelogBackedWindowStore(ctx, args)
		if err != nil {
			return err
		}
	}
	return nil
}

func configChangelogExactlyOnce(rem exactly_once_intr.ReadOnlyExactlyOnceManager, args *StreamTaskArgs) error {
	for _, kvchangelog := range args.kvChangelogs {
		err := kvchangelog.ConfigureExactlyOnce(rem, args.guarantee)
		if err != nil {
			return err
		}
	}
	for _, wschangelog := range args.windowStoreChangelogs {
		err := wschangelog.ConfigureExactlyOnce(rem, args.guarantee)
		if err != nil {
			return err
		}
	}
	return nil
}

func updateFuncs(streamTaskArgs *StreamTaskArgs,
	trackParFunc exactly_once_intr.TrackProdSubStreamFunc,
	recordFinish exactly_once_intr.RecordPrevInstanceFinishFunc,
) {
	streamTaskArgs.ectx.SetTrackParFunc(trackParFunc)
	streamTaskArgs.ectx.SetRecordFinishFunc(recordFinish)
	for _, kvchangelog := range streamTaskArgs.kvChangelogs {
		kvchangelog.SetTrackParFunc(trackParFunc)
	}
	for _, wschangelog := range streamTaskArgs.windowStoreChangelogs {
		wschangelog.SetTrackParFunc(trackParFunc)
	}
}

func getOffsetMap(ctx context.Context,
	tm *transaction.TransactionManager, args *StreamTaskArgs,
) (map[string]uint64, error) {
	offsetMap := make(map[string]uint64)
	for _, src := range args.ectx.Consumers() {
		inputTopicName := src.TopicName()
		offset, err := transaction.CreateOffsetTopicAndGetOffset(ctx, tm, inputTopicName,
			uint8(src.Stream().NumPartition()), args.ectx.SubstreamNum())
		if err != nil {
			return nil, fmt.Errorf("createOffsetTopicAndGetOffset failed: %v", err)
		}
		offsetMap[inputTopicName] = offset
		debug.Fprintf(os.Stderr, "tp %s offset %d\n", inputTopicName, offset)
	}
	return offsetMap, nil
}

func setOffsetOnStream(offsetMap map[string]uint64,
	args *StreamTaskArgs,
) {
	if offsetMap != nil {
		for _, src := range args.ectx.Consumers() {
			inputTopicName := src.TopicName()
			offset := offsetMap[inputTopicName]
			resetTo := offset + 1
			if offset == 0 {
				resetTo = offset
			}
			debug.Fprintf(os.Stderr, "offset restores to %x\n", resetTo)
			src.SetCursor(resetTo, args.ectx.SubstreamNum())
		}
	}
}

func trackStreamAndConfigureExactlyOnce(args *StreamTaskArgs,
	rem exactly_once_intr.ReadOnlyExactlyOnceManager,
	trackStream func(name string, stream *sharedlog_stream.ShardedSharedLogStream),
) error {
	debug.Assert(len(args.ectx.Consumers()) >= 1, "Srcs should be filled")
	debug.Assert(args.env != nil, "env should be filled")
	debug.Assert(args.ectx != nil, "program args should be filled")
	for _, src := range args.ectx.Consumers() {
		if !src.IsInitialSource() {
			err := src.ConfigExactlyOnce(args.guarantee)
			if err != nil {
				return err
			}
		}
		trackStream(src.TopicName(),
			src.Stream().(*sharedlog_stream.ShardedSharedLogStream))
	}
	for _, sink := range args.ectx.Producers() {
		sink.ConfigExactlyOnce(rem, args.guarantee)
		trackStream(sink.TopicName(), sink.Stream().(*sharedlog_stream.ShardedSharedLogStream))
	}

	for _, kvchangelog := range args.kvChangelogs {
		trackStream(kvchangelog.ChangelogTopicName(), kvchangelog.Stream().(*sharedlog_stream.ShardedSharedLogStream))
	}
	for _, winchangelog := range args.windowStoreChangelogs {
		trackStream(winchangelog.ChangelogTopicName(), winchangelog.Stream().(*sharedlog_stream.ShardedSharedLogStream))
	}
	return nil
}

func checkMonitorReturns(
	dctx context.Context,
	dcancel context.CancelFunc,
	args *StreamTaskArgs,
	cmm *control_channel.ControlChannelManager,
	lm exactly_once_intr.ExactlyOnceManagerLogMonitor,
	// run *bool,
) *common.FnOutput {
	select {
	case <-dctx.Done():
		return &common.FnOutput{Success: true, Message: "exit due to ctx cancel"}
	// case out := <-cmm.OutputChan():
	// 	if out.Valid() {
	// 		m := out.Value()
	// 		if m.FinishedPrevTask == args.ectx.FuncName() && m.Epoch+1 == args.ectx.CurEpoch() {
	// 			debug.Fprintf(os.Stderr, "finished prev task %s, funcName %s, meta epoch %d, input epoch %d\n",
	// 				m.FinishedPrevTask, args.ectx.FuncName(), m.Epoch, args.ectx.CurEpoch())
	// 			*run = true
	// 		}
	// 	} else {
	// 		cerr := out.Err()
	// 		debug.Fprintf(os.Stderr, "got control error chan\n")
	// 		lm.SendQuit()
	// 		cmm.SendQuit()
	// 		if cerr != nil {
	// 			debug.Fprintf(os.Stderr, "[ERROR] control channel manager: %v", cerr)
	// 			dcancel()
	// 			return &common.FnOutput{
	// 				Success: false,
	// 				Message: fmt.Sprintf("control channel manager failed: %v", cerr),
	// 			}
	// 		}
	// 	}
	case merr := <-lm.ErrChan():
		debug.Fprintf(os.Stderr, "got monitor error chan\n")
		lm.SendQuit()
		// cmm.SendQuit()
		if merr != nil {
			debug.Fprintf(os.Stderr, "[ERROR] control channel manager: %v", merr)
			dcancel()
			return &common.FnOutput{Success: false, Message: fmt.Sprintf("monitor failed: %v", merr)}
		}
	default:
	}
	return nil
}

func initAfterMarkOrCommit(ctx context.Context, t *StreamTask, args *StreamTaskArgs,
	tracker exactly_once_intr.TopicSubstreamTracker, init *bool, paused *bool,
) error {
	if args.fixedOutParNum != -1 {
		sinks := args.ectx.Producers()
		debug.Assert(len(sinks) == 1, "fixed out param is only usable when there's only one output stream")
		// debug.Fprintf(os.Stderr, "%s tracking substream %d\n", sinks[0].TopicName(), args.fixedOutParNum)
		if err := tracker.AddTopicSubstream(ctx, sinks[0].TopicName(), uint8(args.fixedOutParNum)); err != nil {
			debug.Fprintf(os.Stderr, "[ERROR] track topic partition failed: %v\n", err)
			return fmt.Errorf("track topic partition failed: %v\n", err)
		}
	}
	if *paused && t.resumeFunc != nil {
		t.resumeFunc(t)
		*paused = false
	}
	if !*init {
		args.ectx.StartWarmup()
		if t.initFunc != nil {
			t.initFunc(t)
		}
		*init = true
	}
	return nil
}

func HandleScaleEpochAndBytes(ctx context.Context, msg commtypes.RawMsgAndSeq,
	args processor.ExecutionContext,
) *common.FnOutput {
	for _, sink := range args.Producers() {
		if sink.Stream().NumPartition() > args.SubstreamNum() {
			_, err := sink.ProduceCtrlMsg(ctx, msg, []uint8{args.SubstreamNum()})
			if err != nil {
				return &common.FnOutput{Success: false, Message: err.Error()}
			}
		}
	}
	err := args.RecordFinishFunc()(ctx, args.FuncName(), args.SubstreamNum())
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	return &common.FnOutput{
		Success: true,
		Message: fmt.Sprintf("%s-%d epoch %d exit", args.FuncName(), args.SubstreamNum(), args.CurEpoch()),
		Err:     common_errors.ErrShouldExitForScale,
	}
}
