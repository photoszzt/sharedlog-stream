package stream_task

import (
	"context"
	"fmt"
	"os"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/pkg/control_channel"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/exactly_once_intr"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/stats"
	"sharedlog-stream/pkg/store_restore"
	"sharedlog-stream/pkg/transaction"
	"sync"
)

type ProcessFunc func(ctx context.Context, task *StreamTask, args interface{}) *common.FnOutput

type StreamTask struct {
	appProcessFunc ProcessFunc

	// in case the task consumes multiple streams, the task consumes from the same substream number
	// and the substreams must have the same number of substreams.
	OffMu                sync.Mutex
	CurrentConsumeOffset map[string]uint64

	pauseFunc               func(sargs *StreamTaskArgs) *common.FnOutput
	resumeFunc              func(task *StreamTask, sargs *StreamTaskArgs)
	initFunc                func(task *StreamTask)
	flushFuncForAtLeastOnce func(ctx context.Context, args *StreamTaskArgs) error
	HandleErrFunc           func() error
}

func (t *StreamTask) ExecuteApp(ctx context.Context,
	streamTaskArgs *StreamTaskArgs,
) *common.FnOutput {
	var ret *common.FnOutput
	if streamTaskArgs.guarantee == exactly_once_intr.TWO_PHASE_COMMIT {
		tm, cmm, err := t.setupManagersFor2pc(ctx, streamTaskArgs)
		if err != nil {
			return &common.FnOutput{Success: false, Message: err.Error()}
		}
		debug.Fprint(os.Stderr, "begin transaction processing\n")
		ret = t.processWithTransaction(ctx, tm, cmm, streamTaskArgs)
	} else if streamTaskArgs.guarantee == exactly_once_intr.EPOCH_MARK {
		em, cmm, err := t.SetupManagersForEpoch(ctx, streamTaskArgs)
		if err != nil {
			return &common.FnOutput{Success: false, Message: err.Error()}
		}
		debug.Fprint(os.Stderr, "begin epoch processing\n")
		ret = t.processInEpoch(ctx, em, cmm, streamTaskArgs)
		fmt.Fprintf(os.Stderr, "epoch ret: %v\n", ret)
	} else {
		ret = t.process(ctx, streamTaskArgs)
	}
	if ret != nil && ret.Success {
		for _, src := range streamTaskArgs.ectx.Consumers() {
			ret.Counts[src.Name()] = src.GetCount()
			// debug.Fprintf(os.Stderr, "src %s count %d\n", src.Name(), src.GetCount())
		}
		for _, sink := range streamTaskArgs.ectx.Producers() {
			ret.Counts[sink.Name()] = sink.GetCount()
			// debug.Fprintf(os.Stderr, "sink %s count %d\n", sink.Name(), sink.GetCount())
			if sink.IsFinalOutput() {
				ret.Latencies["eventTimeLatency_"+sink.Name()] = sink.GetEventTimeLatency()
			}
		}
	}
	return ret
}

func (t *StreamTask) flushStreams(ctx context.Context, args *StreamTaskArgs) error {
	for _, sink := range args.ectx.Producers() {
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

func updateReturnMetric(ret *common.FnOutput, warmupChecker *stats.Warmup) {
	ret.Latencies = make(map[string][]int)
	ret.Counts = make(map[string]uint64)
	ret.Duration = warmupChecker.ElapsedAfterWarmup().Seconds()
}

// for key value table, it's possible that the changelog is the source stream of the task
// and restore should make sure that it restores to the previous offset and don't read over
func restoreKVStore(ctx context.Context, args *StreamTaskArgs, offsetMap map[string]uint64,
) error {
	for _, kvchangelog := range args.kvChangelogs {
		topic := kvchangelog.ChangelogManager().TopicName()
		offset := uint64(0)
		ok := false
		if kvchangelog.ChangelogManager().ChangelogIsSrc() {
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

func restoreChangelogBackedWindowStore(ctx context.Context, args *StreamTaskArgs) error {
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
		err := kvchangelog.ChangelogManager().ConfigExactlyOnce(rem, args.guarantee, args.serdeFormat)
		if err != nil {
			return err
		}
	}
	for _, wschangelog := range args.windowStoreChangelogs {
		err := wschangelog.ChangelogManager().ConfigExactlyOnce(rem, args.guarantee, args.serdeFormat)
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

func getOffsetMap(ctx context.Context, tm *transaction.TransactionManager, args *StreamTaskArgs) (map[string]uint64, error) {
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

func setOffsetOnStream(offsetMap map[string]uint64, args *StreamTaskArgs) {
	for _, src := range args.ectx.Consumers() {
		inputTopicName := src.TopicName()
		offset := offsetMap[inputTopicName]
		src.SetCursor(offset+1, args.ectx.SubstreamNum())
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
			err := src.ConfigExactlyOnce(args.serdeFormat, args.guarantee)
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
		if kvchangelog.ChangelogManager() != nil {
			trackStream(kvchangelog.ChangelogManager().TopicName(), kvchangelog.ChangelogManager().Stream())
		}
	}
	for _, winchangelog := range args.windowStoreChangelogs {
		if winchangelog.ChangelogManager() != nil {
			trackStream(winchangelog.ChangelogManager().TopicName(), winchangelog.ChangelogManager().Stream())
		}
	}
	return nil
}

func checkMonitorReturns(
	dctx context.Context,
	dcancel context.CancelFunc,
	args *StreamTaskArgs,
	cmm *control_channel.ControlChannelManager,
	lm exactly_once_intr.ExactlyOnceManagerLogMonitor,
	run *bool,
) *common.FnOutput {
	select {
	case <-dctx.Done():
		return &common.FnOutput{Success: true, Message: "exit due to ctx cancel"}
	case out := <-cmm.OutputChan():
		if out.Valid() {
			m := out.Value()
			debug.Fprintf(os.Stderr, "finished prev task %s, funcName %s, meta epoch %d, input epoch %d\n",
				m.FinishedPrevTask, args.ectx.FuncName(), m.Epoch, args.ectx.CurEpoch())
			if m.FinishedPrevTask == args.ectx.FuncName() && m.Epoch+1 == args.ectx.CurEpoch() {
				*run = true
			}
		} else {
			cerr := out.Err()
			debug.Fprintf(os.Stderr, "got control error chan\n")
			lm.SendQuit()
			cmm.SendQuit()
			if cerr != nil {
				debug.Fprintf(os.Stderr, "[ERROR] control channel manager: %v", cerr)
				dcancel()
				return &common.FnOutput{
					Success: false,
					Message: fmt.Sprintf("control channel manager failed: %v", cerr),
				}
			}
		}
	case merr := <-lm.ErrChan():
		debug.Fprintf(os.Stderr, "got monitor error chan\n")
		lm.SendQuit()
		cmm.SendQuit()
		if merr != nil {
			debug.Fprintf(os.Stderr, "[ERROR] control channel manager: %v", merr)
			dcancel()
			return &common.FnOutput{Success: false, Message: fmt.Sprintf("monitor failed: %v", merr)}
		}
	default:
	}
	return nil
}

func (t *StreamTask) initAfterMarkOrCommit(ctx context.Context, args *StreamTaskArgs,
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
		t.resumeFunc(t, args)
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
