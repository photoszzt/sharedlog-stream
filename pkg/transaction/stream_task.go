package transaction

import (
	"context"
	"fmt"
	"os"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/errors"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/stream/processor"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/stream/processor/store"
	"sharedlog-stream/pkg/txn_data"
	"time"

	"cs.utexas.edu/zjia/faas/types"
	"golang.org/x/xerrors"
)

type StreamTask struct {
	ProcessFunc   func(ctx context.Context, task *StreamTask, args interface{}) (map[string]uint64, *common.FnOutput)
	CurrentOffset map[string]uint64
	CloseFunc     func()
	PauseFunc     func()
	ResumeFunc    func()
	InitFunc      func(progArgs interface{})
	CommitEvery   time.Duration
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

func DefaultTrackSubstreamFunc(ctx context.Context,
	key interface{},
	keySerde commtypes.Serde,
	topicName string,
	substreamId uint8,
) error {
	return nil
}

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

type TrackKeySubStreamFunc func(ctx context.Context,
	key interface{},
	keySerde commtypes.Serde,
	topicName string,
	substreamId uint8,
) error

type RecordPrevInstanceFinishFunc func(ctx context.Context, appId string, instanceID uint8) error

func SetupManagersAndProcessTransactional(ctx context.Context,
	env types.Environment,
	streamTaskArgs *StreamTaskArgsTransaction,
	updateProcArgs func(procArgs interface{}, trackParFunc TrackKeySubStreamFunc,
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
	updateProcArgs func(procArgs interface{}, trackParFunc TrackKeySubStreamFunc,
		recordFinish RecordPrevInstanceFinishFunc),
	task *StreamTask,
) (*TransactionManager, *ControlChannelManager, error) {
	debug.Fprint(os.Stderr, "setup transaction and control manager\n")
	tm, err := SetupTransactionManager(ctx, streamTaskArgs)
	if err != nil {
		return nil, nil, err
	}
	cmm, err := NewControlChannelManager(env, streamTaskArgs.QueryInput.AppId,
		commtypes.SerdeFormat(streamTaskArgs.QueryInput.SerdeFormat), streamTaskArgs.QueryInput.ScaleEpoch)
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
	for _, inputTopicName := range args.QueryInput.InputTopicNames {
		offset, err := createOffsetTopicAndGetOffset(ctx, tm, inputTopicName,
			uint8(args.QueryInput.NumInPartition), args.QueryInput.ParNum)
		if err != nil {
			return nil, fmt.Errorf("createOffsetTopicAndGetOffset failed: %v", err)
		}
		offsetMap[inputTopicName] = offset
		debug.Fprintf(os.Stderr, "tp %s offset %d\n", inputTopicName, offset)
	}
	return offsetMap, nil
}

func setOffsetOnStream(offsetMap map[string]uint64, args *StreamTaskArgsTransaction) {
	for _, inputTopicName := range args.QueryInput.InputTopicNames {
		offset := offsetMap[inputTopicName]
		args.Srcs[inputTopicName].SetCursor(offset+1, args.QueryInput.ParNum)
	}
}

func restoreKVStore(ctx context.Context, tm *TransactionManager,
	t *StreamTask,
	args *StreamTaskArgsTransaction, offsetMap map[string]uint64,
) error {
	for _, kvchangelog := range args.KVChangelogs {
		if kvchangelog.KVStore.TableType() == store.IN_MEM {
			topic := kvchangelog.Changelog.TopicName()
			// offset stream is input stream
			if offset, ok := offsetMap[topic]; ok && offset != 0 {
				err := RestoreChangelogKVStateStore(ctx,
					kvchangelog,
					args.MsgSerde, offset)
				if err != nil {
					return fmt.Errorf("RestoreKVStateStore failed: %v", err)
				}
			} else {
				offset, err := createOffsetTopicAndGetOffset(ctx, tm, topic,
					kvchangelog.Changelog.NumPartition(), kvchangelog.ParNum)
				if err != nil {
					return fmt.Errorf("createOffsetTopicAndGetOffset kv failed: %v", err)
				}
				if offset != 0 {
					err = RestoreChangelogKVStateStore(ctx, kvchangelog, args.MsgSerde, offset)
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
			topic := wschangelog.Changelog.TopicName()
			// offset stream is input stream
			if offset, ok := offsetMap[topic]; ok && offset != 0 {
				err := RestoreChangelogWindowStateStore(ctx, wschangelog,
					args.MsgSerde, offset)
				if err != nil {
					return fmt.Errorf("RestoreWindowStateStore failed: %v", err)
				}
			} else {
				offset, err := createOffsetTopicAndGetOffset(ctx, tm, topic,
					wschangelog.Changelog.NumPartition(), wschangelog.ParNum)
				if err != nil {
					return fmt.Errorf("createOffsetTopicAndGetOffset win failed: %v", err)
				}
				if offset != 0 {
					err = RestoreChangelogWindowStateStore(ctx, wschangelog,
						args.MsgSerde, offset)
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
	debug.Assert(args.MsgSerde != nil, "args's msg serde should not be nil")
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
	if !found || tm.TransactionID == storeTranID {
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
	return kvchangelog.KVStore.CommitTransaction(ctx, tm.TransactionalId, tm.TransactionID)
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
	if !found || tm.TransactionID == storeTranID {
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
	return kvchangelog.WinStore.CommitTransaction(ctx, tm.TransactionalId, tm.TransactionID)
}

func SetupTransactionManager(
	ctx context.Context,
	args *StreamTaskArgsTransaction,
) (*TransactionManager, error) {
	tm, err := NewTransactionManager(ctx, args.Env, args.TransactionalId,
		commtypes.SerdeFormat(args.QueryInput.SerdeFormat))
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
	for _, inputTopicName := range args.QueryInput.InputTopicNames {
		offset := offsetMap[inputTopicName]
		args.Srcs[inputTopicName].SetCursor(offset+1, args.QueryInput.ParNum)
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
	ProcArgs       interface{}
	Env            types.Environment
	Srcs           map[string]processor.Source
	Duration       time.Duration
	WarmupTime     time.Duration
	SerdeFormat    commtypes.SerdeFormat
	ParNum         uint8
	NumInPartition uint8
}

type StreamTaskArgsTransaction struct {
	ProcArgs              processor.ProcArgs
	Env                   types.Environment
	MsgSerde              commtypes.MsgSerde
	Srcs                  map[string]processor.Source
	OutputStreams         []*sharedlog_stream.ShardedSharedLogStream
	QueryInput            *common.QueryInput
	TransactionalId       string
	KVChangelogs          []*KVStoreChangelog
	WindowStoreChangelogs []*WindowStoreChangelog
	FixedOutParNum        uint8
}

func (t *StreamTask) Process(ctx context.Context, args *StreamTaskArgs) *common.FnOutput {
	latencies := make([]int, 0, 128)
	cm, err := NewConsumeSeqManager(args.SerdeFormat)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	debug.Fprint(os.Stderr, "start restore\n")
	for inputTopicName, srcStream := range args.Srcs {
		err = cm.CreateOffsetTopic(args.Env, inputTopicName, args.NumInPartition, args.SerdeFormat)
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
		t.InitFunc(args.ProcArgs)
	}
	hasUncommitted := false
	var off map[string]uint64 = nil
	var ret *common.FnOutput = nil

	var afterWarmupStart time.Time
	afterWarmup := false
	commitTicker := time.NewTicker(t.CommitEvery)
	debug.Fprintf(os.Stderr, "warmup time: %v\n", args.WarmupTime)
	startTime := time.Now()
	for {
		select {
		case <-commitTicker.C:
			if off != nil {
				if t.PauseFunc != nil {
					t.PauseFunc()
				}
				err = commitOffset(ctx, cm, off, args.ParNum)
				if err != nil {
					panic(err)
				}
				if t.ResumeFunc != nil {
					t.ResumeFunc()
				}
			}
			hasUncommitted = false
		default:
		}
		if !afterWarmup && args.WarmupTime != 0 && time.Since(startTime) >= args.WarmupTime {
			afterWarmup = true
			afterWarmupStart = time.Now()
		}
		if args.Duration != 0 && time.Since(startTime) >= args.Duration {
			break
		}
		procStart := time.Now()
		off, ret = t.ProcessFunc(ctx, t, args.ProcArgs)
		if ret != nil {
			if ret.Success {
				// elapsed := time.Since(procStart)
				// latencies = append(latencies, int(elapsed.Microseconds()))
				if hasUncommitted {
					err = commitOffset(ctx, cm, off, args.ParNum)
					if err != nil {
						panic(err)
					}
				}
				if t.PauseFunc != nil {
					t.PauseFunc()
				}
				if t.CloseFunc != nil {
					t.CloseFunc()
				}
				ret.Latencies = map[string][]int{"e2e": latencies}
				if args.WarmupTime != 0 && afterWarmup {
					ret.Duration = time.Since(afterWarmupStart).Seconds()
				} else {
					ret.Duration = time.Since(startTime).Seconds()
				}
				ret.Consumed = make(map[string]uint64)
			}
			return ret
		}
		if !hasUncommitted {
			hasUncommitted = true
		}
		if args.WarmupTime == 0 || afterWarmup {
			elapsed := time.Since(procStart)
			latencies = append(latencies, int(elapsed.Microseconds()))
		}
	}
	if hasUncommitted {
		debug.Fprintf(os.Stderr, "commit left\n")
		err = commitOffset(ctx, cm, off, args.ParNum)
		if err != nil {
			panic(err)
		}
	}
	if t.PauseFunc != nil {
		t.PauseFunc()
	}
	if t.CloseFunc != nil {
		debug.Fprintf(os.Stderr, "closing\n")
		t.CloseFunc()
	}
	duration := time.Since(startTime).Seconds()
	if args.WarmupTime != 0 && afterWarmup {
		duration = time.Since(afterWarmupStart).Seconds()
	}
	return &common.FnOutput{
		Success:   true,
		Duration:  duration,
		Latencies: map[string][]int{"e2e": latencies},
		Consumed:  make(map[string]uint64),
	}
}

func commitOffset(ctx context.Context, cm *ConsumeSeqManager, off map[string]uint64, parNum uint8) error {
	consumedSeqNumConfigs := make([]ConsumedSeqNumConfig, 0)
	for topic, offset := range off {
		consumedSeqNumConfigs = append(consumedSeqNumConfigs, ConsumedSeqNumConfig{
			TopicToTrack:   topic,
			Partition:      parNum,
			ConsumedSeqNum: uint64(offset),
		})
	}
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
	go tm.MonitorTransactionLog(dctx, monitorQuit, monitorErrc, dcancel)
	go cmm.MonitorControlChannel(dctx, controlQuit, controlErrc, meta)

	run := false
	// retc := make(chan *common.FnOutput)
	// runChan := make(chan struct{})
	// go t.processWithTranLoop(dctx, tm, args, retc, runChan)

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
	init := false
	startTimeInit := false
	var afterWarmupStart time.Time
	afterWarmup := false
	warmupDuration := time.Duration(args.QueryInput.WarmupS) * time.Second
	var startTime time.Time
	for {
		select {
		case <-dctx.Done():
			return &common.FnOutput{Success: true, Message: "exit due to ctx cancel"}
		case m := <-meta:
			debug.Fprintf(os.Stderr, "finished prev task %s, funcName %s, meta epoch %d, input epoch %d\n",
				m.FinishedPrevTask, args.ProcArgs.FuncName(), m.Epoch, args.QueryInput.ScaleEpoch)
			if m.FinishedPrevTask == args.ProcArgs.FuncName() && m.Epoch+1 == args.QueryInput.ScaleEpoch {
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
			if !startTimeInit {
				startTime = time.Now()
				startTimeInit = true
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
				err := t.commitTransaction(ctx, currentOffset, tm, args, &hasLiveTransaction, &trackConsumePar)
				if err != nil {
					return &common.FnOutput{Success: false, Message: err.Error()}
				}
				debug.Assert(!hasLiveTransaction, "after commit. there should be no live transaction\n")
				numCommit += 1
				debug.Fprintf(os.Stderr, "transaction committed\n")
			}
			cur_elapsed = time.Since(startTime)
			timeout = duration != 0 && cur_elapsed >= duration
			if timeout || (args.QueryInput.ExitAfterNCommit != 0 && numCommit == int(args.QueryInput.ExitAfterNCommit)) {
				if err := tm.Close(); err != nil {
					debug.Fprintf(os.Stderr, "[ERROR] close transaction manager: %v\n", err)
					return &common.FnOutput{Success: false, Message: fmt.Sprintf("close transaction manager: %v\n", err)}
				}
				if hasLiveTransaction {
					err := t.commitTransaction(ctx, currentOffset, tm, args, &hasLiveTransaction, &trackConsumePar)
					if err != nil {
						return &common.FnOutput{Success: false, Message: err.Error()}
					}
				}
				if t.CloseFunc != nil {
					debug.Fprintf(os.Stderr, "waiting for goroutines to close 2\n")
					t.CloseFunc()
				}
				elapsed := time.Since(procStart)
				latencies = append(latencies, int(elapsed.Microseconds()))
				e2eTime := time.Since(startTime).Seconds()
				if warmupDuration != 0 && afterWarmup {
					e2eTime = time.Since(afterWarmupStart).Seconds()
				}
				debug.Fprintf(os.Stderr, "return final out to chan 2\n")
				return &common.FnOutput{
					Success:   true,
					Duration:  e2eTime,
					Latencies: map[string][]int{"e2e": latencies},
					Consumed:  make(map[string]uint64),
				}
			}
			if !hasLiveTransaction {
				if err := tm.BeginTransaction(ctx, args.KVChangelogs, args.WindowStoreChangelogs); err != nil {
					debug.Fprintf(os.Stderr, "[ERROR] transaction begin failed: %v", err)
					return &common.FnOutput{Success: false, Message: fmt.Sprintf("transaction begin failed: %v\n", err)}
				}
				hasLiveTransaction = true
				commitTimer = time.Now()
				if args.FixedOutParNum != 0 {
					debug.Assert(len(args.QueryInput.OutputTopicNames) == 1, "fixed out param is only usable when there's only one output stream")
					if err := tm.AddTopicPartition(ctx, args.QueryInput.OutputTopicNames[0], []uint8{args.FixedOutParNum}); err != nil {
						debug.Fprintf(os.Stderr, "[ERROR] track topic partition failed: %v\n", err)
						return &common.FnOutput{Success: false, Message: fmt.Sprintf("track topic partition failed: %v\n", err)}
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
						return &common.FnOutput{Success: false, Message: fmt.Sprintf("add offsets failed: %v\n", err)}
					}
				}
				trackConsumePar = true
			}

			off, ret := t.ProcessFunc(ctx, t, args.ProcArgs)
			if ret != nil {
				if ret.Success {
					if hasLiveTransaction {
						err := t.commitTransaction(ctx, currentOffset, tm, args, &hasLiveTransaction, &trackConsumePar)
						if err != nil {
							return &common.FnOutput{Success: false, Message: err.Error()}
						}
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
							return &common.FnOutput{Success: false, Message: fmt.Sprintf("abort failed: %v\n", err)}
						}
					}
				}
				return ret
			}
			currentOffset = off
			if warmupDuration == 0 || afterWarmup {
				elapsed := time.Since(procStart)
				latencies = append(latencies, int(elapsed.Microseconds()))
			}
			idx += 1
		}
	}
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
	currentOffset map[string]uint64,
	tm *TransactionManager,
	args *StreamTaskArgsTransaction,
	hasLiveTransaction *bool,
	trackConsumePar *bool,
) error {
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
		return fmt.Errorf("append offset failed: %v\n", err)
	}
	err = tm.CommitTransaction(ctx, args.KVChangelogs, args.WindowStoreChangelogs)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] commit failed: %v\n", err)
		return fmt.Errorf("commit failed: %v\n", err)
	}
	*hasLiveTransaction = false
	*trackConsumePar = false
	return nil
}

func CommonProcess(ctx context.Context, t *StreamTask, args processor.ProcArgsWithSrcSink,
	proc func(t *StreamTask, msg commtypes.MsgAndSeq) error,
) (map[string]uint64, *common.FnOutput) {
	select {
	case err := <-args.ErrChan():
		return t.CurrentOffset, &common.FnOutput{Success: false, Message: err.Error()}
	default:
	}
	gotMsgs, err := args.Source().Consume(ctx, args.ParNum())
	if err != nil {
		if xerrors.Is(err, errors.ErrStreamSourceTimeout) {
			return t.CurrentOffset, &common.FnOutput{Success: true, Message: err.Error()}
		}
		return t.CurrentOffset, &common.FnOutput{Success: false, Message: err.Error()}
	}
	for _, msg := range gotMsgs.Msgs {
		if msg.MsgArr == nil && msg.Msg.Value == nil {
			continue
		}
		if msg.IsControl {
			v := msg.Msg.Value.(sharedlog_stream.ScaleEpochAndBytes)
			err := args.PushToAllSinks(ctx, commtypes.Message{Key: commtypes.SCALE_FENCE_KEY,
				Value: v.Payload}, args.ParNum(), true)
			if err != nil {
				return t.CurrentOffset, &common.FnOutput{Success: false, Message: err.Error()}
			}
			if args.CurEpoch() < v.ScaleEpoch {
				err = args.RecordFinishFunc()(ctx, args.FuncName(), args.ParNum())
				if err != nil {
					return t.CurrentOffset, &common.FnOutput{Success: false, Message: err.Error()}
				}
				return t.CurrentOffset, &common.FnOutput{
					Success: true,
					Message: fmt.Sprintf("%s-%d epoch %d exit", args.FuncName(), args.ParNum(), args.CurEpoch()),
					Err:     errors.ErrShouldExitForScale,
				}
			}
			continue
		}
		err = proc(t, msg)
		if err != nil {
			return t.CurrentOffset, &common.FnOutput{Success: false, Message: err.Error()}
		}
	}
	return t.CurrentOffset, nil
}
