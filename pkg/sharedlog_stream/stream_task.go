package sharedlog_stream

import (
	"context"
	"fmt"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/errors"
	"sharedlog-stream/pkg/hash"
	"sharedlog-stream/pkg/stream/processor"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/stream/processor/store"
	"sync"
	"time"

	"cs.utexas.edu/zjia/faas/types"
)

type StreamTask struct {
	ProcessFunc func(ctx context.Context, args interface{}) (map[string]uint64, *common.FnOutput)
}

type BaseProcArgs struct {
	Sink         *processor.MeteredSink
	TrackParFunc func([]uint8) error
}

func DefaultTrackSubstreamFunc(ctx context.Context,
	key interface{},
	keySerde commtypes.Serde,
	topicName string,
	substreamId uint8,
) error {
	return nil
}

// finding the last commited marker and gets the marker's seq number
func createOffsetTopicAndGetOffset(ctx context.Context, tm *TransactionManager,
	topic string, numPartition uint8, parNum uint8,
) (uint64, error) {
	err := tm.CreateOffsetTopic(topic, numPartition)
	if err != nil {
		return 0, fmt.Errorf("create offset topic failed: %v", err)
	}
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

func SetupManagersAndProcessTransactional(ctx context.Context,
	env types.Environment,
	streamTaskArgs *StreamTaskArgsTransaction,
	updateProcArgs func(procArgs interface{}, trackParFunc TrackKeySubStreamFunc),
	task *StreamTask,
) *common.FnOutput {
	tm, err := SetupTransactionManager(ctx, streamTaskArgs)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("setup transaction manager failed: %v", err),
		}
	}
	cmm, err := NewControlChannelManager(env, streamTaskArgs.QueryInput.AppId,
		commtypes.SerdeFormat(streamTaskArgs.QueryInput.SerdeFormat))
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}
	err = cmm.RestoreMapping(ctx)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}
	offsetMap, err := getOffsetMap(ctx, tm, streamTaskArgs)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}
	err = restoreStateStore(ctx, tm, streamTaskArgs, offsetMap)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}
	setOffsetOnStream(offsetMap, streamTaskArgs)
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
	updateProcArgs(streamTaskArgs.ProcArgs, trackParFunc)
	cmm.TrackConsistentHash(streamTaskArgs.CHashMu, streamTaskArgs.CHash)
	ret := task.ProcessWithTransaction(ctx, tm, cmm, streamTaskArgs)
	return ret
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
	}
	return offsetMap, nil
}

func setOffsetOnStream(offsetMap map[string]uint64, args *StreamTaskArgsTransaction) {
	for _, inputTopicName := range args.QueryInput.InputTopicNames {
		offset := offsetMap[inputTopicName]
		args.Srcs[inputTopicName].SetCursor(offset+1, args.QueryInput.ParNum)
	}
}

func restoreChangelogBackedKVStore(ctx context.Context, tm *TransactionManager,
	args *StreamTaskArgsTransaction, offsetMap map[string]uint64,
) error {
	for _, kvchangelog := range args.KVChangelogs {
		topic := kvchangelog.Changelog.TopicName()
		// offset stream is input stream
		if offset, ok := offsetMap[topic]; ok {
			err := store.RestoreKVStateStore(ctx,
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
			err = store.RestoreKVStateStore(ctx, kvchangelog, args.MsgSerde, offset)
			if err != nil {
				return fmt.Errorf("RestoreKVStateStore2 failed: %v", err)
			}
		}
	}
	return nil
}

func restoreChangelogBackedWindowStore(ctx context.Context, tm *TransactionManager, args *StreamTaskArgsTransaction, offsetMap map[string]uint64) error {
	for _, wschangelog := range args.WindowStoreChangelogs {
		topic := wschangelog.Changelog.TopicName()
		// offset stream is input stream
		if offset, ok := offsetMap[topic]; ok {
			err := store.RestoreWindowStateStore(ctx, wschangelog,
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
			err = store.RestoreWindowStateStore(ctx, wschangelog,
				args.MsgSerde, offset)
			if err != nil {
				return fmt.Errorf("RestoreWindowStateStore2 failed: %v", err)
			}
		}
	}
	return nil
}

func restoreStateStore(ctx context.Context, tm *TransactionManager, args *StreamTaskArgsTransaction, offsetMap map[string]uint64) error {
	debug.Assert(args.MsgSerde != nil, "args's msg serde should not be nil")
	if args.KVChangelogs != nil {
		err := restoreChangelogBackedKVStore(ctx, tm, args, offsetMap)
		if err != nil {
			return err
		}
	}
	if args.WindowStoreChangelogs != nil {
		err := restoreChangelogBackedWindowStore(ctx, tm, args, offsetMap)
		if err != nil {
			return err
		}
	}
	return nil
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
	tm *TransactionManager, hasLiveTransaction *bool, trackConsumePar *bool,
	retc chan *common.FnOutput,
) {
	err := tm.AppendConsumedSeqNum(ctx, consumedSeqNumConfigs)
	if err != nil {
		retc <- &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("append offset failed: %v\n", err),
		}
	}
	err = tm.CommitTransaction(ctx)
	if err != nil {
		retc <- &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("commit failed: %v\n", err),
		}
	}
	*hasLiveTransaction = false
	*trackConsumePar = false
}

type StreamTaskArgs struct {
	ProcArgs interface{}
	Duration time.Duration
}

type StreamTaskArgsTransaction struct {
	ProcArgs              interface{}
	Env                   types.Environment
	MsgSerde              commtypes.MsgSerde
	Srcs                  map[string]processor.Source
	OutputStream          *ShardedSharedLogStream
	QueryInput            *common.QueryInput
	CHash                 *hash.ConsistentHash
	CHashMu               *sync.RWMutex
	TransactionalId       string
	KVChangelogs          []*store.KVStoreChangelog
	WindowStoreChangelogs []*store.WindowStoreChangelog
	FixedOutParNum        uint8
}

func (t *StreamTask) Process(ctx context.Context, args *StreamTaskArgs) *common.FnOutput {
	latencies := make([]int, 0, 128)
	startTime := time.Now()
	for {
		if args.Duration != 0 && time.Since(startTime) >= args.Duration {
			break
		}
		procStart := time.Now()
		_, ret := t.ProcessFunc(ctx, args.ProcArgs)
		if ret != nil {
			if ret.Success {
				elapsed := time.Since(procStart)
				latencies = append(latencies, int(elapsed.Microseconds()))
				ret.Latencies = map[string][]int{"e2e": latencies}
				ret.Duration = time.Since(startTime).Seconds()
				ret.Consumed = make(map[string]uint64)
			}
			return ret
		}
		elapsed := time.Since(procStart)
		latencies = append(latencies, int(elapsed.Microseconds()))
	}
	return &common.FnOutput{
		Success:   true,
		Duration:  time.Since(startTime).Seconds(),
		Latencies: map[string][]int{"e2e": latencies},
		Consumed:  make(map[string]uint64),
	}
}

func (t *StreamTask) ProcessWithTransaction(
	ctx context.Context,
	tm *TransactionManager,
	cmm *ControlChannelManager,
	args *StreamTaskArgsTransaction,
) *common.FnOutput {
	tm.RecordTopicStreams(args.QueryInput.OutputTopicName, args.OutputStream)

	monitorQuit := make(chan struct{})
	monitorErrc := make(chan error)
	controlErrc := make(chan error)
	controlQuit := make(chan struct{})

	cmm.TrackStream(args.OutputStream.TopicName(), args.OutputStream)
	cmm.TrackOutputTopic(args.OutputStream.TopicName())
	for topic, src := range args.Srcs {
		cmm.TrackStream(topic, src.Stream().(*ShardedSharedLogStream))
	}

	dctx, dcancel := context.WithCancel(ctx)
	go tm.MonitorTransactionLog(ctx, monitorQuit, monitorErrc, dcancel)
	go cmm.MonitorControlChannel(ctx, controlQuit, controlErrc, dcancel)

	retc := make(chan *common.FnOutput)
	go t.processWithTranLoop(dctx, tm, args, retc)
	for {
		select {
		case ret := <-retc:
			monitorQuit <- struct{}{}
			controlQuit <- struct{}{}
			return ret
		case merr := <-monitorErrc:
			monitorQuit <- struct{}{}
			controlQuit <- struct{}{}
			if merr != nil {
				return &common.FnOutput{Success: false, Message: fmt.Sprintf("monitor failed: %v", merr)}
			}
		case cerr := <-controlErrc:
			monitorQuit <- struct{}{}
			controlQuit <- struct{}{}
			if cerr != nil {
				return &common.FnOutput{
					Success: false,
					Message: fmt.Sprintf("control channel manager failed: %v", cerr),
				}
			}
		}
	}
}

func (t *StreamTask) processWithTranLoop(ctx context.Context,
	tm *TransactionManager, args *StreamTaskArgsTransaction,
	retc chan *common.FnOutput,
) {
	latencies := make([]int, 0, 128)
	hasLiveTransaction := false
	trackConsumePar := false
	var currentOffset map[string]uint64
	commitTimer := time.Now()
	commitEvery := time.Duration(args.QueryInput.CommitEveryMs) * time.Millisecond
	duration := time.Duration(args.QueryInput.Duration) * time.Second

	startTime := time.Now()
	idx := 0
L:
	for {
		select {
		case <-ctx.Done():
			break L
		default:
		}
		timeSinceTranStart := time.Since(commitTimer)
		timeout := duration != 0 && time.Since(startTime) >= duration
		if (commitEvery != 0 && timeSinceTranStart > commitEvery) || timeout || idx == 10 {
			/*
				if val, ok := args.QueryInput.TestParams["FailBeforeCommit"]; ok && val {
					fmt.Fprintf(os.Stderr, "about to fail before commit")
					retc <- &common.FnOutput{Success: false, Message: "fail before commit"}
					return
				}
			*/
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
			TrackOffsetAndCommit(ctx, consumedSeqNumConfigs, tm, &hasLiveTransaction, &trackConsumePar, retc)
			/*
				if val, ok := args.QueryInput.TestParams["FailAfterCommit"]; ok && val {
					fmt.Fprintf(os.Stderr, "about to fail after commit")
					retc <- &common.FnOutput{
						Success: false,
						Message: "fail after commit",
					}
					return
				}
			*/
		}
		if timeout {
			if err := tm.Close(); err != nil {
				retc <- &common.FnOutput{Success: false, Message: fmt.Sprintf("close transaction manager: %v\n", err)}
				return
			}
			break
		}
		if !hasLiveTransaction {
			if err := tm.BeginTransaction(ctx); err != nil {
				retc <- &common.FnOutput{Success: false, Message: fmt.Sprintf("transaction begin failed: %v\n", err)}
				return
			}
			/*
				if idx == 5 {
					if val, ok := args.QueryInput.TestParams["FailAfterBegin"]; ok && val {
						fmt.Fprintf(os.Stderr, "about to fail after begin")
						retc <- &common.FnOutput{Success: false, Message: "fail after begin"}
						return
					}
				}
			*/
			hasLiveTransaction = true
			commitTimer = time.Now()
			if args.FixedOutParNum != 0 {
				if err := tm.AddTopicPartition(ctx, args.QueryInput.OutputTopicName, []uint8{args.FixedOutParNum}); err != nil {
					retc <- &common.FnOutput{Success: false, Message: fmt.Sprintf("track topic partition failed: %v\n", err)}
					return
				}
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

		procStart := time.Now()
		off, ret := t.ProcessFunc(ctx, args.ProcArgs)
		if ret != nil {
			if ret.Success {
				if hasLiveTransaction {
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
					TrackOffsetAndCommit(ctx, consumedSeqNumConfigs, tm, &hasLiveTransaction, &trackConsumePar, retc)
				}
				elapsed := time.Since(procStart)
				latencies = append(latencies, int(elapsed.Microseconds()))
				ret.Latencies = map[string][]int{"e2e": latencies}
				ret.Consumed = make(map[string]uint64)
				ret.Duration = time.Since(startTime).Seconds()
			} else {
				if hasLiveTransaction {
					if err := tm.AbortTransaction(ctx); err != nil {
						retc <- &common.FnOutput{Success: false, Message: fmt.Sprintf("abort failed: %v\n", err)}
						return
					}
				}
			}
			retc <- ret
			return
		}
		/*
			if idx == 5 {
				if val, ok := args.QueryInput.TestParams["FailAfterProcess"]; ok && val {
					fmt.Fprintf(os.Stderr, "about to fail after process\n")
					retc <- &common.FnOutput{
						Success: false,
						Message: "fail after begin",
					}
					return
				}
			}
		*/
		currentOffset = off
		elapsed := time.Since(procStart)
		latencies = append(latencies, int(elapsed.Microseconds()))
		idx += 1
	}
	retc <- &common.FnOutput{
		Success:   true,
		Duration:  time.Since(startTime).Seconds(),
		Latencies: map[string][]int{"e2e": latencies},
		Consumed:  make(map[string]uint64),
	}
}
