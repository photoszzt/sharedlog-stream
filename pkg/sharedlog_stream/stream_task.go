package sharedlog_stream

import (
	"context"
	"fmt"
	"os"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/pkg/errors"
	"sharedlog-stream/pkg/stream/processor"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/stream/processor/store"
	"time"

	"cs.utexas.edu/zjia/faas/types"
)

type StreamTask struct {
	ProcessFunc func(ctx context.Context, args interface{}) (map[string]uint64, *common.FnOutput)
}

func DefaultTrackParFunc(u []uint8) error {
	return nil
}

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

func SetupTransactionManager(
	ctx context.Context,
	args *StreamTaskArgsTransaction,
) (*TransactionManager, func([]uint8) error, error) {
	tm, err := NewTransactionManager(ctx, args.Env, args.TransactionalId,
		commtypes.SerdeFormat(args.QueryInput.SerdeFormat))
	if err != nil {
		return nil, nil, fmt.Errorf("NewTransactionManager failed: %v", err)
	}
	err = tm.InitTransaction(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("InitTransaction failed: %v", err)
	}
	offsetMap := make(map[string]uint64)
	for _, inputTopicName := range args.QueryInput.InputTopicNames {
		offset, err := createOffsetTopicAndGetOffset(ctx, tm, inputTopicName,
			uint8(args.QueryInput.NumInPartition), args.QueryInput.ParNum)
		if err != nil {
			return nil, nil, err
		}
		offsetMap[inputTopicName] = offset
	}
	if args.KVChangelogs != nil {
		for _, kvchangelog := range args.KVChangelogs {
			topic := kvchangelog.changelog.TopicName()
			if offset, ok := offsetMap[topic]; ok {
				err = store.RestoreKVStateStore(ctx, kvchangelog.kvStore, kvchangelog.changelog, kvchangelog.parNum,
					args.MsgSerde, offset)
				if err != nil {
					return nil, nil, err
				}
			} else {
				offset, err := createOffsetTopicAndGetOffset(ctx, tm, topic,
					kvchangelog.changelog.NumPartition(), kvchangelog.parNum)
				if err != nil {
					return nil, nil, err
				}
				err = store.RestoreKVStateStore(ctx, kvchangelog.kvStore, kvchangelog.changelog, kvchangelog.parNum,
					args.MsgSerde, offset)
				if err != nil {
					return nil, nil, err
				}
			}
		}
	}
	if args.WindowStoreChangelogs != nil {
		for _, wschangelog := range args.WindowStoreChangelogs {
			topic := wschangelog.changelog.TopicName()
			if offset, ok := offsetMap[topic]; ok {
				err = store.RestoreWindowStateStore(ctx, wschangelog.windowStore,
					wschangelog.changelog, wschangelog.parNum,
					args.MsgSerde, wschangelog.keyWindowTsSerde,
					wschangelog.keySerde, wschangelog.valSerde, offset)
				if err != nil {
					return nil, nil, err
				}
			} else {
				offset, err := createOffsetTopicAndGetOffset(ctx, tm, topic,
					wschangelog.changelog.NumPartition(), wschangelog.parNum)
				if err != nil {
					return nil, nil, err
				}
				err = store.RestoreWindowStateStore(ctx, wschangelog.windowStore,
					wschangelog.changelog, wschangelog.parNum,
					args.MsgSerde, wschangelog.keyWindowTsSerde,
					wschangelog.keySerde, wschangelog.valSerde, offset)
				if err != nil {
					return nil, nil, err
				}
			}
		}
	}

	for _, inputTopicName := range args.QueryInput.InputTopicNames {
		offset := offsetMap[inputTopicName]
		args.Srcs[inputTopicName].SetCursor(offset+1, args.QueryInput.ParNum)
	}

	trackFunc := func(u []uint8) error {
		return tm.AddTopicPartition(ctx, args.QueryInput.OutputTopicName, u)
	}
	return tm, trackFunc, nil
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

type KVStoreChangelog struct {
	kvStore   store.KeyValueStore
	changelog store.Stream
	parNum    uint8
}

func NewKVStoreChangelog(kvStore store.KeyValueStore, changelog store.Stream, parNum uint8) *KVStoreChangelog {
	return &KVStoreChangelog{
		kvStore:   kvStore,
		changelog: changelog,
		parNum:    parNum,
	}
}

type WindowStoreChangelog struct {
	windowStore      store.WindowStore
	changelog        store.Stream
	keyWindowTsSerde commtypes.Serde
	keySerde         commtypes.Serde
	valSerde         commtypes.Serde
	parNum           uint8
}

func NewWindowStoreChangelog(
	wsStore store.WindowStore,
	changelog store.Stream,
	keyWindowTsSerde commtypes.Serde,
	keySerde commtypes.Serde,
	valSerde commtypes.Serde,
	parNum uint8,
) *WindowStoreChangelog {
	return &WindowStoreChangelog{
		windowStore:      wsStore,
		changelog:        changelog,
		keyWindowTsSerde: keyWindowTsSerde,
		keySerde:         keySerde,
		valSerde:         valSerde,
		parNum:           parNum,
	}
}

type StreamTaskArgsTransaction struct {
	ProcArgs              interface{}
	Env                   types.Environment
	MsgSerde              commtypes.MsgSerde
	Srcs                  map[string]processor.Source
	OutputStream          *ShardedSharedLogStream
	QueryInput            *common.QueryInput
	TestParams            map[string]bool
	TransactionalId       string
	KVChangelogs          []*KVStoreChangelog
	WindowStoreChangelogs []*WindowStoreChangelog
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
				ret.Latencies = map[string][]int{
					"e2e": latencies,
				}
				ret.Duration = time.Since(startTime).Seconds()
			}
			return ret
		}
		elapsed := time.Since(procStart)
		latencies = append(latencies, int(elapsed.Microseconds()))
	}
	return &common.FnOutput{
		Success:  true,
		Duration: time.Since(startTime).Seconds(),
		Latencies: map[string][]int{
			"e2e": latencies,
		},
	}
}

func (t *StreamTask) ProcessWithTransaction(
	ctx context.Context,
	tm *TransactionManager,
	args *StreamTaskArgsTransaction,
) *common.FnOutput {
	tm.RecordTopicStreams(args.QueryInput.OutputTopicName, args.OutputStream)

	monitorQuit := make(chan struct{})
	monitorErrc := make(chan error)

	dctx, dcancel := context.WithCancel(ctx)
	go tm.MonitorTransactionLog(ctx, monitorQuit, monitorErrc, dcancel)

	retc := make(chan *common.FnOutput)
	go t.processWithTranLoop(dctx, tm, args, retc)
	for {
		select {
		case ret := <-retc:
			close(monitorQuit)
			return ret
		case merr := <-monitorErrc:
			close(monitorQuit)
			if merr != nil {
				return &common.FnOutput{
					Success: false,
					Message: fmt.Sprintf("monitor failed: %v", merr),
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
			if val, ok := args.TestParams["FailBeforeCommit"]; ok && val {
				fmt.Fprintf(os.Stderr, "about to fail before commit")
				retc <- &common.FnOutput{
					Success: false,
					Message: "fail before commit",
				}
				return
			}
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
			if val, ok := args.TestParams["FailAfterCommit"]; ok && val {
				fmt.Fprintf(os.Stderr, "about to fail after commit")
				retc <- &common.FnOutput{
					Success: false,
					Message: "fail after commit",
				}
				return
			}
		}
		if timeout {
			err := tm.Close()
			if err != nil {
				retc <- &common.FnOutput{
					Success: false,
					Message: fmt.Sprintf("close transaction manager: %v\n", err),
				}
				return
			}
			break
		}
		if !hasLiveTransaction {
			err := tm.BeginTransaction(ctx)
			if err != nil {
				retc <- &common.FnOutput{
					Success: false,
					Message: fmt.Sprintf("transaction begin failed: %v\n", err),
				}
				return
			}
			if idx == 5 {
				if val, ok := args.TestParams["FailAfterBegin"]; ok && val {
					fmt.Fprintf(os.Stderr, "about to fail after begin")
					retc <- &common.FnOutput{
						Success: false,
						Message: "fail after begin",
					}
					return
				}
			}
			hasLiveTransaction = true
			commitTimer = time.Now()
			if args.FixedOutParNum != 0 {
				err = tm.AddTopicPartition(ctx, args.QueryInput.OutputTopicName, []uint8{args.FixedOutParNum})
				if err != nil {
					retc <- &common.FnOutput{
						Success: false,
						Message: fmt.Sprintf("track topic partition failed: %v\n", err),
					}
					return
				}
			}
		}
		if !trackConsumePar {
			for _, inputTopicName := range args.QueryInput.InputTopicNames {
				err := tm.AddTopicTrackConsumedSeqs(ctx, inputTopicName, []uint8{args.QueryInput.ParNum})
				if err != nil {
					retc <- &common.FnOutput{
						Success: false,
						Message: fmt.Sprintf("add offsets failed: %v\n", err),
					}
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
				ret.Latencies = map[string][]int{
					"e2e": latencies,
				}
				ret.Duration = time.Since(startTime).Seconds()
			} else {
				if hasLiveTransaction {
					err := tm.AbortTransaction(ctx)
					if err != nil {
						retc <- &common.FnOutput{
							Success: false,
							Message: fmt.Sprintf("abort failed: %v\n", err),
						}
						return
					}
				}
			}
			retc <- ret
			return
		}
		if idx == 5 {
			if val, ok := args.TestParams["FailAfterProcess"]; ok && val {
				fmt.Fprintf(os.Stderr, "about to fail after process\n")
				retc <- &common.FnOutput{
					Success: false,
					Message: "fail after begin",
				}
				return
			}
		}
		currentOffset = off
		elapsed := time.Since(procStart)
		latencies = append(latencies, int(elapsed.Microseconds()))
		idx += 1
	}
	retc <- &common.FnOutput{
		Success:  true,
		Duration: time.Since(startTime).Seconds(),
		Latencies: map[string][]int{
			"e2e": latencies,
		},
	}
}
