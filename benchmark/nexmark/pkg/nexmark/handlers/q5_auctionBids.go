package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/common/benchutil"
	ntypes "sharedlog-stream/benchmark/nexmark/pkg/nexmark/types"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sharedlog-stream/pkg/concurrent_skiplist"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/errors"
	"sharedlog-stream/pkg/hash"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/stream/processor"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/stream/processor/store"
	"sharedlog-stream/pkg/transaction"
	"sync"
	"time"

	"cs.utexas.edu/zjia/faas/types"
	"golang.org/x/xerrors"
)

type q5AuctionBids struct {
	env      types.Environment
	cHashMu  sync.RWMutex
	cHash    *hash.ConsistentHash
	funcName string
}

func NewQ5AuctionBids(env types.Environment, funcName string) *q5AuctionBids {
	return &q5AuctionBids{
		env:      env,
		cHash:    hash.NewConsistentHash(),
		funcName: funcName,
	}
}

func (h *q5AuctionBids) Call(ctx context.Context, input []byte) ([]byte, error) {
	sp := &common.QueryInput{}
	err := json.Unmarshal(input, sp)
	if err != nil {
		return nil, err
	}
	output := h.processQ5AuctionBids(ctx, sp)
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		return nil, err
	}
	return utils.CompressData(encodedOutput), nil
}

func (h *q5AuctionBids) getSrcSink(ctx context.Context, sp *common.QueryInput,
	msgSerde commtypes.MsgSerde, input_stream *sharedlog_stream.ShardedSharedLogStream,
	output_stream *sharedlog_stream.ShardedSharedLogStream,
) (*processor.MeteredSource, *processor.ConcurrentMeteredSink, error) {
	var seSerde commtypes.Serde
	var aucIdCountSerde commtypes.Serde
	if sp.SerdeFormat == uint8(commtypes.JSON) {
		seSerde = ntypes.StartEndTimeJSONSerde{}
		aucIdCountSerde = ntypes.AuctionIdCountJSONSerde{}
	} else if sp.SerdeFormat == uint8(commtypes.MSGP) {
		seSerde = ntypes.StartEndTimeMsgpSerde{}
		aucIdCountSerde = ntypes.AuctionIdCountMsgpSerde{}
	} else {
		return nil, nil, fmt.Errorf("serde format should be either json or msgp; but %v is given", sp.SerdeFormat)
	}

	eventSerde, err := getEventSerde(sp.SerdeFormat)
	if err != nil {
		return nil, nil, err
	}

	inConfig := &sharedlog_stream.StreamSourceConfig{
		Timeout:      common.SrcConsumeTimeout,
		KeyDecoder:   commtypes.Uint64Serde{},
		ValueDecoder: eventSerde,
		MsgDecoder:   msgSerde,
	}
	outConfig := &sharedlog_stream.StreamSinkConfig{
		MsgSerde:   msgSerde,
		KeySerde:   seSerde,
		ValueSerde: aucIdCountSerde,
	}
	src := processor.NewMeteredSource(sharedlog_stream.NewShardedSharedLogStreamSource(input_stream, inConfig))
	sink := processor.NewConcurrentMeteredSink(sharedlog_stream.NewShardedSharedLogStreamSink(output_stream, outConfig))
	return src, sink, nil
}

func (h *q5AuctionBids) getCountAggProc(ctx context.Context, sp *common.QueryInput, msgSerde commtypes.MsgSerde,
) (*processor.MeteredProcessor, store.WindowStore, error) {
	hopWindow, err := processor.NewTimeWindowsWithGrace(time.Duration(10)*time.Second, time.Duration(5)*time.Second)
	if err != nil {
		return nil, nil, err
	}
	hopWindow, err = hopWindow.AdvanceBy(time.Duration(2) * time.Second)
	if err != nil {
		return nil, nil, err
	}

	var vtSerde commtypes.Serde
	if sp.SerdeFormat == uint8(commtypes.JSON) {
		vtSerde = commtypes.ValueTimestampJSONSerde{
			ValJSONSerde: commtypes.Uint64Serde{},
		}
	} else if sp.SerdeFormat == uint8(commtypes.MSGP) {
		vtSerde = commtypes.ValueTimestampMsgpSerde{
			ValMsgpSerde: commtypes.Uint64Serde{},
		}
	} else {
		return nil, nil, fmt.Errorf("serde format should be either json or msgp; but %v is given", sp.SerdeFormat)
	}
	var countWindowStore store.WindowStore
	countStoreName := "auctionBidsCountStore"
	if sp.TableType == uint8(store.IN_MEM) {
		changelogName := countStoreName + "-changelog"
		changelog, err := sharedlog_stream.NewShardedSharedLogStream(h.env, changelogName, sp.NumOutPartitions[0],
			commtypes.SerdeFormat(sp.SerdeFormat))
		if err != nil {
			return nil, nil, fmt.Errorf("NewShardedSharedLogStream failed: %v", err)
		}
		countMp := &store.MaterializeParam{
			KeySerde:   commtypes.Uint64Serde{},
			ValueSerde: vtSerde,
			MsgSerde:   msgSerde,
			StoreName:  countStoreName,
			Changelog:  changelog,
			Comparable: concurrent_skiplist.CompareFunc(func(lhs, rhs interface{}) int {
				l := lhs.(uint64)
				r := rhs.(uint64)
				if l < r {
					return -1
				} else if l == r {
					return 0
				} else {
					return 1
				}
			}),
			ParNum: sp.ParNum,
		}
		countWindowStore, err = store.NewInMemoryWindowStoreWithChangelog(
			hopWindow.MaxSize()+hopWindow.GracePeriodMs(),
			hopWindow.MaxSize(), false, countMp,
		)
		if err != nil {
			return nil, nil, err
		}
	} else if sp.TableType == uint8(store.MONGODB) {
		client, err := store.InitMongoDBClient(ctx, sp.MongoAddr)
		if err != nil {
			return nil, nil, err
		}
		mkvs, err := store.NewMongoDBKeyValueStore(ctx, &store.MongoDBConfig{
			Client:         client,
			CollectionName: countStoreName,
			DBName:         countStoreName,
			KeySerde:       nil,
			ValueSerde:     nil,
		})
		if err != nil {
			return nil, nil, err
		}
		byteStore, err := store.NewMongoDBSegmentedBytesStore(ctx, countStoreName,
			hopWindow.MaxSize()+hopWindow.GracePeriodMs(),
			&store.WindowKeySchema{}, mkvs)
		if err != nil {
			return nil, nil, err
		}
		countWindowStore = store.NewSegmentedWindowStore(byteStore, false, hopWindow.MaxSize(),
			commtypes.Uint64Serde{}, vtSerde)
	}
	countProc := processor.NewMeteredProcessor(processor.NewStreamWindowAggregateProcessor(countWindowStore,
		processor.InitializerFunc(func() interface{} { return uint64(0) }),
		processor.AggregatorFunc(func(key, value, aggregate interface{}) interface{} {
			val := aggregate.(uint64)
			return val + 1
		}), hopWindow))
	return countProc, countWindowStore, nil
}

type q5AuctionBidsProcessArg struct {
	countProc        *processor.MeteredProcessor
	groupByAuction   *processor.MeteredProcessor
	src              *processor.MeteredSource
	sink             *processor.ConcurrentMeteredSink
	output_stream    *sharedlog_stream.ShardedSharedLogStream
	trackParFunc     transaction.TrackKeySubStreamFunc
	recordFinishFunc transaction.RecordPrevInstanceFinishFunc
	funcName         string
	curEpoch         uint64
	parNum           uint8
	numOutPartition  uint8
}

func (a *q5AuctionBidsProcessArg) Source() processor.Source { return a.src }
func (a *q5AuctionBidsProcessArg) PushToAllSinks(ctx context.Context, msg commtypes.Message, parNum uint8, isControl bool) error {
	return a.sink.Sink(ctx, msg, parNum, isControl)
}
func (a *q5AuctionBidsProcessArg) ParNum() uint8    { return a.parNum }
func (a *q5AuctionBidsProcessArg) CurEpoch() uint64 { return a.curEpoch }
func (a *q5AuctionBidsProcessArg) FuncName() string { return a.funcName }
func (a *q5AuctionBidsProcessArg) RecordFinishFunc() func(ctx context.Context, funcName string, instanceId uint8) error {
	return a.recordFinishFunc
}
func (a *q5AuctionBidsProcessArg) ErrChan() chan error {
	return nil
}

type q5AuctionBidsRestoreArg struct {
	countProc      processor.Processor
	groupByAuction processor.Processor
	src            processor.Source
	parNum         uint8
}

func (h *q5AuctionBids) process(ctx context.Context, t *transaction.StreamTask, argsTmp interface{}) (map[string]uint64, *common.FnOutput) {
	args := argsTmp.(*q5AuctionBidsProcessArg)
	return transaction.CommonProcess(ctx, t, args, func(t *transaction.StreamTask, msg commtypes.MsgAndSeq) error {
		t.CurrentOffset[args.src.TopicName()] = msg.LogSeqNum
		if msg.MsgArr != nil {
			for _, subMsg := range msg.MsgArr {
				if subMsg.Value == nil {
					continue
				}
				err := h.procMsg(ctx, subMsg, args)
				if err != nil {
					return err
				}
			}
			return nil
		}
		return h.procMsg(ctx, msg.Msg, args)
	})
}

func (h *q5AuctionBids) procMsg(ctx context.Context, msg commtypes.Message, args *q5AuctionBidsProcessArg) error {
	event := msg.Value.(*ntypes.Event)
	ts, err := event.ExtractStreamTime()
	if err != nil {
		return fmt.Errorf("fail to extract timestamp: %v", err)
	}
	msg.Timestamp = ts
	countMsgs, err := args.countProc.ProcessAndReturn(ctx, msg)
	if err != nil {
		return fmt.Errorf("countProc err %v", err)
	}
	for _, countMsg := range countMsgs {
		// fmt.Fprintf(os.Stderr, "count msg ts: %v, ", countMsg.Timestamp)
		changeKeyedMsg, err := args.groupByAuction.ProcessAndReturn(ctx, countMsg)
		if err != nil {
			return fmt.Errorf("groupByAuction err %v", err)
		}
		// fmt.Fprintf(os.Stderr, "changeKeyedMsg ts: %v\n", changeKeyedMsg[0].Timestamp)
		// par := uint8(hashSe(changeKeyedMsg[0].Key.(*ntypes.StartEndTime)) % uint32(args.numOutPartition))
		k := changeKeyedMsg[0].Key.(*ntypes.StartEndTime)
		h.cHashMu.RLock()
		parTmp, ok := h.cHash.Get(k)
		h.cHashMu.RUnlock()
		if !ok {
			return xerrors.New("fail to get output partition")
		}
		par := parTmp.(uint8)
		// fmt.Fprintf(os.Stderr, "key is %s, output to substream %d\n", k.String(), par)
		err = args.trackParFunc(ctx, k, args.sink.KeySerde(), args.sink.TopicName(), par)
		if err != nil {
			return fmt.Errorf("add topic partition failed: %v", err)
		}
		err = args.sink.Sink(ctx, changeKeyedMsg[0], par, false)
		if err != nil {
			return fmt.Errorf("sink err %v", err)
		}
	}
	return nil
}

func (h *q5AuctionBids) processWithoutSink(ctx context.Context, argsTmp interface{}) error {
	args := argsTmp.(*q5AuctionBidsRestoreArg)
	gotMsgs, err := args.src.Consume(ctx, args.parNum)
	if err != nil {
		if xerrors.Is(err, errors.ErrStreamSourceTimeout) {
			return nil
		}
		return err
	}

	for _, msg := range gotMsgs.Msgs {
		if msg.MsgArr != nil {
			for _, subMsg := range msg.MsgArr {
				if subMsg.Value == nil {
					continue
				}
				err := h.procMsgWithoutSink(ctx, subMsg, args)
				if err != nil {
					return err
				}
			}
		} else {
			if msg.Msg.Value == nil {
				continue
			}
			err := h.procMsgWithoutSink(ctx, msg.Msg, args)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (h *q5AuctionBids) procMsgWithoutSink(ctx context.Context, msg commtypes.Message, args *q5AuctionBidsRestoreArg) error {
	event := msg.Value.(*ntypes.Event)
	ts, err := event.ExtractStreamTime()
	if err != nil {
		return fmt.Errorf("fail to extract timestamp: %v", err)
	}
	msg.Timestamp = ts
	countMsgs, err := args.countProc.ProcessAndReturn(ctx, msg)
	if err != nil {
		return err
	}
	for _, countMsg := range countMsgs {
		// fmt.Fprintf(os.Stderr, "count msg ts: %v, ", countMsg.Timestamp)
		_, err := args.groupByAuction.ProcessAndReturn(ctx, countMsg)
		if err != nil {
			return err
		}
	}
	return nil
}

func (h *q5AuctionBids) processQ5AuctionBids(ctx context.Context, sp *common.QueryInput) *common.FnOutput {
	msgSerde, err := commtypes.GetMsgSerde(sp.SerdeFormat)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	input_stream, output_streams, err := benchutil.GetShardedInputOutputStreams(ctx, h.env, sp, true)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	debug.Assert(len(output_streams) == 1, "expected only one output stream")
	src, sink, err := h.getSrcSink(ctx, sp, msgSerde, input_stream, output_streams[0])
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	countProc, countStore, err := h.getCountAggProc(ctx, sp, msgSerde)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	groupByAuction := processor.NewMeteredProcessor(processor.NewStreamMapProcessor(processor.MapperFunc(
		func(msg commtypes.Message) (commtypes.Message, error) {
			key := msg.Key.(*commtypes.WindowedKey)
			value := msg.Value.(uint64)
			newKey := &ntypes.StartEndTime{
				StartTime: key.Window.Start(),
				EndTime:   key.Window.End(),
			}
			newVal := &ntypes.AuctionIdCount{
				AucId:     key.Key.(uint64),
				Count:     value,
				TimeStamp: msg.Timestamp,
			}
			return commtypes.Message{Key: newKey, Value: newVal, Timestamp: msg.Timestamp}, nil
		})))
	procArgs := &q5AuctionBidsProcessArg{
		countProc:        countProc,
		groupByAuction:   groupByAuction,
		src:              src,
		sink:             sink,
		output_stream:    output_streams[0],
		parNum:           sp.ParNum,
		numOutPartition:  sp.NumOutPartitions[0],
		trackParFunc:     transaction.DefaultTrackSubstreamFunc,
		recordFinishFunc: transaction.DefaultRecordPrevInstanceFinishFunc,
		curEpoch:         sp.ScaleEpoch,
		funcName:         h.funcName,
	}

	task := transaction.StreamTask{
		ProcessFunc:   h.process,
		CurrentOffset: make(map[string]uint64),
		CommitEvery:   common.CommitDuration,
	}

	transaction.SetupConsistentHash(&h.cHashMu, h.cHash, sp.NumOutPartitions[0])
	srcs := map[string]processor.Source{sp.InputTopicNames[0]: src}
	if sp.EnableTransaction {
		var wsc []*transaction.WindowStoreChangelog
		if countStore.TableType() == store.IN_MEM {
			cstore := countStore.(*store.InMemoryWindowStoreWithChangelog)
			wsc = []*transaction.WindowStoreChangelog{
				transaction.NewWindowStoreChangelog(
					cstore,
					cstore.MaterializeParam().Changelog,
					cstore.KeyWindowTsSerde(),
					cstore.MaterializeParam().KeySerde,
					cstore.MaterializeParam().ValueSerde, 0),
			}
		} else if countStore.TableType() == store.MONGODB {
			wsc = []*transaction.WindowStoreChangelog{
				transaction.NewWindowStoreChangelogForExternalStore(countStore, input_stream,
					h.processWithoutSink, &q5AuctionBidsRestoreArg{
						countProc:      countProc.InnerProcessor(),
						groupByAuction: groupByAuction.InnerProcessor(),
						src:            src.InnerSource(),
						parNum:         sp.ParNum,
					}, fmt.Sprintf("%s-%s-%d", h.funcName, countStore.Name(), sp.ParNum), sp.ParNum),
			}
		} else {
			panic("unrecognized table type")
		}
		streamTaskArgs := transaction.StreamTaskArgsTransaction{
			ProcArgs:      procArgs,
			Env:           h.env,
			Srcs:          srcs,
			OutputStreams: output_streams,
			QueryInput:    sp,
			TransactionalId: fmt.Sprintf("%s-%s-%d-%s", h.funcName, sp.InputTopicNames[0],
				sp.ParNum, sp.OutputTopicNames[0]),
			FixedOutParNum:        0,
			WindowStoreChangelogs: wsc,
			KVChangelogs:          nil,
			MsgSerde:              msgSerde,
		}
		ret := transaction.SetupManagersAndProcessTransactional(ctx, h.env, &streamTaskArgs,
			func(procArgs interface{}, trackParFunc transaction.TrackKeySubStreamFunc, recordFinishFunc transaction.RecordPrevInstanceFinishFunc) {
				procArgs.(*q5AuctionBidsProcessArg).trackParFunc = trackParFunc
				procArgs.(*q5AuctionBidsProcessArg).recordFinishFunc = recordFinishFunc
			}, &task)
		if ret != nil && ret.Success {
			ret.Latencies["src"] = src.GetLatency()
			ret.Latencies["sink"] = sink.GetLatency()
			ret.Latencies["count"] = countProc.GetLatency()
			ret.Latencies["changeKey"] = groupByAuction.GetLatency()
			ret.Consumed["src"] = src.GetCount()
		}
		return ret
	}
	// return h.process(ctx, sp, args)
	streamTaskArgs := transaction.StreamTaskArgs{
		ProcArgs:       procArgs,
		Duration:       time.Duration(sp.Duration) * time.Second,
		Srcs:           srcs,
		ParNum:         sp.ParNum,
		SerdeFormat:    commtypes.SerdeFormat(sp.SerdeFormat),
		Env:            h.env,
		NumInPartition: sp.NumInPartition,
	}
	ret := task.Process(ctx, &streamTaskArgs)
	if ret != nil && ret.Success {
		ret.Latencies["src"] = src.GetLatency()
		ret.Latencies["sink"] = sink.GetLatency()
		ret.Latencies["count"] = countProc.GetLatency()
		ret.Latencies["changeKey"] = groupByAuction.GetLatency()
		ret.Consumed["src"] = src.GetCount()
	}
	return ret
}
