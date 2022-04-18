package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/common/benchutil"
	ntypes "sharedlog-stream/benchmark/nexmark/pkg/nexmark/types"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sharedlog-stream/pkg/errors"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/stream/processor"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/stream/processor/store"
	"sharedlog-stream/pkg/transaction"
	"sharedlog-stream/pkg/treemap"
	"time"

	"cs.utexas.edu/zjia/faas/types"
	"golang.org/x/xerrors"
)

type q5MaxBid struct {
	env      types.Environment
	funcName string
}

func NewQ5MaxBid(env types.Environment, funcName string) types.FuncHandler {
	return &q5MaxBid{
		env:      env,
		funcName: funcName,
	}
}

func (h *q5MaxBid) Call(ctx context.Context, input []byte) ([]byte, error) {
	parsedInput := &common.QueryInput{}
	err := json.Unmarshal(input, parsedInput)
	if err != nil {
		return nil, err
	}
	output := h.processQ5MaxBid(ctx, parsedInput)
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		panic(err)
	}
	return utils.CompressData(encodedOutput), nil
}

func (h *q5MaxBid) getSrcSink(ctx context.Context, sp *common.QueryInput,
	input_stream *sharedlog_stream.ShardedSharedLogStream,
	output_stream *sharedlog_stream.ShardedSharedLogStream,
	seSerde commtypes.Serde, aucIdCountSerde commtypes.Serde,
	aucIdCountMaxSerde commtypes.Serde,
	msgSerde commtypes.MsgSerde,
) (*processor.MeteredSource, *processor.ConcurrentMeteredSink, error) {
	inConfig := &sharedlog_stream.StreamSourceConfig{
		Timeout:      time.Duration(20) * time.Second,
		KeyDecoder:   seSerde,
		ValueDecoder: aucIdCountSerde,
		MsgDecoder:   msgSerde,
	}
	outConfig := &sharedlog_stream.StreamSinkConfig{
		MsgSerde:   msgSerde,
		KeySerde:   seSerde,
		ValueSerde: aucIdCountMaxSerde,
	}
	src := processor.NewMeteredSource(sharedlog_stream.NewShardedSharedLogStreamSource(input_stream, inConfig))
	sink := processor.NewConcurrentMeteredSink(sharedlog_stream.NewShardedSharedLogStreamSink(output_stream, outConfig))
	sink.MarkFinalOutput()
	return src, sink, nil
}

type q5MaxBidProcessArgs struct {
	maxBid           *processor.MeteredProcessor
	stJoin           *processor.MeteredProcessor
	chooseMaxCnt     *processor.MeteredProcessor
	src              *processor.MeteredSource
	sink             *processor.ConcurrentMeteredSink
	output_stream    *sharedlog_stream.ShardedSharedLogStream
	trackParFunc     transaction.TrackKeySubStreamFunc
	recordFinishFunc transaction.RecordPrevInstanceFinishFunc
	funcName         string
	curEpoch         uint64
	parNum           uint8
}

func (a *q5MaxBidProcessArgs) Source() processor.Source { return a.src }
func (a *q5MaxBidProcessArgs) Sink() processor.Sink     { return a.sink }
func (a *q5MaxBidProcessArgs) ParNum() uint8            { return a.parNum }
func (a *q5MaxBidProcessArgs) CurEpoch() uint64         { return a.curEpoch }
func (a *q5MaxBidProcessArgs) FuncName() string         { return a.funcName }
func (a *q5MaxBidProcessArgs) RecordFinishFunc() func(ctx context.Context, funcName string, instanceId uint8) error {
	return a.recordFinishFunc
}

type q5MaxBidRestoreArgs struct {
	maxBid       processor.Processor
	stJoin       processor.Processor
	chooseMaxCnt processor.Processor
	src          processor.Source
	parNum       uint8
}

func (h *q5MaxBid) process(ctx context.Context, t *transaction.StreamTask, argsTmp interface{}) (map[string]uint64, *common.FnOutput) {
	args := argsTmp.(*q5MaxBidProcessArgs)
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

func (h *q5MaxBid) procMsg(ctx context.Context, msg commtypes.Message, args *q5MaxBidProcessArgs) error {
	aic := msg.Value.(*ntypes.AuctionIdCount)
	ts, err := aic.ExtractStreamTime()
	if err != nil {
		return fmt.Errorf("fail to extract timestamp: %v", err)
	}
	msg.Timestamp = ts
	// fmt.Fprintf(os.Stderr, "got msg with key: %v, val: %v, ts: %v\n", msg.Msg.Key, msg.Msg.Value, msg.Msg.Timestamp)
	_, err = args.maxBid.ProcessAndReturn(ctx, msg)
	if err != nil {
		return fmt.Errorf("maxBid err: %v", err)
	}
	joinedOutput, err := args.stJoin.ProcessAndReturn(ctx, msg)
	if err != nil {
		return fmt.Errorf("joined err: %v", err)
	}
	filteredMx, err := args.chooseMaxCnt.ProcessAndReturn(ctx, joinedOutput[0])
	if err != nil {
		return fmt.Errorf("filteredMx err: %v", err)
	}
	for _, filtered := range filteredMx {
		err = args.sink.Sink(ctx, filtered, args.parNum, false)
		if err != nil {
			return fmt.Errorf("sink err: %v", err)
		}
	}
	return nil
}

func (h *q5MaxBid) processWithoutSink(ctx context.Context, argsTmp interface{}) error {
	args := argsTmp.(*q5MaxBidRestoreArgs)
	gotMsgs, err := args.src.Consume(ctx, args.parNum)
	if err != nil {
		if xerrors.Is(err, errors.ErrStreamSourceTimeout) {
			return nil
		}
		return fmt.Errorf("consume err: %v", err)
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

func (h *q5MaxBid) procMsgWithoutSink(ctx context.Context, msg commtypes.Message, args *q5MaxBidRestoreArgs) error {
	aic := msg.Value.(*ntypes.AuctionIdCount)
	ts, err := aic.ExtractStreamTime()
	if err != nil {
		return fmt.Errorf("fail to extract timestamp: %v", err)
	}
	msg.Timestamp = ts
	// fmt.Fprintf(os.Stderr, "got msg with key: %v, val: %v, ts: %v\n", msg.Msg.Key, msg.Msg.Value, msg.Msg.Timestamp)
	_, err = args.maxBid.ProcessAndReturn(ctx, msg)
	if err != nil {
		return fmt.Errorf("maxBid err: %v", err)
	}
	joinedOutput, err := args.stJoin.ProcessAndReturn(ctx, msg)
	if err != nil {
		return fmt.Errorf("joined err: %v", err)
	}
	_, err = args.chooseMaxCnt.ProcessAndReturn(ctx, joinedOutput[0])
	if err != nil {
		return fmt.Errorf("filteredMx err: %v", err)
	}
	return nil
}

func (h *q5MaxBid) processQ5MaxBid(ctx context.Context, sp *common.QueryInput) *common.FnOutput {
	input_stream, output_stream, err := benchutil.GetShardedInputOutputStreams(ctx, h.env, sp, true)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}

	var seSerde commtypes.Serde
	var vtSerde commtypes.Serde
	var aucIdCountSerde commtypes.Serde
	var aucIdCountMaxSerde commtypes.Serde
	if sp.SerdeFormat == uint8(commtypes.JSON) {
		seSerde = ntypes.StartEndTimeJSONSerde{}
		aucIdCountSerde = ntypes.AuctionIdCountJSONSerde{}
		vtSerde = commtypes.ValueTimestampJSONSerde{
			ValJSONSerde: commtypes.Uint64Serde{},
		}
		aucIdCountMaxSerde = ntypes.AuctionIdCntMaxJSONSerde{}
	} else if sp.SerdeFormat == uint8(commtypes.MSGP) {
		seSerde = ntypes.StartEndTimeMsgpSerde{}
		aucIdCountSerde = ntypes.AuctionIdCountMsgpSerde{}
		vtSerde = commtypes.ValueTimestampMsgpSerde{
			ValMsgpSerde: commtypes.Uint64Serde{},
		}
		aucIdCountMaxSerde = ntypes.AuctionIdCntMaxMsgpSerde{}
	} else {
		return &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("serde format should be either json or msgp; but %v is given", sp.SerdeFormat),
		}
	}
	msgSerde, err := commtypes.GetMsgSerde(sp.SerdeFormat)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	src, sink, err := h.getSrcSink(ctx, sp,
		input_stream, output_stream, seSerde, aucIdCountSerde, aucIdCountMaxSerde, msgSerde)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	maxBidStoreName := "maxBidsKVStore"
	var kvstore store.KeyValueStore
	if sp.TableType == uint8(store.IN_MEM) {
		mp := &store.MaterializeParam{
			KeySerde:   seSerde,
			ValueSerde: vtSerde,
			MsgSerde:   msgSerde,
			StoreName:  maxBidStoreName,
			Changelog:  output_stream,
			ParNum:     sp.ParNum,
		}
		inMemStore := store.NewInMemoryKeyValueStore(mp.StoreName, func(a, b treemap.Key) int {
			ka := a.(*ntypes.StartEndTime)
			kb := b.(*ntypes.StartEndTime)
			return ntypes.CompareStartEndTime(ka, kb)
		})
		kvstore = store.NewKeyValueStoreWithChangelog(mp, inMemStore, false)
	} else if sp.TableType == uint8(store.MONGODB) {
		client, err := store.InitMongoDBClient(ctx, sp.MongoAddr)
		if err != nil {
			return &common.FnOutput{Success: false, Message: err.Error()}
		}
		kvstore, err = store.NewMongoDBKeyValueStore(ctx, &store.MongoDBConfig{
			Client:         client,
			CollectionName: maxBidStoreName,
			DBName:         maxBidStoreName,
			KeySerde:       seSerde,
			ValueSerde:     vtSerde,
		})
		if err != nil {
			return &common.FnOutput{Success: false, Message: err.Error()}
		}
	} else {
		panic("unrecognized table type")
	}
	maxBid := processor.NewMeteredProcessor(processor.NewStreamAggregateProcessor(kvstore, processor.InitializerFunc(func() interface{} {
		return uint64(0)
	}), processor.AggregatorFunc(func(key, value, aggregate interface{}) interface{} {
		v := value.(*ntypes.AuctionIdCount)
		agg := aggregate.(uint64)
		if v.Count > agg {
			return v.Count
		}
		return agg
	})))
	stJoin := processor.NewMeteredProcessor(processor.NewStreamTableJoinProcessor(maxBidStoreName, kvstore,
		processor.ValueJoinerWithKeyFunc(
			func(readOnlyKey interface{}, leftValue interface{}, rightValue interface{}) interface{} {
				lv := leftValue.(*ntypes.AuctionIdCount)
				rv := rightValue.(commtypes.ValueTimestamp)
				return &ntypes.AuctionIdCntMax{
					AucId:  lv.AucId,
					Count:  lv.Count,
					MaxCnt: rv.Value.(uint64),
				}
			})))
	chooseMaxCnt := processor.NewMeteredProcessor(
		processor.NewStreamFilterProcessor(
			processor.PredicateFunc(func(msg *commtypes.Message) (bool, error) {
				v := msg.Value.(*ntypes.AuctionIdCntMax)
				return v.Count >= v.MaxCnt, nil
			})))

	procArgs := &q5MaxBidProcessArgs{
		maxBid:           maxBid,
		stJoin:           stJoin,
		chooseMaxCnt:     chooseMaxCnt,
		src:              src,
		sink:             sink,
		output_stream:    output_stream,
		parNum:           sp.ParNum,
		trackParFunc:     transaction.DefaultTrackSubstreamFunc,
		recordFinishFunc: transaction.DefaultRecordPrevInstanceFinishFunc,
		curEpoch:         sp.ScaleEpoch,
		funcName:         h.funcName,
	}

	task := transaction.StreamTask{
		ProcessFunc:   h.process,
		CurrentOffset: make(map[string]uint64),
	}

	if sp.EnableTransaction {
		srcs := make(map[string]processor.Source)
		srcs[sp.InputTopicNames[0]] = src
		var kvc []*transaction.KVStoreChangelog
		if sp.TableType == uint8(store.IN_MEM) {
			kvstore_mem := kvstore.(*store.KeyValueStoreWithChangelog)
			mp := kvstore_mem.MaterializeParam()
			kvc = []*transaction.KVStoreChangelog{
				transaction.NewKVStoreChangelog(kvstore, mp.Changelog,
					mp.KeySerde, mp.ValueSerde, sp.ParNum),
			}
		} else if sp.TableType == uint8(store.MONGODB) {
			// TODO: MONGODB
			kvc = []*transaction.KVStoreChangelog{
				transaction.NewKVStoreChangelogForExternalStore(kvstore, input_stream, h.processWithoutSink, &q5MaxBidRestoreArgs{
					maxBid:       maxBid.InnerProcessor(),
					stJoin:       stJoin.InnerProcessor(),
					chooseMaxCnt: chooseMaxCnt.InnerProcessor(),
					src:          src.InnerSource(),
					parNum:       sp.ParNum,
				}, fmt.Sprintf("%s-%s-%d", h.funcName, kvstore.Name(), sp.ParNum), sp.ParNum),
			}
		} else {
			panic("unrecognized table type")
		}
		streamTaskArgs := transaction.StreamTaskArgsTransaction{
			ProcArgs:     procArgs,
			Env:          h.env,
			Srcs:         srcs,
			OutputStream: output_stream,
			QueryInput:   sp,
			TransactionalId: fmt.Sprintf("%s-%s-%d-%s", h.funcName,
				sp.InputTopicNames[0], sp.ParNum, sp.OutputTopicName),
			FixedOutParNum:        sp.ParNum,
			KVChangelogs:          kvc,
			WindowStoreChangelogs: nil,
			MsgSerde:              msgSerde,
			CHash:                 nil,
			CHashMu:               nil,
		}
		ret := transaction.SetupManagersAndProcessTransactional(ctx, h.env, &streamTaskArgs,
			func(procArgs interface{}, trackParFunc transaction.TrackKeySubStreamFunc, recordFinshFunc transaction.RecordPrevInstanceFinishFunc) {
				procArgs.(*q5MaxBidProcessArgs).trackParFunc = trackParFunc
				procArgs.(*q5MaxBidProcessArgs).recordFinishFunc = recordFinshFunc
			}, &task)
		if ret != nil && ret.Success {
			ret.Latencies["src"] = src.GetLatency()
			ret.Latencies["sink"] = sink.GetLatency()
			ret.Latencies["maxBid"] = maxBid.GetLatency()
			ret.Latencies["stJoin"] = stJoin.GetLatency()
			ret.Latencies["chooseMaxCnt"] = chooseMaxCnt.GetLatency()
			ret.Latencies["eventTimeLatency"] = sink.GetEventTimeLatency()
			ret.Consumed["src"] = src.GetCount()
		}
		return ret
	}
	streamTaskArgs := transaction.StreamTaskArgs{
		ProcArgs: procArgs,
		Duration: time.Duration(sp.Duration) * time.Second,
	}
	ret := task.Process(ctx, &streamTaskArgs)
	if ret != nil && ret.Success {
		ret.Latencies["src"] = src.GetLatency()
		ret.Latencies["sink"] = sink.GetLatency()
		ret.Latencies["maxBid"] = maxBid.GetLatency()
		ret.Latencies["stJoin"] = stJoin.GetLatency()
		ret.Latencies["chooseMaxCnt"] = chooseMaxCnt.GetLatency()
		ret.Latencies["eventTimeLatency"] = sink.GetEventTimeLatency()
		ret.Consumed["src"] = src.GetCount()
	}
	return ret
}
