package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"sharedlog-stream/benchmark/common"
	ntypes "sharedlog-stream/benchmark/nexmark/pkg/nexmark/types"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sharedlog-stream/pkg/hash"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/stream/processor"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/stream/processor/store"
	"sharedlog-stream/pkg/treemap"

	"cs.utexas.edu/zjia/faas/types"
)

type q3JoinTableHandler struct {
	env           types.Environment
	cHash         *hash.ConsistentHash
	currentOffset map[string]uint64
}

func NewQ3JoinTableHandler(env types.Environment) types.FuncHandler {
	return &q3JoinTableHandler{
		env:           env,
		cHash:         hash.NewConsistentHash(),
		currentOffset: make(map[string]uint64),
	}
}

func (h *q3JoinTableHandler) Call(ctx context.Context, input []byte) ([]byte, error) {
	parsedInput := &common.QueryInput{}
	err := json.Unmarshal(input, parsedInput)
	if err != nil {
		return nil, err
	}
	output := h.Query3JoinTable(ctx, parsedInput)
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		panic(err)
	}
	fmt.Printf("query 3 output: %v\n", encodedOutput)
	return utils.CompressData(encodedOutput), nil
}

func (h *q3JoinTableHandler) process(ctx context.Context,
	argsTmp interface{},
	trackParFunc func([]uint8) error,
) (map[string]uint64, *common.FnOutput) {
	return h.currentOffset, nil
}

func (h *q3JoinTableHandler) toAuctionsBySellerIDTable(
	sp *common.QueryInput,
	eventSerde commtypes.Serde,
	msgSerde commtypes.MsgSerde,
) (*processor.MeteredProcessor, *store.InMemoryKeyValueStore, error) {
	auctionsBySellerIDStoreName := "auctionsBySellerIDStore"
	auctionsBySellerIDStore := store.NewInMemoryKeyValueStore(auctionsBySellerIDStoreName, func(a, b treemap.Key) int {
		valA := a.(uint64)
		valB := b.(uint64)
		if valA < valB {
			return -1
		} else if valA == valB {
			return 0
		} else {
			return 1
		}
	})
	toTableProc := processor.NewMeteredProcessor(processor.NewStoreToKVTableProcessor(auctionsBySellerIDStore))
	return toTableProc, auctionsBySellerIDStore, nil
}

func (h *q3JoinTableHandler) toPersonsByIDMapTable(
	sp *common.QueryInput,
	eventSerde commtypes.Serde,
	msgSerde commtypes.MsgSerde,
) (*processor.MeteredProcessor, *store.InMemoryKeyValueStore, error) {
	personsByIDStoreName := "personsByIDStore"
	personsByIDStore := store.NewInMemoryKeyValueStore(personsByIDStoreName, func(a, b treemap.Key) int {
		valA := a.(uint64)
		valB := b.(uint64)
		if valA < valB {
			return -1
		} else if valA == valB {
			return 0
		} else {
			return 1
		}
	})
	toTableProc := processor.NewMeteredProcessor(processor.NewStoreToKVTableProcessor(personsByIDStore))
	return toTableProc, personsByIDStore, nil
}

func (h *q3JoinTableHandler) getShardedInputOutputStreams(ctx context.Context,
	input *common.QueryInput,
) (*sharedlog_stream.ShardedSharedLogStream, /* auction */
	*sharedlog_stream.ShardedSharedLogStream, /* person */
	*sharedlog_stream.ShardedSharedLogStream, /* output */
	error,
) {
	inputStreamAuction, err := sharedlog_stream.NewShardedSharedLogStream(h.env, input.InputTopicNames[0], uint8(input.NumInPartition))
	if err != nil {
		return nil, nil, nil, fmt.Errorf("NewSharedlogStream for input stream failed: %v", err)

	}
	inputStreamPerson, err := sharedlog_stream.NewShardedSharedLogStream(h.env, input.InputTopicNames[1], uint8(input.NumInPartition))
	if err != nil {
		return nil, nil, nil, fmt.Errorf("NewSharedlogStream for input stream failed: %v", err)

	}
	outputStream, err := sharedlog_stream.NewShardedSharedLogStream(h.env, input.OutputTopicName, uint8(input.NumOutPartition))
	if err != nil {
		return nil, nil, nil, fmt.Errorf("NewSharedlogStream for output stream failed: %v", err)
	}
	return inputStreamAuction, inputStreamPerson, outputStream, nil
}

func (h *q3JoinTableHandler) getSrcSink(ctx context.Context, sp *common.QueryInput,
	auctionsStream *sharedlog_stream.ShardedSharedLogStream,
	personsStream *sharedlog_stream.ShardedSharedLogStream,
	outputStream *sharedlog_stream.ShardedSharedLogStream,
	eventSerde commtypes.Serde,
	msgSerde commtypes.MsgSerde,
) (*processor.MeteredSource, /* auctionSrc */
	*processor.MeteredSource, /* personsSrc */
	*processor.MeteredSink, error,
) {
	auctionsConfig := &sharedlog_stream.SharedLogStreamConfig{
		Timeout:      common.SrcConsumeTimeout,
		KeyDecoder:   commtypes.Uint64Decoder{},
		ValueDecoder: eventSerde,
		MsgDecoder:   msgSerde,
	}
	personsConfig := &sharedlog_stream.SharedLogStreamConfig{
		Timeout:      common.SrcConsumeTimeout,
		KeyDecoder:   commtypes.Uint64Decoder{},
		ValueDecoder: eventSerde,
		MsgDecoder:   msgSerde,
	}
	outConfig := &sharedlog_stream.StreamSinkConfig{
		ValueEncoder: commtypes.EncoderFunc(func(val interface{}) ([]byte, error) {
			ret := val.(*ntypes.NameCityStateId)
			if sp.SerdeFormat == uint8(commtypes.JSON) {
				return json.Marshal(ret)
			} else {
				return ret.MarshalMsg(nil)
			}
		}),
	}

	auctionsSrc := processor.NewMeteredSource(sharedlog_stream.NewShardedSharedLogStreamSource(auctionsStream, auctionsConfig))
	personsSrc := processor.NewMeteredSource(sharedlog_stream.NewShardedSharedLogStreamSource(personsStream, personsConfig))
	sink := processor.NewMeteredSink(sharedlog_stream.NewShardedSharedLogStreamSink(outputStream, outConfig))
	return auctionsSrc, personsSrc, sink, nil
}

type q3JoinTableProcessArgs struct {
	personSrc  *processor.MeteredSource
	auctionSrc *processor.MeteredSource
	sink       *processor.MeteredSink
}

func (h *q3JoinTableHandler) Query3JoinTable(ctx context.Context, sp *common.QueryInput) *common.FnOutput {
	/*
		msgSerde, err := commtypes.GetMsgSerde(sp.SerdeFormat)
		if err != nil {
			return &common.FnOutput{
				Success: false,
				Message: fmt.Sprintf("get msg serde err: %v", err),
			}
		}
		eventSerde, err := getEventSerde(sp.SerdeFormat)
		if err != nil {
			return &common.FnOutput{
				Success: false,
				Message: fmt.Sprintf("get event serde err: %v", err),
			}
		}
		auctionsStream, personsStream, outputStream, err := h.getShardedInputOutputStreams(ctx, sp)
		if err != nil {
			return &common.FnOutput{
				Success: false,
				Message: fmt.Sprintf("get input output err: %v", err),
			}
		}
		auctionsSrc, personsSrc, sink, err := h.getSrcSink(ctx, sp, auctionsStream, personsStream, outputStream, eventSerde, msgSerde)
		if err != nil {
			return &common.FnOutput{
				Success: false,
				Message: fmt.Sprintf("getSrcSink err: %v\n", err),
			}
		}

		toAuctionsTable, auctionsStore, err := h.toAuctionsBySellerIDTable(sp, eventSerde, msgSerde)
		if err != nil {
			return &common.FnOutput{
				Success: false,
				Message: fmt.Sprintf("toAucTab err: %v\n", err),
			}
		}

		toPersonsTable, personsStore, err := h.toPersonsByIDMapTable(sp, eventSerde, msgSerde)
		if err != nil {
			return &common.FnOutput{
				Success: false,
				Message: fmt.Sprintf("toPersonsTab err: %v\n", err),
			}
		}

		joiner := processor.ValueJoinerWithKeyFunc(func(readOnlyKey interface{}, leftVal interface{}, rightVal interface{}) interface{} {
			event := rightVal.(*ntypes.Event)
			return &ntypes.NameCityStateId{
				Name:  event.NewPerson.Name,
				City:  event.NewPerson.City,
				State: event.NewPerson.State,
				ID:    event.NewPerson.ID,
			}
		})

		personJoinsAuctions := processor.NewMeteredProcessor(
			processor.NewTableTableJoinProcessor(auctionsStore.Name(), auctionsStore, joiner))

		auctionJoinsPersons := processor.NewMeteredProcessor(
			processor.NewTableTableJoinProcessor(personsStore.Name(), personsStore, joiner))

		pJoinA := processor.NewAsyncFuncRunner(ctx, func(c context.Context, m commtypes.Message) error {
			// msg is person
			_, err := toPersonsTable.ProcessAndReturn(ctx, m)
			if err != nil {
				return err
			}
			joinedMsgs, err := personJoinsAuctions.ProcessAndReturn(ctx, m)
			if err != nil {
				return err
			}

			k := joinedMsgs[0].Key.(uint64)
			parTmp, ok := h.cHash.Get(k)
			if !ok {
				return fmt.Errorf("fail to get output partition")
			}
			par := parTmp.(uint8)
			err = sink.Sink(ctx, joinedMsgs[0], par, false)
			if err != nil {
				return fmt.Errorf("sink err: %v", err)
			}
			return nil
		})

		for i := uint8(0); i < sp.NumOutPartition; i++ {
			h.cHash.Add(i)
		}

		procArgs := &q3JoinTableProcessArgs{
			auctionSrc: auctionsSrc,
			personSrc:  personsSrc,
			sink:       sink,
		}

		task := sharedlog_stream.StreamTask{
			ProcessFunc: h.process,
		}
		if sp.EnableTransaction {
			srcs := make(map[string]processor.Source)
			srcs[sp.InputTopicNames[0]] = auctionsSrc
			srcs[sp.InputTopicNames[1]] = personsSrc
			streamTaskArgs := sharedlog_stream.StreamTaskArgsTransaction{
				ProcArgs:     procArgs,
				Env:          h.env,
				Srcs:         srcs,
				OutputStream: outputStream,
				QueryInput:   sp,
				TransactionalId: fmt.Sprintf("q3JoinTable-%s-%d-%s",
					sp.InputTopicNames[0], sp.ParNum, sp.OutputTopicName),
			}
			ret := task.ProcessWithTransaction(ctx, &streamTaskArgs)
			if ret != nil && ret.Success {
				ret.Latencies["auctionsSrc"] = auctionsSrc.GetLatency()
				ret.Latencies["personsSrc"] = personsSrc.GetLatency()
				ret.Latencies["sink"] = sink.GetLatency()
			}
			return ret
		}
		streamTaskArgs := sharedlog_stream.StreamTaskArgs{
			ProcArgs: procArgs,
			Duration: time.Duration(sp.Duration) * time.Second,
		}
		ret := task.Process(ctx, &streamTaskArgs)
		if ret != nil && ret.Success {
			ret.Latencies["auctionsSrc"] = auctionsSrc.GetLatency()
			ret.Latencies["personsSrc"] = personsSrc.GetLatency()
			ret.Latencies["sink"] = sink.GetLatency()
		}
		return ret
	*/
	return nil
}
