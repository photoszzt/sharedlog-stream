package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sharedlog-stream/pkg/stream/processor"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/stream/processor/store"
	"sharedlog-stream/pkg/treemap"

	"cs.utexas.edu/zjia/faas/types"
)

type query3JoinTableProcessorHandler struct {
	env           types.Environment
	currentOffset map[string]uint64
}

func NewQuery3JoinTableProcessor(env types.Environment) types.FuncHandler {
	return &query3JoinTableProcessorHandler{
		env:           env,
		currentOffset: make(map[string]uint64),
	}
}

func (h *query3JoinTableProcessorHandler) Call(ctx context.Context, input []byte) ([]byte, error) {
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

func (h *query3JoinTableProcessorHandler) process(ctx context.Context,
	argsTmp interface{},
	trackParFunc func([]uint8) error,
) (map[string]uint64, *common.FnOutput) {
	return h.currentOffset, nil
}

func (h *query3JoinTableProcessorHandler) toAuctionsBySellerIDTable(
	sp *common.QueryInput,
	eventSerde commtypes.Serde,
	msgSerde commtypes.MsgSerde,
) (*processor.MeteredProcessor, error) {
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
	return toTableProc, nil
}

func (h *query3JoinTableProcessorHandler) toPersonsByIDMapTable(
	sp *common.QueryInput,
	eventSerde commtypes.Serde,
	msgSerde commtypes.MsgSerde,
) (*processor.MeteredProcessor, error) {
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
	return toTableProc, nil
}

func (h *query3JoinTableProcessorHandler) Query3JoinTable(ctx context.Context, sp *common.QueryInput) *common.FnOutput {

	/*
		joiner := processor.ValueJoinerFunc(func(leftVal interface{}, rightVal interface{}) interface{} {
			event := rightVal.(*ntypes.Event)
			return &ntypes.NameCityStateId{
				Name:  event.NewPerson.Name,
				City:  event.NewPerson.City,
				State: event.NewPerson.State,
				ID:    event.NewPerson.ID,
			}
		})
	*/
	return nil
}
