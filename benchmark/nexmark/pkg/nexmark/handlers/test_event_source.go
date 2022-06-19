package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"sharedlog-stream/benchmark/common"
	nexmarkutils "sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/utils"

	ntypes "sharedlog-stream/benchmark/nexmark/pkg/nexmark/types"

	"cs.utexas.edu/zjia/faas/types"
)

type testEventSource struct {
	env      types.Environment
	funcName string
}

func NewTestEventSource(env types.Environment, funcName string) types.FuncHandler {
	return &testEventSource{
		env:      env,
		funcName: funcName,
	}
}

func (h *testEventSource) Call(ctx context.Context, input []byte) ([]byte, error) {
	inputConfig := &common.TestSourceInput{}
	err := json.Unmarshal(input, inputConfig)
	if err != nil {
		return nil, err
	}
	output := h.eventGeneration(ctx, inputConfig)
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		panic(err)
	}
	return nexmarkutils.CompressData(encodedOutput), nil
}

func (h *testEventSource) eventGeneration(ctx context.Context, sp *common.TestSourceInput) *common.FnOutput {
	stream, err := sharedlog_stream.NewShardedSharedLogStream(h.env, "nexmark_src", 1, commtypes.JSON)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	msgSerde, err := commtypes.GetMsgSerde(commtypes.JSON)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	eventSerde, err := ntypes.GetEventSerde(commtypes.JSON)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	events_byte, err := utils.ReadFileContent(sp.FileName)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	events := ntypes.Events{}
	err = json.Unmarshal(events_byte, &events)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	for _, event := range events.EventsArr {
		msgEncoded, err := encodeEvent(&event, eventSerde, msgSerde)
		if err != nil {
			return &common.FnOutput{Success: false, Message: fmt.Sprintf("msg serialization failed: %v", err)}
		}
		_, err = stream.Push(ctx, msgEncoded, 0, sharedlog_stream.SingleDataRecordMeta,
			commtypes.EmptyProducerId)
		if err != nil {
			return &common.FnOutput{Success: false, Message: err.Error()}
		}
	}
	return &common.FnOutput{Success: true}
}
