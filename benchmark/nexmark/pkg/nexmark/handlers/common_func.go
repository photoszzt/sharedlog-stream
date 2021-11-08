package handlers

import (
	"context"
	"fmt"
	"sharedlog-stream/benchmark/common"
	ntypes "sharedlog-stream/benchmark/nexmark/pkg/nexmark/types"

	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/stream/processor/commtypes"

	"cs.utexas.edu/zjia/faas/types"
)

func only_bid(msg *commtypes.Message) (bool, error) {
	event := msg.Value.(*ntypes.Event)
	return event.Etype == ntypes.BID, nil
}

func getEventSerde(serdeFormat uint8) (commtypes.Serde, error) {
	if serdeFormat == uint8(commtypes.JSON) {
		return ntypes.EventJSONSerde{}, nil
	} else if serdeFormat == uint8(commtypes.MSGP) {
		return ntypes.EventMsgpSerde{}, nil
	} else {
		return nil, fmt.Errorf("serde format should be either json or msgp; but %v is given", serdeFormat)
	}
}

func getPersonTimeSerde(serdeFormat uint8) (commtypes.Serde, error) {
	if serdeFormat == uint8(commtypes.JSON) {
		return ntypes.PersonTimeJSONSerde{}, nil
	} else if serdeFormat == uint8(commtypes.MSGP) {
		return ntypes.PersonTimeMsgpSerde{}, nil
	} else {
		return nil, fmt.Errorf("serde format should be either json or msgp; but %v is given", serdeFormat)
	}
}

func getShardedInputOutputStreams(ctx context.Context, env types.Environment, input *common.QueryInput) (*sharedlog_stream.ShardedSharedLogStream, *sharedlog_stream.ShardedSharedLogStream, error) {
	inputStream, err := sharedlog_stream.NewShardedSharedLogStream(env, input.InputTopicName, uint8(input.NumInPartition))
	if err != nil {
		return nil, nil, fmt.Errorf("NewSharedlogStream for input stream failed: %v", err)
	}
	err = inputStream.InitStream(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("InitStream failed: %v", err)
	}
	outputStream, err := sharedlog_stream.NewShardedSharedLogStream(env, input.OutputTopicName, uint8(input.NumOutPartition))
	if err != nil {
		return nil, nil, fmt.Errorf("NewSharedlogStream for output stream failed: %v", err)
	}
	err = outputStream.InitStream(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("InitStream failed: %v", err)
	}
	return inputStream, outputStream, nil
}
