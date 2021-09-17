package spike_detection

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/stream"
	"sharedlog-stream/pkg/stream/processor"

	"cs.utexas.edu/zjia/faas/types"
)

const (
	SPIKE_THRESHOLD = 0.03
)

type spikeDetectionHandler struct {
	env types.Environment
}

func spikeDetectionPredicate(msg processor.Message) (bool, error) {
	valAvg := msg.Value.(ValAndAvg)
	return math.Abs(valAvg.Val-valAvg.Avg) > SPIKE_THRESHOLD*valAvg.Avg, nil
}

func NewSpikeDetectionHandler(env types.Environment) types.FuncHandler {
	return &spikeDetectionHandler{
		env: env,
	}
}

func (h *spikeDetectionHandler) Call(ctx context.Context, input []byte) ([]byte, error) {
	parsedInput := &common.QueryInput{}
	err := json.Unmarshal(input, parsedInput)
	if err != nil {
		return nil, err
	}
	outputCh := make(chan *common.FnOutput)
	go SpikeDetection(ctx, h.env, parsedInput, outputCh)
	output := <-outputCh
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		panic(err)
	}
	fmt.Printf("query 1 outputs %s\n", encodedOutput)
	return utils.CompressData(encodedOutput), nil
}

type SdSerde struct {
	encoder processor.Encoder
	decoder processor.Decoder
}

func SpikeDetection(ctx context.Context, env types.Environment,
	input *common.QueryInput, output chan *common.FnOutput) {
	inputStream, err := sharedlog_stream.NewSharedLogStream(ctx, env, input.InputTopicName)
	if err != nil {
		output <- &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("NewSharedlogStream for input stream failed: %v", err),
		}
		return
	}
	/*
		outputStream, err := sharedlog_stream.NewSharedLogStream(ctx, env, input.OutputTopicName)
		if err != nil {
			output <- &common.FnOutput{
				Success: false,
				Message: fmt.Sprintf("NewSharedlogStream for output stream failed: %v", err),
			}
			return
		}
	*/
	// var msgEncoder processor.MsgEncoder
	var msgDecoder processor.MsgDecoder
	var sdSerde processor.Serde
	if input.SerdeFormat == uint8(common.JSON) {
		sdSerde = SensorDataJSONSerde{}
		// msgEncoder = common.MessageSerializedJSONEncoder{}
		msgDecoder = common.MessageSerializedJSONDecoder{}
	} else if input.SerdeFormat == uint8(common.MSGP) {
		// msgEncoder = common.MessageSerializedMsgpEncoder{}
		msgDecoder = common.MessageSerializedMsgpDecoder{}
		sdSerde = SensorDataMsgpSerde{}
	} else {
		output <- &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("serde format should be either json or msgp; but %v is given", input.SerdeFormat),
		}
	}
	builder := stream.NewStreamBuilder()
	builder.Source("spike-detection-src", sharedlog_stream.NewSharedLogStreamSource(inputStream,
		int(input.Duration), processor.StringDecoder{}, sdSerde, msgDecoder))
}
