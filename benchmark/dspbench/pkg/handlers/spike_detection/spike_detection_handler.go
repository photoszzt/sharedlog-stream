package spike_detection

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/stream"
	"sharedlog-stream/pkg/stream/processor"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/stream/processor/store"
	"time"

	"cs.utexas.edu/zjia/faas/types"
)

const (
	SPIKE_THRESHOLD = 0.03
)

type spikeDetectionHandler struct {
	env types.Environment
}

func spikeDetectionPredicate(msg *commtypes.Message) (bool, error) {
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
	return utils.CompressData(encodedOutput), nil
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
	changeLog, err := sharedlog_stream.NewLogStore(ctx, env, "moving-avg-log")
	if err != nil {
		output <- &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("NewSharedlogStream for input stream failed: %v", err),
		}
		return
	}

	outputStream, err := sharedlog_stream.NewSharedLogStream(ctx, env, input.OutputTopicName)
	if err != nil {
		output <- &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("NewSharedlogStream for output stream failed: %v", err),
		}
		return
	}

	var msgSerde commtypes.MsgSerde
	var sdSerde commtypes.Serde
	var timeValSerde commtypes.Serde
	var vaSerde commtypes.Serde
	if input.SerdeFormat == uint8(commtypes.JSON) {
		sdSerde = SensorDataJSONSerde{}
		msgSerde = commtypes.MessageSerializedJSONSerde{}
		timeValSerde = commtypes.ValueTimestampJSONSerde{
			ValJSONSerde: SumAndHistJSONSerde{},
		}
	} else if input.SerdeFormat == uint8(commtypes.MSGP) {
		msgSerde = commtypes.MessageSerializedMsgpSerde{}
		sdSerde = SensorDataMsgpSerde{}
		timeValSerde = commtypes.ValueTimestampMsgpSerde{
			ValMsgpSerde: SumAndHistMsgpSerde{},
		}
	} else {
		output <- &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("serde format should be either json or msgp; but %v is given", input.SerdeFormat),
		}
	}
	movingAverageWindow := 1000

	pctx := store.NewProcessorContext()
	aggStoreName := "moving-avg-store"

	inConfig := &sharedlog_stream.SharedLogStreamConfig{
		Timeout:      time.Duration(input.Duration) * time.Second,
		KeyDecoder:   commtypes.StringDecoder{},
		ValueDecoder: sdSerde,
		MsgDecoder:   msgSerde,
	}
	outConfig := &sharedlog_stream.StreamSinkConfig{
		KeyEncoder:   commtypes.StringEncoder{},
		ValueEncoder: vaSerde,
		MsgEncoder:   msgSerde,
	}
	builder := stream.NewStreamBuilder()
	builder.Source("spike-detection-src",
		sharedlog_stream.NewSharedLogStreamSource(inputStream, inConfig)).
		GroupByKey(&stream.Grouped{KeySerde: commtypes.StringSerde{},
			ValueSerde: sdSerde, Name: "group-by-devid"}).
		Aggregate("moving-avg",
			&store.MaterializeParam{
				KeySerde:   commtypes.StringSerde{},
				ValueSerde: timeValSerde,
				MsgSerde:   msgSerde,
				StoreName:  aggStoreName,
				Changelog:  changeLog,
			},
			processor.InitializerFunc(func() interface{} {
				return &SumAndHist{
					Sum:     0,
					History: make([]float64, movingAverageWindow),
				}
			}),
			processor.AggregatorFunc(func(key interface{}, value interface{}, agg interface{}) interface{} {
				nextVal := value.(*SensorData)
				aggVal := agg.(*SumAndHist)
				var newHist []float64
				newSum := aggVal.Sum
				if len(aggVal.History) > movingAverageWindow-1 {
					valToRemove := aggVal.History[0]
					newHist = aggVal.History[1:]
					newSum -= valToRemove
				}
				newHist = append(newHist, nextVal.Val)
				newSum += nextVal.Val
				return &SumAndHist{
					Sum:     newSum,
					History: newHist,
				}
			})).
		MapValues("calc-avg", processor.ValueMapperFunc(func(value interface{}) (interface{}, error) {
			val := value.(*SumAndHist)
			return ValAndAvg{
				Val: val.Val,
				Avg: val.Sum / float64(len(val.History)),
			}, nil
		}), "").
		Filter("get-spike", processor.PredicateFunc(spikeDetectionPredicate), "").
		Process("spike-detection-sink", sharedlog_stream.NewSharedLogStreamSink(outputStream, outConfig))

	tp, err_arrs := builder.Build()
	if err_arrs != nil {
		output <- &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("build stream failed: %v", err_arrs),
		}
	}
	pumps := make(map[processor.Node]processor.Pump)
	var srcPumps []processor.SourcePump
	nodes := processor.FlattenNodeTree(tp.Sources())
	processor.ReverseNodes(nodes)

	for _, node := range nodes {
		pipe := processor.NewPipe(processor.ResolvePumps(pumps, node.Children()))
		node.Processor().WithPipe(pipe)
		node.Processor().WithProcessorContext(pctx)
		pump := processor.NewSyncPump(node, pipe)
		pumps[node] = pump
	}
	for source, node := range tp.Sources() {
		srcPump := processor.NewSourcePump(node.Name(), source, 0,
			processor.ResolvePumps(pumps, node.Children()), func(err error) {
				log.Fatal(err.Error())
			})
		srcPumps = append(srcPumps, srcPump)
	}

	duration := time.Duration(input.Duration) * time.Second
	latencies := make([]int, 0, 128)
	startTime := time.Now()
	<-time.After(duration)
	for _, srcPump := range srcPumps {
		srcPump.Stop()
		srcPump.Close()
	}

	output <- &common.FnOutput{
		Success:   true,
		Duration:  time.Since(startTime).Seconds(),
		Latencies: map[string][]int{"e2e": latencies},
	}

}
