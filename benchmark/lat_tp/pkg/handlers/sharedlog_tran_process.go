package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"sharedlog-stream/benchmark/common"
	datatype "sharedlog-stream/benchmark/lat_tp/pkg/data_type"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/exactly_once_intr"
	"sharedlog-stream/pkg/optional"
	"sharedlog-stream/pkg/proc_interface"
	"sharedlog-stream/pkg/processor"
	"sharedlog-stream/pkg/producer_consumer"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/stream_task"
	"time"

	"cs.utexas.edu/zjia/faas/types"
)

type sharedlogTranProcessHandler struct {
	env types.Environment
}

func NewSharedlogTranProcessHandler(env types.Environment) types.FuncHandler {
	return &sharedlogTranProcessHandler{
		env: env,
	}
}

func (h *sharedlogTranProcessHandler) Call(ctx context.Context, input []byte) ([]byte, error) {
	parsedInput := &common.TranProcessBenchParam{}
	err := json.Unmarshal(input, parsedInput)
	if err != nil {
		return nil, err
	}
	output := h.sharedlogTranProcess(ctx, parsedInput)
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		panic(err)
	}
	return common.CompressData(encodedOutput), nil
}

func (h *sharedlogTranProcessHandler) sharedlogTranProcess(ctx context.Context, sp *common.TranProcessBenchParam) *common.FnOutput {
	inStream, err := sharedlog_stream.NewShardedSharedLogStream(h.env, sp.InTopicName, sp.NumPartition, commtypes.SerdeFormat(sp.SerdeFormat))
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	outStream, err := sharedlog_stream.NewShardedSharedLogStream(h.env, sp.OutTopicName, sp.NumPartition, commtypes.SerdeFormat(sp.SerdeFormat))
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	serdeFormat := commtypes.SerdeFormat(sp.SerdeFormat)
	inConfig := &producer_consumer.StreamConsumerConfig{
		Timeout:     common.SrcConsumeTimeout,
		SerdeFormat: serdeFormat,
	}
	outConfig := &producer_consumer.StreamSinkConfig{
		FlushDuration: time.Duration(100) * time.Millisecond,
		Format:        serdeFormat,
	}
	consumer, err := producer_consumer.NewShardedSharedLogStreamConsumer(inStream, inConfig, 1, 0)
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	src := producer_consumer.NewMeteredConsumer(consumer, 0)
	sink, err := producer_consumer.NewMeteredProducer(producer_consumer.NewShardedSharedLogStreamProducer(outStream, outConfig), 0)
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	src.SetInitialSource(true)
	sink.MarkFinalOutput()
	msgSerde, err := commtypes.GetMsgGSerdeG[string, datatype.PayloadTs](commtypes.MSGP, commtypes.StringSerdeG{}, datatype.PayloadTsMsgpSerdeG{})
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	srcs, sinks := []*producer_consumer.MeteredConsumer{src}, []producer_consumer.MeteredProducerIntr{sink}
	ectx := processor.NewExecutionContextFromComponents(proc_interface.NewBaseSrcsSinks(srcs, sinks),
		proc_interface.NewBaseProcArgs("tranProcess", 1, 0))
	outProc := processor.NewFixedSubstreamOutputProcessorG("subG1", sinks[0], 0, msgSerde)
	task := stream_task.NewStreamTaskBuilder().MarkFinalStage().
		AppProcessFunc(func(ctx context.Context, task *stream_task.StreamTask, args processor.ExecutionContext) (*common.FnOutput, optional.Option[commtypes.RawMsgAndSeq]) {
			return stream_task.CommonProcess(ctx, task, &ectx,
				func(ctx context.Context, msg commtypes.MessageG[string, datatype.PayloadTs], argsTmp interface{}) error {
					return outProc.Process(ctx, msg)
				}, msgSerde)
		}).Build()
	streamTaskArgs := stream_task.NewStreamTaskArgsBuilder(h.env, &ectx,
		fmt.Sprintf("tranProcess-%s-%d-%s", sp.InTopicName,
			0, sp.OutTopicName)).
		Guarantee(exactly_once_intr.EPOCH_MARK).
		AppID("tranProcess").
		Warmup(time.Duration(0) * time.Second).
		CommitEveryMs(sp.CommitEveryMs).
		FlushEveryMs(sp.FlushMs).
		Duration(sp.Duration).
		SerdeFormat(commtypes.SerdeFormat(sp.SerdeFormat)).
		WaitEndMark(true).FixedOutParNum(0).Build()
	return stream_task.ExecuteApp(ctx, task, streamTaskArgs, stream_task.EmptySetupSnapshotCallback,
		func() {
			outProc.OutputRemainingStats()
		})
}
