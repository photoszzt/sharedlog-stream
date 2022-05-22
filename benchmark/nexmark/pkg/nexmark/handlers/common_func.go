package handlers

import (
	"context"
	"fmt"
	"os"
	"sharedlog-stream/benchmark/common"
	ntypes "sharedlog-stream/benchmark/nexmark/pkg/nexmark/types"
	"sync"
	"time"

	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/errors"
	"sharedlog-stream/pkg/hash"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/source_sink"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/transaction"
	"sharedlog-stream/pkg/transaction/tran_interface"

	"golang.org/x/xerrors"
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

/*
func getPersonTimeSerde(serdeFormat uint8) (commtypes.Serde, error) {
	if serdeFormat == uint8(commtypes.JSON) {
		return ntypes.PersonTimeJSONSerde{}, nil
	} else if serdeFormat == uint8(commtypes.MSGP) {
		return ntypes.PersonTimeMsgpSerde{}, nil
	} else {
		return nil, fmt.Errorf("serde format should be either json or msgp; but %v is given", serdeFormat)
	}
}
*/

type JoinWorkerFunc func(c context.Context, m commtypes.Message) ([]commtypes.Message, error)

type joinProcArgs struct {
	src          *source_sink.MeteredSource
	sink         *source_sink.ConcurrentMeteredSyncSink
	trackParFunc tran_interface.TrackKeySubStreamFunc

	runner  JoinWorkerFunc
	cHashMu *sync.RWMutex
	cHash   *hash.ConsistentHash
	parNum  uint8
}

type JoinProcManager struct {
	out  chan *common.FnOutput
	run  chan struct{}
	done chan struct{}
}

func NewJoinProcManager() *JoinProcManager {
	out := make(chan *common.FnOutput, 1)
	run := make(chan struct{})
	return &JoinProcManager{
		out: out,
		run: run,
	}
}

func (jm *JoinProcManager) Out() <-chan *common.FnOutput {
	return jm.out
}

func (jm *JoinProcManager) LaunchJoinProcLoop(
	ctx context.Context,
	task *transaction.StreamTask,
	procArgs *joinProcArgs,
	wg *sync.WaitGroup,
) {
	done := make(chan struct{})
	jm.done = done
	wg.Add(1)
	go joinProcLoop(ctx, jm.out, task, procArgs, wg, jm.run, done)
}

func (jm *JoinProcManager) Run() {
	jm.run <- struct{}{}
}

func (jm *JoinProcManager) RequestToTerminate() {
	close(jm.done)
}

func joinProcLoop(
	ctx context.Context,
	out chan *common.FnOutput,
	task *transaction.StreamTask,
	procArgs *joinProcArgs,
	wg *sync.WaitGroup,
	run chan struct{},
	done chan struct{},
) {
	defer wg.Done()
	<-run
	debug.Fprintf(os.Stderr, "joinProc start running\n")
	id := ctx.Value("id").(string)
	for {
		select {
		case <-ctx.Done():
			return
		case <-done:
			debug.Fprintf(os.Stderr, "got done msg\n")
			return
		default:
		}
		debug.Fprintf(os.Stderr, "before consume\n")
		gotMsgs, err := procArgs.src.Consume(ctx, procArgs.parNum)
		if err != nil {
			if xerrors.Is(err, errors.ErrStreamSourceTimeout) {
				debug.Fprintf(os.Stderr, "[TIMEOUT]%s %s timeout, out chan len: %d\n",
					id, procArgs.src.TopicName(), len(out))
				out <- &common.FnOutput{Success: true, Message: err.Error()}
				debug.Fprintf(os.Stderr, "%s done sending msg\n", id)
				return
			}
			debug.Fprintf(os.Stderr, "[ERROR] consume: %v, out chan len: %d\n", err, len(out))
			out <- &common.FnOutput{Success: false, Message: err.Error()}
			debug.Fprintf(os.Stderr, "%s done sending msg2\n", id)
			return
		}
		debug.Fprintf(os.Stderr, "after consume\n")
		for _, msg := range gotMsgs.Msgs {
			task.OffMu.Lock()
			task.CurrentOffset[procArgs.src.TopicName()] = msg.LogSeqNum
			task.OffMu.Unlock()

			if msg.MsgArr != nil {
				debug.Fprintf(os.Stderr, "got msgarr\n")
				for _, subMsg := range msg.MsgArr {
					if subMsg.Value == nil {
						continue
					}
					debug.Fprintf(os.Stderr, "before proc msg with sink1\n")
					err = procMsgWithSink(ctx, subMsg, procArgs)
					if err != nil {
						debug.Fprintf(os.Stderr, "[ERROR] %s progMsgWithSink: %v, out chan len: %d\n", id, err, len(out))
						out <- &common.FnOutput{Success: false, Message: err.Error()}
						debug.Fprintf(os.Stderr, "%s done send msg3\n", id)
						return
					}
				}
			} else {
				debug.Fprintf(os.Stderr, "got single msg\n")
				if msg.Msg.Value == nil {
					continue
				}
				err = procMsgWithSink(ctx, msg.Msg, procArgs)
				if err != nil {
					debug.Fprintf(os.Stderr, "[ERROR] %s progMsgWithSink2: %v, out chan len: %d\n", id, err, len(out))
					out <- &common.FnOutput{Success: false, Message: err.Error()}
					debug.Fprintf(os.Stderr, "%s done send msg4\n", id)
					return
				}
			}
		}
		debug.Fprintf(os.Stderr, "after for loop\n")
	}
}

func procMsgWithSink(ctx context.Context, msg commtypes.Message, procArgs *joinProcArgs,
) error {
	st := msg.Value.(commtypes.StreamTimeExtractor)
	ts, err := st.ExtractStreamTime()
	if err != nil {
		debug.Fprintf(os.Stderr, "[ERROR] %s return extract ts: %v\n", ctx.Value("id"), err)
		return fmt.Errorf("fail to extract timestamp: %v", err)
	}
	msg.Timestamp = ts
	msgs, err := procArgs.runner(ctx, msg)
	if err != nil {
		debug.Fprintf(os.Stderr, "[ERROR] %s return runner: %v\n", ctx.Value("id"), err)
		return err
	}
	err = pushMsgsToSink(ctx, procArgs.sink, procArgs.cHash, procArgs.cHashMu, msgs, procArgs.trackParFunc)
	if err != nil {
		debug.Fprintf(os.Stderr, "[ERROR] %s return push to sink: %v\n", ctx.Value("id"), err)
		return err
	}
	return nil
}

/*
func joinProcWithoutSink(
	ctx context.Context,
	out chan *common.FnOutput,
	procArgs *joinProcArgs,
) {
	defer procArgs.wg.Done()
	gotMsgs, err := procArgs.src.Consume(ctx, procArgs.parNum)
	if err != nil {
		if xerrors.Is(err, errors.ErrStreamSourceTimeout) {
			debug.Fprintf(os.Stderr, "%s timeout, gen output\n", procArgs.src.TopicName())
			out <- &common.FnOutput{Success: true, Message: err.Error()}
			return
		}
		out <- &common.FnOutput{Success: false, Message: err.Error()}
		return
	}
	for _, msg := range gotMsgs {
		procArgs.offMu.Lock()
		procArgs.currentOffset[procArgs.src.TopicName()] = msg.LogSeqNum
		procArgs.offMu.Unlock()

		st := msg.Msg.Value.(commtypes.StreamTimeExtractor)
		ts, err := st.ExtractStreamTime()
		if err != nil {
			out <- &common.FnOutput{Success: false, Message: fmt.Sprintf("fail to extract timestamp: %v", err)}
			return
		}
		msg.Msg.Timestamp = ts
		_, err = procArgs.runner(ctx, msg.Msg)
		if err != nil {
			out <- &common.FnOutput{Success: false, Message: err.Error()}
		}
	}
	out <- nil
}

func joinProcSerial(
	ctx context.Context,
	procArgs *joinProcArgs,
) *common.FnOutput {
	gotMsgs, err := procArgs.src.Consume(ctx, procArgs.parNum)
	if err != nil {
		if xerrors.Is(err, errors.ErrStreamSourceTimeout) {
			debug.Fprintf(os.Stderr, "%s timeout, gen output\n", procArgs.src.TopicName())
			return &common.FnOutput{Success: true, Message: err.Error()}
		}
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	for _, msg := range gotMsgs {
		procArgs.offMu.Lock()
		procArgs.currentOffset[procArgs.src.TopicName()] = msg.LogSeqNum
		procArgs.offMu.Unlock()

		st := msg.Msg.Value.(commtypes.StreamTimeExtractor)
		ts, err := st.ExtractStreamTime()
		if err != nil {
			return &common.FnOutput{Success: false, Message: fmt.Sprintf("fail to extract timestamp: %v", err)}
		}
		msg.Msg.Timestamp = ts
		msgs, err := procArgs.runner(ctx, msg.Msg)
		if err != nil {
			return &common.FnOutput{Success: false, Message: err.Error()}
		}
		err = pushMsgsToSink(ctx, procArgs.sink, procArgs.cHash, procArgs.cHashMu, msgs, procArgs.trackParFunc)
		if err != nil {
			return &common.FnOutput{Success: false, Message: err.Error()}
		}
	}
	return nil
}
*/

type joinProcWithoutSinkArgs struct {
	src    source_sink.Source
	runner JoinWorkerFunc
	parNum uint8
}

func joinProcSerialWithoutSink(
	ctx context.Context,
	procArgsTmp interface{},
) error {
	procArgs := procArgsTmp.(*joinProcWithoutSinkArgs)
	gotMsgs, err := procArgs.src.Consume(ctx, procArgs.parNum)
	if err != nil {
		if xerrors.Is(err, errors.ErrStreamSourceTimeout) {
			debug.Fprintf(os.Stderr, "%s timeout, gen output\n", procArgs.src.TopicName())
			return err
		}
		return err
	}
	for _, msg := range gotMsgs.Msgs {
		if msg.MsgArr != nil {
			for _, subMsg := range msg.MsgArr {
				err = procMsg(ctx, subMsg, procArgs)
				if err != nil {
					return err
				}
			}
		} else {
			err = procMsg(ctx, msg.Msg, procArgs)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func procMsg(ctx context.Context, msg commtypes.Message, procArgs *joinProcWithoutSinkArgs) error {
	st := msg.Value.(commtypes.StreamTimeExtractor)
	ts, err := st.ExtractStreamTime()
	if err != nil {
		return fmt.Errorf("fail to extract timestamp: %v", err)
	}
	msg.Timestamp = ts
	_, err = procArgs.runner(ctx, msg)
	if err != nil {
		return err
	}
	return nil
}

func pushMsgsToSink(
	ctx context.Context,
	sink *source_sink.ConcurrentMeteredSyncSink,
	cHash *hash.ConsistentHash,
	cHashMu *sync.RWMutex,
	msgs []commtypes.Message,
	trackParFunc tran_interface.TrackKeySubStreamFunc,
) error {
	for _, msg := range msgs {
		key := msg.Key.(uint64)
		cHashMu.RLock()
		parTmp, ok := cHash.Get(key)
		cHashMu.RUnlock()
		if !ok {
			return fmt.Errorf("fail to calculate partition")
		}
		par := parTmp.(uint8)
		err := trackParFunc(ctx, key, sink.KeySerde(), sink.TopicName(), par)
		if err != nil {
			return fmt.Errorf("add topic partition failed: %v", err)
		}
		err = sink.Produce(ctx, msg, par, false)
		if err != nil {
			return err
		}
	}
	return nil
}

func getSrcSink(ctx context.Context, sp *common.QueryInput,
	input_stream *sharedlog_stream.ShardedSharedLogStream,
	output_stream *sharedlog_stream.ShardedSharedLogStream,
) (*source_sink.MeteredSource,
	*source_sink.MeteredSyncSink,
	error) {
	msgSerde, err := commtypes.GetMsgSerde(sp.SerdeFormat)
	if err != nil {
		return nil, nil, fmt.Errorf("get msg serde failed: %v", err)
	}
	eventSerde, err := getEventSerde(sp.SerdeFormat)
	if err != nil {
		return nil, nil, err
	}
	inConfig := &source_sink.StreamSourceConfig{
		Timeout: common.SrcConsumeTimeout,
		KVMsgSerdes: commtypes.KVMsgSerdes{
			KeySerde: commtypes.StringSerde{},
			ValSerde: eventSerde,
			MsgSerde: msgSerde,
		},
	}
	outConfig := &source_sink.StreamSinkConfig{
		KVMsgSerdes: commtypes.KVMsgSerdes{
			KeySerde: commtypes.StringSerde{},
			ValSerde: eventSerde,
			MsgSerde: msgSerde,
		},
		FlushDuration: time.Duration(sp.FlushMs) * time.Millisecond,
	}
	src := source_sink.NewMeteredSource(source_sink.NewShardedSharedLogStreamSource(input_stream, inConfig), time.Duration(sp.WarmupS)*time.Second)
	sink := source_sink.NewMeteredSyncSink(source_sink.NewShardedSharedLogStreamSyncSink(output_stream, outConfig), time.Duration(sp.WarmupS)*time.Second)
	return src, sink, nil
}

func getSrcSinkUint64Key(
	ctx context.Context,
	sp *common.QueryInput,
	input_stream *sharedlog_stream.ShardedSharedLogStream,
	output_stream *sharedlog_stream.ShardedSharedLogStream,
) (*source_sink.MeteredSource,
	*source_sink.MeteredSyncSink,
	error,
) {
	msgSerde, err := commtypes.GetMsgSerde(sp.SerdeFormat)
	if err != nil {
		return nil, nil, err
	}
	eventSerde, err := getEventSerde(sp.SerdeFormat)
	if err != nil {
		return nil, nil, err
	}
	inConfig := &source_sink.StreamSourceConfig{
		Timeout: common.SrcConsumeTimeout,
		KVMsgSerdes: commtypes.KVMsgSerdes{
			KeySerde: commtypes.StringSerde{},
			ValSerde: eventSerde,
			MsgSerde: msgSerde,
		},
	}
	outConfig := &source_sink.StreamSinkConfig{
		KVMsgSerdes: commtypes.KVMsgSerdes{
			MsgSerde: msgSerde,
			ValSerde: eventSerde,
			KeySerde: commtypes.Uint64Serde{},
		},
		FlushDuration: time.Duration(sp.FlushMs) * time.Millisecond,
	}

	src := source_sink.NewMeteredSource(source_sink.NewShardedSharedLogStreamSource(input_stream, inConfig),
		time.Duration(sp.WarmupS)*time.Second)
	sink := source_sink.NewMeteredSyncSink(source_sink.NewShardedSharedLogStreamSyncSink(output_stream, outConfig),
		time.Duration(sp.WarmupS)*time.Second)
	return src, sink, nil
}

func CommonGetSrcSink(ctx context.Context, sp *common.QueryInput,
	input_stream *sharedlog_stream.ShardedSharedLogStream,
	output_stream *sharedlog_stream.ShardedSharedLogStream,
) (*source_sink.MeteredSource, *source_sink.MeteredSink, error) {
	msgSerde, err := commtypes.GetMsgSerde(sp.SerdeFormat)
	if err != nil {
		return nil, nil, fmt.Errorf("get msg serde err: %v", err)
	}
	eventSerde, err := getEventSerde(sp.SerdeFormat)
	if err != nil {
		return nil, nil, fmt.Errorf("get event serde err: %v", err)
	}
	inConfig := &source_sink.StreamSourceConfig{
		Timeout: common.SrcConsumeTimeout,
		KVMsgSerdes: commtypes.KVMsgSerdes{
			KeySerde: commtypes.StringSerde{},
			ValSerde: eventSerde,
			MsgSerde: msgSerde,
		},
	}
	outConfig := &source_sink.StreamSinkConfig{
		KVMsgSerdes: commtypes.KVMsgSerdes{
			KeySerde: commtypes.Uint64Serde{},
			ValSerde: eventSerde,
			MsgSerde: msgSerde,
		},
		FlushDuration: time.Duration(sp.FlushMs) * time.Millisecond,
	}
	// fmt.Fprintf(os.Stderr, "output to %v\n", output_stream.TopicName())
	src := source_sink.NewMeteredSource(source_sink.NewShardedSharedLogStreamSource(input_stream, inConfig),
		time.Duration(sp.WarmupS)*time.Second)
	sink := source_sink.NewMeteredSink(source_sink.NewShardedSharedLogStreamSink(output_stream, outConfig),
		time.Duration(sp.WarmupS)*time.Second)
	return src, sink, nil
}
