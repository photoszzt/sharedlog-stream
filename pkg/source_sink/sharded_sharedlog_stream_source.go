package source_sink

import (
	"context"
	"os"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/txn_data"
	"time"
)

type ScaleEpochAndBytes struct {
	Payload    []uint8
	ScaleEpoch uint64
}

type StreamSourceConfig struct {
	KVMsgSerdes commtypes.KVMsgSerdes
	Timeout     time.Duration
}

type ShardedSharedLogStreamSource struct {
	kvmsgSerdes     commtypes.KVMsgSerdes
	payloadArrSerde commtypes.Serde
	stream          *sharedlog_stream.ShardedSharedLogStream
	tac             *TransactionAwareConsumer
	name            string
	timeout         time.Duration
	transactional   bool
	initialSource   bool
}

var _ = Source(&ShardedSharedLogStreamSource{})

func NewShardedSharedLogStreamSource(stream *sharedlog_stream.ShardedSharedLogStream, config *StreamSourceConfig) *ShardedSharedLogStreamSource {
	debug.Fprintf(os.Stderr, "%s timeout: %v\n", stream.TopicName(), config.Timeout)
	return &ShardedSharedLogStreamSource{
		stream:          stream,
		timeout:         config.Timeout,
		kvmsgSerdes:     config.KVMsgSerdes,
		name:            "src",
		payloadArrSerde: sharedlog_stream.DEFAULT_PAYLOAD_ARR_SERDE,
	}
}

func (s *ShardedSharedLogStreamSource) SetInitialSource(initial bool) {
	s.initialSource = initial
}
func (s *ShardedSharedLogStreamSource) IsInitialSource() bool {
	return s.initialSource
}

func (s *ShardedSharedLogStreamSource) InTransaction(serdeFormat commtypes.SerdeFormat) error {
	s.transactional = true
	var err error = nil
	s.tac, err = NewTransactionAwareConsumer(s.stream, serdeFormat)
	return err
}

func (s *ShardedSharedLogStreamSource) Stream() sharedlog_stream.Stream {
	return s.stream
}

func (s *ShardedSharedLogStreamSource) SetName(name string) {
	s.name = name
}

func (s *ShardedSharedLogStreamSource) Name() string {
	return s.name
}

func (s *ShardedSharedLogStreamSource) KVMsgSerdes() commtypes.KVMsgSerdes {
	return s.kvmsgSerdes
}

func (s *ShardedSharedLogStreamSource) TopicName() string {
	return s.stream.TopicName()
}

func (s *ShardedSharedLogStreamSource) SetCursor(cursor uint64, parNum uint8) {
	s.stream.SetCursor(cursor, parNum)
}

func (s *ShardedSharedLogStreamSource) readNext(ctx context.Context, parNum uint8) (*commtypes.RawMsg, error) {
	if s.transactional {
		return s.tac.ReadNext(ctx, parNum)
	} else {
		return s.stream.ReadNext(ctx, parNum)
	}
}

func (s *ShardedSharedLogStreamSource) Consume(ctx context.Context, parNum uint8) (*commtypes.MsgAndSeqs, error) {
	startTime := time.Now()
	var msgs []commtypes.MsgAndSeq
	totalLen := uint32(0)
L:
	for {
		select {
		case <-ctx.Done():
			break L
		default:
		}
		duration := time.Since(startTime)
		// debug.Fprintf(os.Stderr, "consume dur: %v\n", duration)
		if s.timeout != 0 && duration >= s.timeout {
			break
		}
		rawMsg, err := s.readNext(ctx, parNum)
		if err != nil {
			if common_errors.IsStreamEmptyError(err) {
				// debug.Fprintf(os.Stderr, "stream is empty\n")
				time.Sleep(time.Duration(100) * time.Microsecond)
				continue
			} else if common_errors.IsStreamTimeoutError(err) {
				// debug.Fprintf(os.Stderr, "stream time out\n")
				continue
			} else {
				return nil, err
			}
		}

		if !rawMsg.IsControl && len(rawMsg.Payload) == 0 {
			continue
		}
		if rawMsg.IsControl {
			msgs = append(msgs, commtypes.MsgAndSeq{
				Msg: commtypes.Message{
					Key: txn_data.SCALE_FENCE_KEY,
					Value: ScaleEpochAndBytes{
						ScaleEpoch: rawMsg.ScaleEpoch,
						Payload:    rawMsg.Payload,
					},
				},
				MsgArr:    nil,
				MsgSeqNum: rawMsg.MsgSeqNum,
				LogSeqNum: rawMsg.LogSeqNum,
				IsControl: true,
			})
			totalLen += 1
			continue
		}
		msgAndSeq, err := commtypes.DecodeRawMsg(
			rawMsg, s.kvmsgSerdes, s.payloadArrSerde,
			commtypes.DecodeMsg,
		)
		if err != nil {
			return nil, err
		}
		if msgAndSeq.MsgArr != nil {
			totalLen += uint32(len(msgAndSeq.MsgArr))
		} else {
			totalLen += 1
		}
		msgs = append(msgs, *msgAndSeq)

		return &commtypes.MsgAndSeqs{Msgs: msgs, TotalLen: totalLen}, nil
	}
	return nil, common_errors.ErrStreamSourceTimeout
}
