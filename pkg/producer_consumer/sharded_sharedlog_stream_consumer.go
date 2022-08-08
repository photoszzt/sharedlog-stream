package producer_consumer

import (
	"context"
	"fmt"
	"os"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/data_structure"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/exactly_once_intr"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/txn_data"
	"sync/atomic"
	"time"
)

type ScaleEpochAndBytes struct {
	Payload    []byte
	ScaleEpoch uint16
}

type StartTimeAndProdIdx struct {
	StartTime int64
	ProdIdx   uint8
}

type StreamConsumerConfigG[K, V any] struct {
	MsgSerde    commtypes.MessageSerdeG[K, V]
	Timeout     time.Duration
	SerdeFormat commtypes.SerdeFormat
}

type StreamConsumerConfig struct {
	MsgSerde    commtypes.MessageSerde
	Timeout     time.Duration
	SerdeFormat commtypes.SerdeFormat
}

type ShardedSharedLogStreamConsumer[K, V any] struct {
	epochMarkerSerde commtypes.SerdeG[commtypes.EpochMarker]
	payloadArrSerde  commtypes.SerdeG[commtypes.PayloadArr]
	msgSerde         commtypes.MessageSerdeG[K, V]
	producerNotEnd   data_structure.Uint8Set
	stream           *sharedlog_stream.ShardedSharedLogStream
	tac              *TransactionAwareConsumer
	emc              *EpochMarkConsumer
	name             string
	currentSeqNum    uint64
	timeout          time.Duration
	guarantee        exactly_once_intr.GuaranteeMth
	initialSource    bool
	serdeFormat      commtypes.SerdeFormat
	numSrcProducer   uint8
}

var _ = Consumer(&ShardedSharedLogStreamConsumer[int, string]{})

func NewShardedSharedLogStreamConsumerG[K, V any](stream *sharedlog_stream.ShardedSharedLogStream,
	config *StreamConsumerConfigG[K, V], numSrcProducer uint8, instanceId uint8,
) (*ShardedSharedLogStreamConsumer[K, V], error) {
	// debug.Fprintf(os.Stderr, "%s timeout: %v\n", stream.TopicName(), config.Timeout)
	epochMarkSerde, err := commtypes.GetEpochMarkerSerdeG(config.SerdeFormat)
	if err != nil {
		return nil, err
	}
	notEnded := make(data_structure.Uint8Set)
	if numSrcProducer != 1 {
		for i := uint8(0); i < numSrcProducer; i++ {
			notEnded.Add(i)
		}
	} else {
		notEnded.Add(instanceId)
	}
	return &ShardedSharedLogStreamConsumer[K, V]{
		epochMarkerSerde: epochMarkSerde,
		stream:           stream,
		timeout:          config.Timeout,
		msgSerde:         config.MsgSerde,
		name:             "src",
		payloadArrSerde:  sharedlog_stream.DEFAULT_PAYLOAD_ARR_SERDEG,
		guarantee:        exactly_once_intr.AT_LEAST_ONCE,
		serdeFormat:      config.SerdeFormat,
		numSrcProducer:   numSrcProducer,
		producerNotEnd:   notEnded,
	}, nil
}

func (s *ShardedSharedLogStreamConsumer[K, V]) SetInitialSource(initial bool) {
	s.initialSource = initial
}
func (s *ShardedSharedLogStreamConsumer[K, V]) IsInitialSource() bool {
	return s.initialSource
}

func (s *ShardedSharedLogStreamConsumer[K, V]) NumSubstreamProducer() uint8 {
	return s.numSrcProducer
}

func (s *ShardedSharedLogStreamConsumer[K, V]) SrcProducerEnd(prodIdx uint8) {
	s.producerNotEnd.Remove(prodIdx)
	debug.Fprintf(os.Stderr, "%d producer ended, %v remain\n", prodIdx, s.producerNotEnd)
}

func (s *ShardedSharedLogStreamConsumer[K, V]) AllProducerEnded() bool {
	return len(s.producerNotEnd) == 0
}

func (s *ShardedSharedLogStreamConsumer[K, V]) ConfigExactlyOnce(
	guarantee exactly_once_intr.GuaranteeMth,
) error {
	debug.Assert(guarantee == exactly_once_intr.TWO_PHASE_COMMIT || guarantee == exactly_once_intr.EPOCH_MARK,
		"configure exactly once should specify 2pc or epoch mark")
	s.guarantee = guarantee
	var err error = nil
	if s.guarantee == exactly_once_intr.TWO_PHASE_COMMIT {
		s.tac, err = NewTransactionAwareConsumer(s.stream, s.serdeFormat)
	} else if s.guarantee == exactly_once_intr.EPOCH_MARK {
		s.emc, err = NewEpochMarkConsumer(s.stream, s.serdeFormat)
	}
	return err
}

func (s *ShardedSharedLogStreamConsumer[K, V]) Stream() sharedlog_stream.Stream {
	return s.stream
}

func (s *ShardedSharedLogStreamConsumer[K, V]) SetName(name string) {
	s.name = name
}

func (s *ShardedSharedLogStreamConsumer[K, V]) Name() string {
	return s.name
}

func (s *ShardedSharedLogStreamConsumer[K, V]) MsgSerde() commtypes.MessageSerdeG[K, V] {
	return s.msgSerde
}

func (s *ShardedSharedLogStreamConsumer[K, V]) TopicName() string {
	return s.stream.TopicName()
}

func (s *ShardedSharedLogStreamConsumer[K, V]) SetCursor(cursor uint64, parNum uint8) {
	s.stream.SetCursor(cursor, parNum)
}

func (s *ShardedSharedLogStreamConsumer[K, V]) readNext(ctx context.Context, parNum uint8) (*commtypes.RawMsg, error) {
	if s.guarantee == exactly_once_intr.TWO_PHASE_COMMIT {
		return s.tac.ReadNext(ctx, parNum)
	} else if s.guarantee == exactly_once_intr.EPOCH_MARK {
		return s.emc.ReadNext(ctx, parNum)
	} else {
		rawMsg, err := s.stream.ReadNext(ctx, parNum)
		if err != nil {
			return nil, err
		}
		if rawMsg.IsControl {
			epochMark, err := s.epochMarkerSerde.Decode(rawMsg.Payload)
			if err != nil {
				return nil, err
			}
			// debug.Fprintf(os.Stderr, "%+v\n", epochMark)
			rawMsg.Mark = epochMark.Mark
			if epochMark.Mark == commtypes.SCALE_FENCE {
				rawMsg.ScaleEpoch = epochMark.ScaleEpoch
			} else if epochMark.Mark == commtypes.STREAM_END {
				rawMsg.StartTime = epochMark.StartTime
				rawMsg.ProdIdx = epochMark.ProdIndex
			}
		}
		return rawMsg, err
	}
}

func (s *ShardedSharedLogStreamConsumer[K, V]) RecordCurrentConsumedSeqNum(seqNum uint64) {
	atomic.StoreUint64(&s.currentSeqNum, seqNum)
}

func (s *ShardedSharedLogStreamConsumer[K, V]) CurrentConsumedSeqNum() uint64 {
	return atomic.LoadUint64(&s.currentSeqNum)
}

func (s *ShardedSharedLogStreamConsumer[K, V]) Consume(ctx context.Context, parNum uint8) (*commtypes.MsgAndSeqs, error) {
	startTime := time.Now()
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

		if rawMsg.IsControl {
			if rawMsg.Mark == commtypes.SCALE_FENCE {
				msgs := &commtypes.MsgAndSeq{
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
				}
				return &commtypes.MsgAndSeqs{Msgs: msgs, TotalLen: 1}, nil
			} else if rawMsg.Mark == commtypes.STREAM_END {
				// debug.Fprintf(os.Stderr, "got stream end in consume %s %d\n", s.stream.TopicName(), parNum)
				msgs := &commtypes.MsgAndSeq{
					Msg: commtypes.Message{
						Key:   commtypes.END_OF_STREAM_KEY,
						Value: StartTimeAndProdIdx{StartTime: rawMsg.StartTime, ProdIdx: rawMsg.ProdIdx},
					},
					MsgArr:    nil,
					MsgSeqNum: rawMsg.MsgSeqNum,
					LogSeqNum: rawMsg.LogSeqNum,
					IsControl: true,
				}
				return &commtypes.MsgAndSeqs{Msgs: msgs, TotalLen: 1}, nil
			} else if rawMsg.Mark != commtypes.ABORT && rawMsg.Mark != commtypes.EPOCH_END {
				return nil, fmt.Errorf("unrecognized mark: %v", rawMsg.Mark)
			}
		} else if len(rawMsg.Payload) == 0 {
			continue
		} else {
			totalLen := uint32(0)
			msgAndSeq, err := commtypes.DecodeRawMsgG(rawMsg, s.msgSerde, s.payloadArrSerde)
			if err != nil {
				return nil, fmt.Errorf("fail to decode raw msg: %v", err)
			}
			if msgAndSeq.MsgArr != nil {
				totalLen += uint32(len(msgAndSeq.MsgArr))
			} else {
				totalLen += 1
			}
			return &commtypes.MsgAndSeqs{Msgs: msgAndSeq, TotalLen: totalLen}, nil
		}
	}
	return nil, common_errors.ErrStreamSourceTimeout
}
