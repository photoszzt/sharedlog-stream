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

/*
type StreamConsumerConfigG[K, V any] struct {
	// MsgSerde    commtypes.MessageSerdeG
	Timeout     time.Duration
	SerdeFormat commtypes.SerdeFormat
}
*/

type StreamConsumerConfig struct {
	// MsgSerde    commtypes.MessageSerde
	Timeout     time.Duration
	SerdeFormat commtypes.SerdeFormat
}

type ShardedSharedLogStreamConsumer struct {
	epochMarkerSerde commtypes.SerdeG[commtypes.EpochMarker]
	payloadArrSerde  commtypes.SerdeG[commtypes.PayloadArr]
	// msgSerde         commtypes.MessageSerdeG
	producerNotEnd        data_structure.Uint8Set
	producerNotScaleFence data_structure.Uint8Set
	stream                *sharedlog_stream.ShardedSharedLogStream
	tac                   *TransactionAwareConsumer
	emc                   *EpochMarkConsumer
	name                  string
	currentSeqNum         uint64
	timeout               time.Duration
	guarantee             exactly_once_intr.GuaranteeMth
	initialSource         bool
	serdeFormat           commtypes.SerdeFormat
	numSrcProducer        uint8
}

var _ = Consumer(&ShardedSharedLogStreamConsumer{})

func NewShardedSharedLogStreamConsumer(stream *sharedlog_stream.ShardedSharedLogStream,
	config *StreamConsumerConfig, numSrcProducer uint8, instanceId uint8,
) (*ShardedSharedLogStreamConsumer, error) {
	// debug.Fprintf(os.Stderr, "%s timeout: %v\n", stream.TopicName(), config.Timeout)
	epochMarkSerde, err := commtypes.GetEpochMarkerSerdeG(config.SerdeFormat)
	if err != nil {
		return nil, err
	}
	notEnded := make(data_structure.Uint8Set)
	notScaleFenced := make(data_structure.Uint8Set)
	if numSrcProducer != 1 {
		for i := uint8(0); i < numSrcProducer; i++ {
			notEnded.Add(i)
			notScaleFenced.Add(i)
		}
	} else {
		notEnded.Add(instanceId)
		notScaleFenced.Add(instanceId)
	}
	fmt.Fprintf(os.Stderr, "%s notEnded: %v, notScaleFence: %v\n", stream.TopicName(), notEnded, notScaleFenced)
	return &ShardedSharedLogStreamConsumer{
		epochMarkerSerde: epochMarkSerde,
		stream:           stream,
		timeout:          config.Timeout,
		// msgSerde:         config.MsgSerde,
		payloadArrSerde:       sharedlog_stream.DEFAULT_PAYLOAD_ARR_SERDEG,
		guarantee:             exactly_once_intr.AT_LEAST_ONCE,
		serdeFormat:           config.SerdeFormat,
		numSrcProducer:        numSrcProducer,
		producerNotEnd:        notEnded,
		producerNotScaleFence: notScaleFenced,
	}, nil
}

func (s *ShardedSharedLogStreamConsumer) SetInitialSource(initial bool) {
	s.initialSource = initial
}
func (s *ShardedSharedLogStreamConsumer) IsInitialSource() bool {
	return s.initialSource
}

func (s *ShardedSharedLogStreamConsumer) NumSubstreamProducer() uint8 {
	return s.numSrcProducer
}

func (s *ShardedSharedLogStreamConsumer) SrcProducerEnd(prodIdx uint8) {
	s.producerNotEnd.Remove(prodIdx)
	fmt.Fprintf(os.Stderr, "%d producer ended, %v remain\n", prodIdx, s.producerNotEnd)
}

func (s *ShardedSharedLogStreamConsumer) SrcProducerGotScaleFence(prodIdx uint8) {
	s.producerNotScaleFence.Remove(prodIdx)
	fmt.Fprintf(os.Stderr, "%d producer got scale fence, %v remain\n", prodIdx, s.producerNotScaleFence)
}

func (s *ShardedSharedLogStreamConsumer) AllProducerEnded() bool {
	return len(s.producerNotEnd) == 0
}

func (s *ShardedSharedLogStreamConsumer) AllProducerScaleFenced() bool {
	return len(s.producerNotScaleFence) == 0
}

func (s *ShardedSharedLogStreamConsumer) ConfigExactlyOnce(
	guarantee exactly_once_intr.GuaranteeMth,
) error {
	debug.Assert(guarantee == exactly_once_intr.TWO_PHASE_COMMIT || guarantee == exactly_once_intr.EPOCH_MARK,
		"configure exactly once should specify 2pc or epoch mark")
	s.guarantee = guarantee
	var err error = nil
	if s.guarantee == exactly_once_intr.TWO_PHASE_COMMIT {
		s.tac, err = NewTransactionAwareConsumer(s.stream, s.serdeFormat)
	} else if s.guarantee == exactly_once_intr.EPOCH_MARK {
		s.emc, err = NewEpochMarkConsumer(s.TopicName(), s.stream, s.serdeFormat)
	}
	return err
}

func (s *ShardedSharedLogStreamConsumer) Stream() sharedlog_stream.Stream {
	return s.stream
}

func (s *ShardedSharedLogStreamConsumer) SetName(name string) {
	s.name = name
}

func (s *ShardedSharedLogStreamConsumer) Name() string {
	return s.name
}

func (s *ShardedSharedLogStreamConsumer) TopicName() string {
	return s.stream.TopicName()
}

func (s *ShardedSharedLogStreamConsumer) SetCursor(cursor uint64, parNum uint8) {
	s.stream.SetCursor(cursor, parNum)
}

func (s *ShardedSharedLogStreamConsumer) readNext(ctx context.Context, parNum uint8) (*commtypes.RawMsg, error) {
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
				rawMsg.ProdIdx = epochMark.ProdIndex
			} else if epochMark.Mark == commtypes.STREAM_END {
				rawMsg.StartTime = epochMark.StartTime
				rawMsg.ProdIdx = epochMark.ProdIndex
			}
		}
		return rawMsg, err
	}
}

func (s *ShardedSharedLogStreamConsumer) RecordCurrentConsumedSeqNum(seqNum uint64) {
	atomic.StoreUint64(&s.currentSeqNum, seqNum)
}

func (s *ShardedSharedLogStreamConsumer) CurrentConsumedSeqNum() uint64 {
	return atomic.LoadUint64(&s.currentSeqNum)
}
func (s *ShardedSharedLogStreamConsumer) Consume(ctx context.Context, parNum uint8) (commtypes.RawMsgAndSeq, error) {
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
				return commtypes.RawMsgAndSeq{}, err
			}
		}
		if len(rawMsg.Payload) == 0 {
			continue
		}
		if rawMsg.IsControl {
			return commtypes.RawMsgAndSeq{
				Payload:    rawMsg.Payload,
				PayloadArr: nil,
				AuxData:    rawMsg.AuxData,
				MsgSeqNum:  rawMsg.MsgSeqNum,
				LogSeqNum:  rawMsg.LogSeqNum,
				IsControl:  true,
				StartTime:  rawMsg.StartTime,
				Mark:       rawMsg.Mark,
				ScaleEpoch: rawMsg.ScaleEpoch,
				ProdIdx:    rawMsg.ProdIdx,
			}, nil
		} else {
			if rawMsg.IsPayloadArr {
				payloadArr, err := s.payloadArrSerde.Decode(rawMsg.Payload)
				if err != nil {
					return commtypes.RawMsgAndSeq{}, fmt.Errorf("fail to decode payload arr: %v", err)
				}
				return commtypes.RawMsgAndSeq{
					Payload:    nil,
					PayloadArr: payloadArr.Payloads,
					AuxData:    rawMsg.AuxData,
					MsgSeqNum:  rawMsg.MsgSeqNum,
					LogSeqNum:  rawMsg.LogSeqNum,
					IsControl:  false,
					InjTsMs:    rawMsg.InjTsMs,
				}, nil
			} else {
				return commtypes.RawMsgAndSeq{
					Payload:    rawMsg.Payload,
					PayloadArr: nil,
					AuxData:    rawMsg.AuxData,
					MsgSeqNum:  rawMsg.MsgSeqNum,
					LogSeqNum:  rawMsg.LogSeqNum,
					IsControl:  false,
					InjTsMs:    rawMsg.InjTsMs,
				}, nil
			}
		}
	}
	return commtypes.RawMsgAndSeq{}, common_errors.ErrStreamSourceTimeout
}

/*
func (s *ShardedSharedLogStreamConsumer) Consume(ctx context.Context, parNum uint8) (*commtypes.MsgAndSeqs, error) {
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
*/
