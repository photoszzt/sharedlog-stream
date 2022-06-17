package sharedlog_stream

import (
	"context"
	"fmt"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/transaction/tran_interface"

	"cs.utexas.edu/zjia/faas/types"
	"golang.org/x/xerrors"
)

type PartitionFunc func(interface{}) uint32

// subSharedLogStreams array should only be modified in one thread
type ShardedSharedLogStream struct {
	topicName string

	subSharedLogStreams []*BufferedSinkStream
	numPartitions       uint8
	serdeFormat         commtypes.SerdeFormat
}

var _ = Stream(&ShardedSharedLogStream{})

var (
	ErrZeroParNum = xerrors.New("Shards must be positive")
)

func NewShardedSharedLogStream(env types.Environment, topicName string, numPartitions uint8, serdeFormat commtypes.SerdeFormat) (*ShardedSharedLogStream, error) {
	if numPartitions == 0 {
		panic(ErrZeroParNum)
	}
	streams := make([]*BufferedSinkStream, 0, numPartitions)
	for i := uint8(0); i < numPartitions; i++ {
		s, err := NewSharedLogStream(env, topicName, serdeFormat)
		if err != nil {
			return nil, err
		}
		buf := NewBufferedSinkStream(s, i)
		streams = append(streams, buf)
	}
	return &ShardedSharedLogStream{
		subSharedLogStreams: streams,
		numPartitions:       numPartitions,
		topicName:           topicName,
		serdeFormat:         serdeFormat,
	}, nil
}

func (s *ShardedSharedLogStream) ExactlyOnce(gua tran_interface.GuaranteeMth) {
	for _, substream := range s.subSharedLogStreams {
		substream.ExactlyOnce(gua)
	}
}

func (s *ShardedSharedLogStream) ResetInitialProd() {
	for _, substream := range s.subSharedLogStreams {
		substream.ResetInitialProd()
	}
}

func (s *ShardedSharedLogStream) GetInitialProdSeqNum(substreamNum uint8) uint64 {
	return s.subSharedLogStreams[substreamNum].GetInitialProdSeqNum()
}

func (s *ShardedSharedLogStream) GetCurrentProdSeqNum(substreamNum uint8) uint64 {
	return s.subSharedLogStreams[substreamNum].GetCurrentProdSeqNum()
}

func (s *ShardedSharedLogStream) ScaleSubstreams(env types.Environment, scaleTo uint8) error {
	if scaleTo == 0 {
		return fmt.Errorf("updated number of substreams should be larger and equal to one")
	}
	if scaleTo > s.numPartitions {
		numAdd := scaleTo - s.numPartitions
		for i := uint8(0); i < numAdd; i++ {
			subs, err := NewSharedLogStream(env, s.topicName, s.serdeFormat)
			if err != nil {
				return err
			}
			buf := NewBufferedSinkStream(subs, i+s.numPartitions)
			s.subSharedLogStreams = append(s.subSharedLogStreams, buf)
		}
	}
	s.numPartitions = scaleTo
	return nil
}

func (s *ShardedSharedLogStream) NumPartition() uint8 {
	return s.numPartitions
}

func (s *ShardedSharedLogStream) Push(ctx context.Context, payload []byte, parNumber uint8,
	meta LogEntryMeta, producerId tran_interface.ProducerId,
) (uint64, error) {
	return s.subSharedLogStreams[parNumber].Stream.Push(ctx, payload, parNumber, meta, producerId)
}

func (s *ShardedSharedLogStream) BufPush(ctx context.Context, payload []byte, parNum uint8, producerId tran_interface.ProducerId) error {
	return s.subSharedLogStreams[parNum].BufPushGoroutineSafe(ctx, payload, producerId)
}

func (s *ShardedSharedLogStream) Flush(ctx context.Context, producerId tran_interface.ProducerId) error {
	for i := uint8(0); i < s.numPartitions; i++ {
		if err := s.subSharedLogStreams[i].FlushGoroutineSafe(ctx, producerId); err != nil {
			return err
		}
	}
	return nil
}

func (s *ShardedSharedLogStream) BufPushNoLock(ctx context.Context, payload []byte, parNum uint8, producerId tran_interface.ProducerId) error {
	return s.subSharedLogStreams[parNum].BufPushNoLock(ctx, payload, producerId)
}

func (s *ShardedSharedLogStream) FlushNoLock(ctx context.Context, producerId tran_interface.ProducerId) error {
	for i := uint8(0); i < s.numPartitions; i++ {
		if err := s.subSharedLogStreams[i].FlushNoLock(ctx, producerId); err != nil {
			return err
		}
	}
	return nil
}

func (s *ShardedSharedLogStream) PushWithTag(ctx context.Context, payload []byte, parNumber uint8, tags []uint64,
	additionalTopic []string, meta LogEntryMeta, producerId tran_interface.ProducerId,
) (uint64, error) {
	return s.subSharedLogStreams[parNumber].Stream.PushWithTag(ctx, payload, parNumber, tags,
		additionalTopic, meta, producerId)
}

func (s *ShardedSharedLogStream) ReadNext(ctx context.Context, parNumber uint8) (*commtypes.RawMsg, error) {
	if parNumber < s.numPartitions {
		par := parNumber
		shard := s.subSharedLogStreams[par]
		return shard.Stream.ReadNext(ctx, parNumber)
	} else {
		return nil, xerrors.Errorf("Invalid partition number: %d", parNumber)
	}
}

func (s *ShardedSharedLogStream) ReadNextWithTag(
	ctx context.Context, parNumber uint8, tag uint64,
) (*commtypes.RawMsg, error) {
	if parNumber < s.numPartitions {
		par := parNumber
		shard := s.subSharedLogStreams[par]
		return shard.Stream.ReadNextWithTag(ctx, parNumber, tag)
	} else {
		return nil, xerrors.Errorf("Invalid partition number: %d", parNumber)
	}
}

func (s *ShardedSharedLogStream) ReadBackwardWithTag(ctx context.Context, tailSeqNum uint64, parNum uint8, tag uint64) (*commtypes.RawMsg, error) {
	if parNum < s.numPartitions {
		par := parNum
		shard := s.subSharedLogStreams[par]
		return shard.Stream.ReadBackwardWithTag(ctx, tailSeqNum, parNum, tag)
	} else {
		return nil, xerrors.Errorf("Invalid partition number: %d", parNum)
	}
}

func (s *ShardedSharedLogStream) TopicName() string {
	return s.topicName
}

func (s *ShardedSharedLogStream) TopicNameHash() uint64 {
	return s.subSharedLogStreams[0].Stream.topicNameHash
}

func (s *ShardedSharedLogStream) SetCursor(cursor uint64, parNum uint8) {
	s.subSharedLogStreams[parNum].Stream.cursor = cursor
}
