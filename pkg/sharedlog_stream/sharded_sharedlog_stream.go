package sharedlog_stream

import (
	"context"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/stream/processor/store"

	"cs.utexas.edu/zjia/faas/types"
	"golang.org/x/xerrors"
)

type PartitionFunc func(interface{}) uint32

type ShardedSharedLogStream struct {
	topicName           string
	subSharedLogStreams []*SharedLogStream
	numPartitions       uint8
}

var _ = store.Stream(&ShardedSharedLogStream{})

var (
	ErrZeroParNum = xerrors.New("Shards must be positive")
)

func NewShardedSharedLogStream(env types.Environment, topicName string, numPartitions uint8) (*ShardedSharedLogStream, error) {
	if numPartitions == 0 {
		return nil, ErrZeroParNum
	}
	streams := make([]*SharedLogStream, 0, 16)
	for i := 0; i < int(numPartitions); i++ {
		s := NewSharedLogStream(env, topicName)
		streams = append(streams, s)
	}
	return &ShardedSharedLogStream{
		subSharedLogStreams: streams,
		numPartitions:       numPartitions,
		topicName:           topicName,
	}, nil
}

func (s *ShardedSharedLogStream) Push(ctx context.Context, payload []byte, parNumber uint8, isControl bool) (uint64, error) {
	return s.subSharedLogStreams[parNumber].Push(ctx, payload, parNumber, isControl)
}

func (s *ShardedSharedLogStream) PushWithTag(ctx context.Context, payload []byte, parNumber uint8, tags []uint64, isControl bool) (uint64, error) {
	return s.subSharedLogStreams[parNumber].PushWithTag(ctx, payload, parNumber, tags, isControl)
}

func (s *ShardedSharedLogStream) ReadNext(ctx context.Context, parNumber uint8) (commtypes.AppIDGen, []commtypes.RawMsg, error) {
	if parNumber < s.numPartitions {
		par := parNumber
		shard := s.subSharedLogStreams[par]
		return shard.ReadNext(ctx, parNumber)
	} else {
		return commtypes.EmptyAppIDGen, nil, xerrors.Errorf("Invalid partition number: %d", parNumber)
	}
}

func (s *ShardedSharedLogStream) ReadNextWithTag(ctx context.Context, parNumber uint8, tag uint64) (commtypes.AppIDGen, []commtypes.RawMsg, error) {
	if parNumber < s.numPartitions {
		par := parNumber
		shard := s.subSharedLogStreams[par]
		return shard.ReadNextWithTag(ctx, parNumber, tag)
	} else {
		return commtypes.EmptyAppIDGen, nil, xerrors.Errorf("Invalid partition number: %d", parNumber)
	}
}

func (s *ShardedSharedLogStream) ReadBackwardWithTag(ctx context.Context, tailSeqNum uint64, parNum uint8, tag uint64) (*commtypes.AppIDGen, *commtypes.RawMsg, error) {
	if parNum < s.numPartitions {
		par := parNum
		shard := s.subSharedLogStreams[par]
		return shard.ReadBackwardWithTag(ctx, tailSeqNum, parNum, tag)
	} else {
		return nil, nil, xerrors.Errorf("Invalid partition number: %d", parNum)
	}
}

func (s *ShardedSharedLogStream) TopicName() string {
	return s.topicName
}

func (s *ShardedSharedLogStream) TopicNameHash() uint64 {
	return s.subSharedLogStreams[0].topicNameHash
}

func (s *ShardedSharedLogStream) SetCursor(cursor uint64, parNum uint8) {
	s.subSharedLogStreams[parNum].cursor = cursor
}
