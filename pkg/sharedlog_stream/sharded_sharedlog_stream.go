package sharedlog_stream

import (
	"context"
	"fmt"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/stream/processor/store"
	"sync"

	"cs.utexas.edu/zjia/faas/types"
	"golang.org/x/xerrors"
)

type PartitionFunc func(interface{}) uint32

type ShardedSharedLogStream struct {
	topicName string

	sMu                 sync.RWMutex // guards
	subSharedLogStreams []*SharedLogStream
	numPartitions       uint8
	serdeFormat         commtypes.SerdeFormat
}

var _ = store.Stream(&ShardedSharedLogStream{})

var (
	ErrZeroParNum = xerrors.New("Shards must be positive")
)

func NewShardedSharedLogStream(env types.Environment, topicName string, numPartitions uint8, serdeFormat commtypes.SerdeFormat) (*ShardedSharedLogStream, error) {
	if numPartitions == 0 {
		return nil, ErrZeroParNum
	}
	streams := make([]*SharedLogStream, 0, 16)
	for i := uint8(0); i < numPartitions; i++ {
		s, err := NewSharedLogStream(env, topicName, serdeFormat)
		if err != nil {
			return nil, err
		}
		streams = append(streams, s)
	}
	return &ShardedSharedLogStream{
		subSharedLogStreams: streams,
		numPartitions:       numPartitions,
		topicName:           topicName,
		serdeFormat:         serdeFormat,
	}, nil
}

func (s *ShardedSharedLogStream) ScaleSubstreams(env types.Environment, scaleTo uint8) error {
	s.sMu.Lock()
	defer s.sMu.Unlock()
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
			s.subSharedLogStreams = append(s.subSharedLogStreams, subs)
		}
	}
	s.numPartitions = scaleTo
	return nil
}

func (s *ShardedSharedLogStream) NumPartition() uint8 {
	s.sMu.RLock()
	defer s.sMu.RUnlock()
	return s.numPartitions
}

func (s *ShardedSharedLogStream) SetInTransaction(inTransaction bool) {
	s.sMu.RLock()
	defer s.sMu.RUnlock()
	for i := uint8(0); i < s.numPartitions; i++ {
		s.subSharedLogStreams[i].SetInTransaction(inTransaction)
	}
}

func (s *ShardedSharedLogStream) Push(ctx context.Context, payload []byte, parNumber uint8, isControl bool) (uint64, error) {
	s.sMu.RLock()
	defer s.sMu.RUnlock()
	return s.subSharedLogStreams[parNumber].Push(ctx, payload, parNumber, isControl)
}

func (s *ShardedSharedLogStream) PushWithTag(ctx context.Context, payload []byte, parNumber uint8, tags []uint64, isControl bool) (uint64, error) {
	s.sMu.RLock()
	defer s.sMu.RUnlock()
	return s.subSharedLogStreams[parNumber].PushWithTag(ctx, payload, parNumber, tags, isControl)
}

func (s *ShardedSharedLogStream) ReadNext(ctx context.Context, parNumber uint8) (commtypes.TaskIDGen, []commtypes.RawMsg, error) {
	s.sMu.RLock()
	defer s.sMu.RUnlock()
	if parNumber < s.numPartitions {
		par := parNumber
		shard := s.subSharedLogStreams[par]
		return shard.ReadNext(ctx, parNumber)
	} else {
		return commtypes.EmptyAppIDGen, nil, xerrors.Errorf("Invalid partition number: %d", parNumber)
	}
}

func (s *ShardedSharedLogStream) ReadNextWithTag(
	ctx context.Context, parNumber uint8, tag uint64,
) (commtypes.TaskIDGen, []commtypes.RawMsg, error) {
	s.sMu.RLock()
	defer s.sMu.RUnlock()
	if parNumber < s.numPartitions {
		par := parNumber
		shard := s.subSharedLogStreams[par]
		return shard.ReadNextWithTag(ctx, parNumber, tag)
	} else {
		return commtypes.EmptyAppIDGen, nil, xerrors.Errorf("Invalid partition number: %d", parNumber)
	}
}

func (s *ShardedSharedLogStream) ReadBackwardWithTag(ctx context.Context, tailSeqNum uint64, parNum uint8, tag uint64) (*commtypes.TaskIDGen, *commtypes.RawMsg, error) {
	s.sMu.RLock()
	defer s.sMu.RUnlock()
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
	s.sMu.RLock()
	defer s.sMu.RUnlock()
	s.subSharedLogStreams[parNum].cursor = cursor
}

func (s *ShardedSharedLogStream) SetTaskEpoch(epoch uint16) {
	s.sMu.RLock()
	defer s.sMu.RUnlock()
	for i := uint8(0); i < s.numPartitions; i++ {
		s.subSharedLogStreams[i].SetTaskEpoch(epoch)
	}
}

func (s *ShardedSharedLogStream) SetTaskId(id uint64) {
	s.sMu.RLock()
	defer s.sMu.RUnlock()
	for i := uint8(0); i < s.numPartitions; i++ {
		s.subSharedLogStreams[i].SetTaskId(id)
	}
}
