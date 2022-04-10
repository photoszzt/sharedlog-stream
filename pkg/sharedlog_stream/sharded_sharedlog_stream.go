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

// subSharedLogStreams array should only be modified in one thread
type ShardedSharedLogStream struct {
	topicName string

	sMu                 sync.RWMutex // guards
	subSharedLogStreams []*BufferedSinkStream
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
			buf := NewBufferedSinkStream(subs, i+s.numPartitions)
			s.subSharedLogStreams = append(s.subSharedLogStreams, buf)
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
		s.subSharedLogStreams[i].Stream.SetInTransaction(inTransaction)
	}
}

func (s *ShardedSharedLogStream) Push(ctx context.Context, payload []byte, parNumber uint8,
	isControl bool, payloadIsArr bool,
) (uint64, error) {
	s.sMu.RLock()
	defer s.sMu.RUnlock()
	return s.subSharedLogStreams[parNumber].Stream.Push(ctx, payload, parNumber, isControl, payloadIsArr)
}

func (s *ShardedSharedLogStream) BufPush(ctx context.Context, payload []byte, parNum uint8) error {
	s.sMu.RLock()
	defer s.sMu.RUnlock()
	return s.subSharedLogStreams[parNum].BufPush(ctx, payload)
}

func (s *ShardedSharedLogStream) Flush(ctx context.Context) error {
	s.sMu.RLock()
	defer s.sMu.RUnlock()
	for i := uint8(0); i < s.numPartitions; i++ {
		if err := s.subSharedLogStreams[i].Flush(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (s *ShardedSharedLogStream) PushWithTag(ctx context.Context, payload []byte, parNumber uint8, tags []uint64,
	isControl bool, payloadIsArr bool,
) (uint64, error) {
	s.sMu.RLock()
	defer s.sMu.RUnlock()
	return s.subSharedLogStreams[parNumber].Stream.PushWithTag(ctx, payload, parNumber, tags, isControl, payloadIsArr)
}

func (s *ShardedSharedLogStream) ReadNext(ctx context.Context, parNumber uint8) (commtypes.TaskIDGen, []commtypes.RawMsg, error) {
	s.sMu.RLock()
	defer s.sMu.RUnlock()
	if parNumber < s.numPartitions {
		par := parNumber
		shard := s.subSharedLogStreams[par]
		return shard.Stream.ReadNext(ctx, parNumber)
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
		return shard.Stream.ReadNextWithTag(ctx, parNumber, tag)
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
		return shard.Stream.ReadBackwardWithTag(ctx, tailSeqNum, parNum, tag)
	} else {
		return nil, nil, xerrors.Errorf("Invalid partition number: %d", parNum)
	}
}

func (s *ShardedSharedLogStream) TopicName() string {
	return s.topicName
}

func (s *ShardedSharedLogStream) TopicNameHash() uint64 {
	return s.subSharedLogStreams[0].Stream.topicNameHash
}

func (s *ShardedSharedLogStream) SetCursor(cursor uint64, parNum uint8) {
	s.sMu.RLock()
	defer s.sMu.RUnlock()
	s.subSharedLogStreams[parNum].Stream.cursor = cursor
}

func (s *ShardedSharedLogStream) SetTaskEpoch(epoch uint16) {
	s.sMu.RLock()
	defer s.sMu.RUnlock()
	for i := uint8(0); i < s.numPartitions; i++ {
		s.subSharedLogStreams[i].Stream.SetTaskEpoch(epoch)
	}
}

func (s *ShardedSharedLogStream) SetTaskId(id uint64) {
	s.sMu.RLock()
	defer s.sMu.RUnlock()
	for i := uint8(0); i < s.numPartitions; i++ {
		s.subSharedLogStreams[i].Stream.SetTaskId(id)
	}
}
