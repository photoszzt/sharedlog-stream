package sharedlog_stream

import (
	"context"

	"cs.utexas.edu/zjia/faas/types"
	"github.com/rs/zerolog/log"
	"golang.org/x/xerrors"
)

type PartitionFunc func(interface{}) uint32

type ShardedSharedLogStream struct {
	subSharedLogStreams []*SharedLogStream
	numPartitions       uint32
	parFunc             PartitionFunc
}

func NewShardedSharedLogStream(ctx context.Context, env types.Environment,
	topicName string, numPartitions uint32, partitionFunc PartitionFunc) (*ShardedSharedLogStream, error) {
	if numPartitions <= 0 {
		log.Fatal().Msgf("Shards must be positive")
	}
	streams := make([]*SharedLogStream, 0, 16)
	for i := 0; i < int(numPartitions); i++ {
		s, err := NewSharedLogStream(ctx, env, topicName)
		if err != nil {
			return nil, err
		}
		streams = append(streams, s)
	}
	return &ShardedSharedLogStream{
		subSharedLogStreams: streams,
		numPartitions:       numPartitions,
		parFunc:             partitionFunc,
	}, nil
}

func (s *ShardedSharedLogStream) Push(payload []byte) (uint32, uint64, error) {
	par := s.parFunc(payload) % s.numPartitions
	shard := s.subSharedLogStreams[par]
	seq, err := shard.Push(payload)
	return par, seq, err
}

func (s *ShardedSharedLogStream) PushToPartition(payload []byte, parNumber uint32) (uint64, error) {
	return s.subSharedLogStreams[parNumber].Push(payload)
}

func (s *ShardedSharedLogStream) PopFromPartition(parNumber uint32) ([]byte, error) {
	if 0 <= parNumber && parNumber < s.numPartitions {
		par := parNumber
		shard := s.subSharedLogStreams[par]
		return shard.Pop()
	} else {
		return nil, xerrors.Errorf("Invalid partition number: %d", parNumber)
	}
}
