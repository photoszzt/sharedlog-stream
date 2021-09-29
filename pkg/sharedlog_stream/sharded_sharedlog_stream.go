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
}

func NewShardedSharedLogStream(ctx context.Context, env types.Environment, topicName string, numPartitions uint32) (*ShardedSharedLogStream, error) {
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
	}, nil
}

func (s *ShardedSharedLogStream) PushToPartition(payload []byte, parNumber uint32) (uint64, error) {
	return s.subSharedLogStreams[parNumber].Push(payload)
}

func (s *ShardedSharedLogStream) PopFromPartition(parNumber uint32) ([]byte, error) {
	if parNumber < s.numPartitions {
		par := parNumber
		shard := s.subSharedLogStreams[par]
		return shard.Pop()
	} else {
		return nil, xerrors.Errorf("Invalid partition number: %d", parNumber)
	}
}
