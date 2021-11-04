package sharedlog_stream

import (
	"context"

	"cs.utexas.edu/zjia/faas/types"
	"github.com/rs/zerolog/log"
	"golang.org/x/xerrors"
)

type PartitionFunc func(interface{}) uint32

type ShardedSharedLogStream struct {
	topicName           string
	subSharedLogStreams []*SharedLogStream
	numPartitions       uint8
}

func NewShardedSharedLogStream(ctx context.Context, env types.Environment, topicName string, numPartitions uint8) (*ShardedSharedLogStream, error) {
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
		topicName:           topicName,
	}, nil
}

func (s *ShardedSharedLogStream) Push(payload []byte, parNumber uint8, additionalTag []uint64) (uint64, error) {
	return s.subSharedLogStreams[parNumber].Push(payload, parNumber, additionalTag)
}

func (s *ShardedSharedLogStream) Pop(parNumber uint8) ([]byte, error) {
	if parNumber < s.numPartitions {
		par := parNumber
		shard := s.subSharedLogStreams[par]
		return shard.Pop(parNumber)
	} else {
		return nil, xerrors.Errorf("Invalid partition number: %d", parNumber)
	}
}

func (s *ShardedSharedLogStream) TopicName() string {
	return s.topicName
}
