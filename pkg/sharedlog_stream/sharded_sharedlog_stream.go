package sharedlog_stream

import (
	"context"
	"sharedlog-stream/pkg/stream/processor/commtypes"

	"cs.utexas.edu/zjia/faas/types"
	"golang.org/x/xerrors"
)

type PartitionFunc func(interface{}) uint32

type ShardedSharedLogStream struct {
	topicName           string
	subSharedLogStreams []*SharedLogStream
	numPartitions       uint8
}

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

func (s *ShardedSharedLogStream) InitStream(ctx context.Context) error {
	for i := 0; i < int(s.numPartitions); i++ {
		err := s.subSharedLogStreams[i].InitStream(ctx)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *ShardedSharedLogStream) Push(ctx context.Context, payload []byte, parNumber uint8, isControl bool) (uint64, error) {
	return s.subSharedLogStreams[parNumber].Push(ctx, payload, parNumber, isControl)
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

func (s *ShardedSharedLogStream) TopicName() string {
	return s.topicName
}
