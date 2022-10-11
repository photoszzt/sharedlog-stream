package sharedlog_stream

import (
	"context"
	"fmt"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/exactly_once_intr"
	"sync"

	"cs.utexas.edu/zjia/faas/types"
	"golang.org/x/xerrors"
)

type PartitionFunc func(interface{}) uint32

type SizableShardedSharedLogStream struct {
	topicName string

	mux                 sync.RWMutex // this mux is for modify number of entries of subSharedLogStreams
	subSharedLogStreams []*BufferedSinkStream
	numPartitions       uint8
	serdeFormat         commtypes.SerdeFormat
}

func NewSizableShardedSharedLogStream(env types.Environment, topicName string, numPartitions uint8,
	serdeFormat commtypes.SerdeFormat,
) (*SizableShardedSharedLogStream, error) {
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
	return &SizableShardedSharedLogStream{
		subSharedLogStreams: streams,
		numPartitions:       numPartitions,
		topicName:           topicName,
		serdeFormat:         serdeFormat,
	}, nil
}

func (s *SizableShardedSharedLogStream) NumPartition() uint8 {
	s.mux.RLock()
	ret := s.numPartitions
	s.mux.RUnlock()
	return ret
}

func (s *SizableShardedSharedLogStream) Push(ctx context.Context, payload []byte, parNum uint8,
	meta LogEntryMeta, producerId commtypes.ProducerId,
) (uint64, error) {
	s.mux.RLock()
	off, err := s.subSharedLogStreams[parNum].Push(ctx, payload, parNum, meta, producerId)
	s.mux.RUnlock()
	return off, err
}

func (s *SizableShardedSharedLogStream) BufPush(ctx context.Context, payload []byte, parNum uint8, producerId commtypes.ProducerId) error {
	// idTmp := ctx.Value(commtypes.CTXID{})
	// id := ""
	// if idTmp != nil {
	// 	id = idTmp.(string)
	// }
	// debug.Fprintf(os.Stderr, "[id=%s] %s(%d) before bufpush\n", id, s.topicName, parNum)
	s.mux.RLock()
	err := s.subSharedLogStreams[parNum].BufPushGoroutineSafe(ctx, payload, producerId)
	s.mux.RUnlock()
	// debug.Fprintf(os.Stderr, "[id=%s] %s(%d) after bufpush\n", id, s.topicName, parNum)
	return err
}

func (s *SizableShardedSharedLogStream) Flush(ctx context.Context, producerId commtypes.ProducerId) error {
	// idTmp := ctx.Value(commtypes.CTXID{})
	// id := ""
	// if idTmp != nil {
	// 	id = idTmp.(string)
	// }
	s.mux.RLock()
	for i := uint8(0); i < s.numPartitions; i++ {
		// debug.Fprintf(os.Stderr, "[id=%s] %s(%d) before flush\n", id, s.topicName, i)
		err := s.subSharedLogStreams[i].FlushGoroutineSafe(ctx, producerId)
		if err != nil {
			s.mux.RUnlock()
			return err
		}
		// debug.Fprintf(os.Stderr, "[id=%s] %s(%d) after flush\n", id, s.topicName, i)
	}
	s.mux.RUnlock()
	return nil
}

func (s *SizableShardedSharedLogStream) BufPushNoLock(ctx context.Context, payload []byte, parNum uint8, producerId commtypes.ProducerId) error {
	s.mux.RLock()
	err := s.subSharedLogStreams[parNum].BufPushNoLock(ctx, payload, producerId)
	s.mux.RUnlock()
	return err
}

func (s *SizableShardedSharedLogStream) FlushNoLock(ctx context.Context, producerId commtypes.ProducerId) error {
	s.mux.RLock()
	for i := uint8(0); i < s.numPartitions; i++ {
		if err := s.subSharedLogStreams[i].FlushNoLock(ctx, producerId); err != nil {
			s.mux.RLock()
			return err
		}
	}
	s.mux.RLock()
	return nil
}

func (s *SizableShardedSharedLogStream) PushWithTag(ctx context.Context, payload []byte, parNum uint8, tags []uint64,
	additionalTopic []string, meta LogEntryMeta, producerId commtypes.ProducerId,
) (uint64, error) {
	s.mux.RLock()
	off, err := s.subSharedLogStreams[parNum].PushWithTag(ctx, payload, parNum, tags,
		additionalTopic, meta, producerId)
	s.mux.RUnlock()
	return off, err
}

func (s *SizableShardedSharedLogStream) ReadNext(ctx context.Context, parNumber uint8) (*commtypes.RawMsg, error) {
	s.mux.RLock()
	if parNumber < s.numPartitions {
		par := parNumber
		shard := s.subSharedLogStreams[par]
		msg, err := shard.Stream.ReadNext(ctx, parNumber)
		s.mux.RUnlock()
		return msg, err
	} else {
		s.mux.RUnlock()
		return nil, xerrors.Errorf("Invalid partition number: %d", parNumber)
	}
}

func (s *SizableShardedSharedLogStream) ReadNextWithTag(
	ctx context.Context, parNumber uint8, tag uint64,
) (*commtypes.RawMsg, error) {
	s.mux.RLock()
	if parNumber < s.numPartitions {
		par := parNumber
		shard := s.subSharedLogStreams[par]
		msg, err := shard.Stream.ReadNextWithTag(ctx, parNumber, tag)
		s.mux.RUnlock()
		return msg, err
	} else {
		s.mux.RUnlock()
		return nil, xerrors.Errorf("Invalid partition number: %d", parNumber)
	}
}

func (s *SizableShardedSharedLogStream) ReadBackwardWithTag(ctx context.Context, tailSeqNum uint64, parNum uint8, tag uint64) (*commtypes.RawMsg, error) {
	s.mux.RLock()
	if parNum < s.numPartitions {
		par := parNum
		shard := s.subSharedLogStreams[par]
		msg, err := shard.Stream.ReadBackwardWithTag(ctx, tailSeqNum, parNum, tag)
		s.mux.RUnlock()
		return msg, err
	} else {
		s.mux.RUnlock()
		return nil, xerrors.Errorf("Invalid partition number: %d", parNum)
	}
}

func (s *SizableShardedSharedLogStream) TopicNameHash() uint64 {
	return s.subSharedLogStreams[0].Stream.topicNameHash
}

func (s *SizableShardedSharedLogStream) TopicName() string {
	return s.topicName
}

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

func NewShardedSharedLogStreamWithSinkBufferSize(env types.Environment, topicName string,
	numPartitions uint8, serdeFormat commtypes.SerdeFormat, sinkBufferSize int,
) (*ShardedSharedLogStream, error) {
	if numPartitions == 0 {
		panic(ErrZeroParNum)
	}
	streams := make([]*BufferedSinkStream, 0, numPartitions)
	for i := uint8(0); i < numPartitions; i++ {
		s, err := NewSharedLogStream(env, topicName, serdeFormat)
		if err != nil {
			return nil, err
		}
		buf := NewBufferedSinkStreamWithBufferSize(s, sinkBufferSize, i)
		streams = append(streams, buf)
	}
	return &ShardedSharedLogStream{
		subSharedLogStreams: streams,
		numPartitions:       numPartitions,
		topicName:           topicName,
		serdeFormat:         serdeFormat,
	}, nil
}

func (s *ShardedSharedLogStream) ExactlyOnce(gua exactly_once_intr.GuaranteeMth) {
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

func (s *SizableShardedSharedLogStream) ScaleSubstreams(env types.Environment, scaleTo uint8) error {
	if scaleTo == 0 {
		return fmt.Errorf("updated number of substreams should be larger and equal to one")
	}
	s.mux.Lock()
	if scaleTo > s.numPartitions {
		numAdd := scaleTo - s.numPartitions
		for i := uint8(0); i < numAdd; i++ {
			subs, err := NewSharedLogStream(env, s.topicName, s.serdeFormat)
			if err != nil {
				s.mux.Unlock()
				return err
			}
			buf := NewBufferedSinkStream(subs, i+s.numPartitions)
			s.subSharedLogStreams = append(s.subSharedLogStreams, buf)
		}
	}
	s.numPartitions = scaleTo
	s.mux.Unlock()
	return nil
}

func (s *ShardedSharedLogStream) NumPartition() uint8 {
	ret := s.numPartitions
	return ret
}

func (s *ShardedSharedLogStream) Push(ctx context.Context, payload []byte, parNum uint8,
	meta LogEntryMeta, producerId commtypes.ProducerId,
) (uint64, error) {
	off, err := s.subSharedLogStreams[parNum].Push(ctx, payload, parNum, meta, producerId)
	return off, err
}

func (s *ShardedSharedLogStream) BufPush(ctx context.Context, payload []byte, parNum uint8, producerId commtypes.ProducerId) error {
	// idTmp := ctx.Value(commtypes.CTXID{})
	// id := ""
	// if idTmp != nil {
	// 	id = idTmp.(string)
	// }
	// debug.Fprintf(os.Stderr, "[id=%s] %s(%d) before bufpush\n", id, s.topicName, parNum)
	err := s.subSharedLogStreams[parNum].BufPushGoroutineSafe(ctx, payload, producerId)
	// debug.Fprintf(os.Stderr, "[id=%s] %s(%d) after bufpush\n", id, s.topicName, parNum)
	return err
}

func (s *ShardedSharedLogStream) Flush(ctx context.Context, producerId commtypes.ProducerId) error {
	// idTmp := ctx.Value(commtypes.CTXID{})
	// id := ""
	// if idTmp != nil {
	// 	id = idTmp.(string)
	// }
	for i := uint8(0); i < s.numPartitions; i++ {
		// debug.Fprintf(os.Stderr, "[id=%s] %s(%d) before flush\n", id, s.topicName, i)
		err := s.subSharedLogStreams[i].FlushGoroutineSafe(ctx, producerId)
		if err != nil {
			return err
		}
		// debug.Fprintf(os.Stderr, "[id=%s] %s(%d) after flush\n", id, s.topicName, i)
	}
	return nil
}

func (s *ShardedSharedLogStream) BufPushNoLock(ctx context.Context, payload []byte, parNum uint8, producerId commtypes.ProducerId) error {
	err := s.subSharedLogStreams[parNum].BufPushNoLock(ctx, payload, producerId)
	return err
}

func (s *ShardedSharedLogStream) FlushNoLock(ctx context.Context, producerId commtypes.ProducerId) error {
	for i := uint8(0); i < s.numPartitions; i++ {
		if err := s.subSharedLogStreams[i].FlushNoLock(ctx, producerId); err != nil {
			return err
		}
	}
	return nil
}

func (s *ShardedSharedLogStream) PushWithTag(ctx context.Context, payload []byte, parNum uint8, tags []uint64,
	additionalTopic []string, meta LogEntryMeta, producerId commtypes.ProducerId,
) (uint64, error) {
	off, err := s.subSharedLogStreams[parNum].PushWithTag(ctx, payload, parNum, tags,
		additionalTopic, meta, producerId)
	return off, err
}

func (s *ShardedSharedLogStream) ReadNext(ctx context.Context, parNumber uint8) (*commtypes.RawMsg, error) {
	if parNumber < s.numPartitions {
		par := parNumber
		shard := s.subSharedLogStreams[par]
		msg, err := shard.Stream.ReadNext(ctx, parNumber)
		return msg, err
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
		msg, err := shard.Stream.ReadNextWithTag(ctx, parNumber, tag)
		return msg, err
	} else {
		return nil, xerrors.Errorf("Invalid partition number: %d", parNumber)
	}
}

func (s *ShardedSharedLogStream) ReadBackwardWithTag(ctx context.Context, tailSeqNum uint64, parNum uint8, tag uint64) (*commtypes.RawMsg, error) {
	if parNum < s.numPartitions {
		par := parNum
		shard := s.subSharedLogStreams[par]
		msg, err := shard.Stream.ReadBackwardWithTag(ctx, tailSeqNum, parNum, tag)
		return msg, err
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

// not threadsafe for modify cursor; readlock is protecting the subSharedLogStreams
func (s *ShardedSharedLogStream) SetCursor(cursor uint64, parNum uint8) {
	s.subSharedLogStreams[parNum].Stream.cursor = cursor
}

func (s *ShardedSharedLogStream) GetCuror(parNum uint8) uint64 {
	cur := s.subSharedLogStreams[parNum].Stream.cursor
	return cur
}
