package checkpt

import (
	"context"
	"sharedlog-stream/pkg/common_errors"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/types/known/emptypb"
)

type CheckpointManagerServer struct {
	UnimplementedChkptMngrServer

	tpCounts sync.Map
	ended    atomic.Bool
}

func NewCheckpointManagerServer() *CheckpointManagerServer {
	s := CheckpointManagerServer{}
	s.ended.Store(false)
	return &s
}

func (s *CheckpointManagerServer) ResetCheckPointCount(tpNames []string) {
	for _, tp := range tpNames {
		v := new(uint32)
		s.tpCounts.Store(tp, v)
	}
}

func (s *CheckpointManagerServer) Ended() bool { return s.ended.Load() }

func (s *CheckpointManagerServer) FinishChkpt(ctx context.Context, in *FinMsg) (*emptypb.Empty, error) {
	for _, tp := range in.TopicNames {
		v, ok := s.tpCounts.Load(tp)
		if !ok {
			return nil, common_errors.ErrTopicNotFound
		}
		c := v.(*uint32)
		atomic.AddUint32(c, 1)
	}
	return &emptypb.Empty{}, nil
}

func (s *CheckpointManagerServer) ReqChkmngrEndedIfNot(context.Context, *emptypb.Empty) (*emptypb.Empty, error) {
	ended := s.ended.Load()
	if !ended {
		s.ended.CompareAndSwap(false, true)
	}
	return &emptypb.Empty{}, nil
}

func (s *CheckpointManagerServer) WaitForChkptFinish(
	finalOutputTopicNames []string,
	finalNumOutPartition []uint8,
) (bool, error) {
	for {
		allFinished := true
		for idx, tp := range finalOutputTopicNames {
			v, ok := s.tpCounts.Load(tp)
			if !ok {
				return false, common_errors.ErrTopicNotFound
			}
			c := v.(*uint32)
			count := atomic.LoadUint32(c)
			parNum := finalNumOutPartition[idx]
			if count != uint32(parNum) {
				allFinished = false
				break
			}
		}
		if allFinished {
			return false, nil
		}
		ended := s.ended.Load()
		if ended {
			return true, nil
		}
		time.Sleep(5 * time.Millisecond)
	}
}
