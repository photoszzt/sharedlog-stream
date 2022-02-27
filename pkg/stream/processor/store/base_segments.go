package store

import (
	"context"
	"fmt"

	"github.com/google/btree"
)

type BaseSegments struct {
	segments        *btree.BTree
	name            string
	retentionPeriod int64
	segmentInterval int64
}

var _ = Segments(&BaseSegments{})

type Int64 int64

func (x Int64) Less(than btree.Item) bool {
	return x < than.(Int64)
}

type KeySegment struct {
	Key   btree.Item
	Value Segment
}

func (kv *KeySegment) Less(than btree.Item) bool {
	return kv.Key.Less(than)
}

func NewBaseSegments(name string, retentionPeriod int64, segmentInterval int64) *BaseSegments {
	return &BaseSegments{
		segments:        btree.New(2),
		name:            name,
		retentionPeriod: retentionPeriod,
		segmentInterval: segmentInterval,
	}
}

func (s *BaseSegments) SegmentId(ts int64) int64 {
	return ts / s.segmentInterval
}

func (s *BaseSegments) SegmentName(segmentId int64) string {
	return fmt.Sprintf("%s.%d%d", s.name, segmentId, s.segmentInterval)
}

func (s *BaseSegments) GetSegmentForTimestamp(ts int64) Segment {
	ks := s.segments.Get(Int64(s.SegmentId(ts))).(*KeySegment)
	return ks.Value
}

func (s *BaseSegments) GetOrCreateSegmentIfLive(ctx context.Context, segmentId int64,
	streamTime int64, getOrCreateSegment func(segmentId int64) Segment,
) Segment {
	minLiveTimestamp := streamTime - s.retentionPeriod
	minLiveSegment := s.SegmentId(minLiveTimestamp)
	var toReturn Segment
	if segmentId >= minLiveSegment {
		toReturn = getOrCreateSegment(segmentId)
	} else {
		toReturn = nil
	}
	s.cleanupEarlierThan(ctx, minLiveSegment)
	return toReturn
}

func (s *BaseSegments) GetOrCreateSegment(segmentId int64) Segment {
	panic("Should not call this method")
}

func (s *BaseSegments) cleanupEarlierThan(ctx context.Context, minLiveSegment int64) error {
	var got []btree.Item
	s.segments.AscendLessThan(Int64(minLiveSegment), func(i btree.Item) bool {
		got = append(got, i)
		return true
	})
	for _, item := range got {
		kv := item.(*KeySegment)
		ret := s.segments.Delete(kv.Key)
		err := ret.(*KeySegment).Value.Destroy(ctx)
		if err != nil {
			return err
		}
	}
	return nil
}
