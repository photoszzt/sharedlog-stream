package store

import (
	"bytes"
	"context"
	"fmt"

	"github.com/rs/zerolog/log"
)

type BaseSegmentedBytesStore struct {
	keySchema          KeySchema
	segments           Segments
	name               string
	observedStreamTime int64
}

var _ = SegmentedBytesStore(&BaseSegmentedBytesStore{})

func NewRedisSegmentedBytesStore(name string,
	retention int64, // ms
	keySchema KeySchema,
	rkvs *RedisKeyValueStore,
) *BaseSegmentedBytesStore {
	segmentInterval := retention / 2
	if segmentInterval < 60_000 {
		segmentInterval = 60_000
	}
	return &BaseSegmentedBytesStore{
		name:               name,
		keySchema:          keySchema,
		segments:           NewRedisKeyValueSegments(name, retention, segmentInterval, rkvs),
		observedStreamTime: -1,
	}
}

func (s *BaseSegmentedBytesStore) IsOpen() bool { return true }
func (s *BaseSegmentedBytesStore) Name() string { return s.name }

// Fetch all records from the segmented store with the provided key and time range
// from all existing segments
func (s *BaseSegmentedBytesStore) Fetch(key []byte, from int64, to int64,
	iterFunc func(int64 /* ts */, KeyT, ValueT) error,
) error {
	binaryFrom := s.keySchema.LowerRangeFixedSize(key, from)
	binaryTo := s.keySchema.UpperRangeFixedSize(key, to)
	segment_slice := s.segments.Segments(from, to)
	for _, seg := range segment_slice {
		seg.Range(binaryFrom, binaryTo, func(kt KeyT, vt ValueT) error {
			bytes := kt.([]byte)
			has, ts := s.keySchema.HasNextCondition(bytes, key, key, from, to)
			if has {
				err := iterFunc(ts, bytes, vt)
				if err != nil {
					return err
				}
			}
			return nil
		})
	}
	return nil
}

// Fetch all records from the segmented store with the provided key and time range
// from all existing segments in backward order (from latest to earliest)
func (s *BaseSegmentedBytesStore) BackwardFetch(key []byte, from int64, to int64,
	iterFunc func(int64 /* ts */, KeyT, ValueT) error,
) error {
	panic("not implemented")
}

func (s *BaseSegmentedBytesStore) FetchWithKeyRange(keyFrom []byte, keyTo []byte,
	from int64, to int64,
	iterFunc func(int64 /* ts */, KeyT, ValueT) error,
) error {
	if bytes.Compare(keyFrom, keyTo) > 0 {
		return fmt.Errorf("key from should be smaller than key to")
	}
	binaryFrom := s.keySchema.LowerRangeFixedSize(keyFrom, from)
	binaryTo := s.keySchema.UpperRangeFixedSize(keyTo, to)
	segment_slice := s.segments.Segments(from, to)
	for _, seg := range segment_slice {
		seg.Range(binaryFrom, binaryTo, func(kt KeyT, vt ValueT) error {
			bytes := kt.([]byte)
			has, ts := s.keySchema.HasNextCondition(bytes, keyFrom, keyTo, from, to)
			if has {
				err := iterFunc(ts, bytes, vt)
				if err != nil {
					return err
				}
			}
			return nil
		})
	}
	return nil
}

func (s *BaseSegmentedBytesStore) BackwardFetchWithKeyRange(
	keyFrom []byte,
	keyTo []byte,
	from int64,
	to int64,
	iterFunc func(int64 /* ts */, KeyT, ValueT) error,
) error {
	panic("not implemented")
}

func (s *BaseSegmentedBytesStore) FetchAll(
	iterFunc func(int64 /* ts */, KeyT, ValueT) error,
) error {
	panic("not implemented")
}

func (s *BaseSegmentedBytesStore) BackwardFetchAll(
	iterFunc func(int64 /* ts */, KeyT, ValueT) error,
) error {
	panic("not implemented")
}

func (s *BaseSegmentedBytesStore) Remove(ctx context.Context, key []byte) error {
	ts := s.keySchema.SegmentTimestamp(key)
	if ts > s.observedStreamTime {
		s.observedStreamTime = ts
	}
	segment := s.segments.GetSegmentForTimestamp(ts)
	if segment == nil {
		return nil
	}
	err := segment.Delete(ctx, key)
	return err
}

func (s *BaseSegmentedBytesStore) RemoveWithTs(key []byte, timestamp uint64) {
	panic("not implemented")
}

func (s *BaseSegmentedBytesStore) Put(ctx context.Context, key []byte, value []byte) error {
	ts := s.keySchema.SegmentTimestamp(key)
	if ts > s.observedStreamTime {
		s.observedStreamTime = ts
	}
	segmentId := s.segments.SegmentId(ts)
	segment, err := s.segments.GetOrCreateSegmentIfLive(ctx, segmentId, s.observedStreamTime, s.segments.GetOrCreateSegment)
	if err != nil {
		return err
	}
	if segment == nil {
		log.Warn().Msg("Skipping record for expired segment")
	} else {
		segment.Put(ctx, key, value)
	}
	return nil
}

func (s *BaseSegmentedBytesStore) Get(ctx context.Context, key []byte) (ValueT, bool, error) {
	segment := s.segments.GetSegmentForTimestamp(s.keySchema.SegmentTimestamp(key))
	if segment == nil {
		return nil, false, nil
	}
	return segment.Get(ctx, key)
}
