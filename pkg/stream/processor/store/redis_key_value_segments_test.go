package store

import (
	"context"
	"testing"
)

/*
func TestShouldCreateSegments(t *testing.T) {
	rkvs := getRedisKeyValueStore()
	segments := NewRedisKeyValueSegments("test", bstest_retention_period, bstest_segment_interval, rkvs)
	ctx := context.Background()
	rkvs.rdb.FlushAll(ctx)
	_, err := segments.GetOrCreateSegmentIfLive(ctx, 0, -1, segments.GetOrCreateSegment, segments.CleanupExpiredMeta)
	if err != nil {
		t.Fatal(err.Error())
	}
	_, err = segments.GetOrCreateSegmentIfLive(ctx, 1, -1, segments.GetOrCreateSegment, segments.CleanupExpiredMeta)
	if err != nil {
		t.Fatal(err.Error())
	}
	_, err = segments.GetOrCreateSegmentIfLive(ctx, 2, -1, segments.GetOrCreateSegment, segments.CleanupExpiredMeta)
	if err != nil {
		t.Fatal(err.Error())
	}
	segs, err := segments.getSegments(ctx)
	if err != nil {
		t.Fatal(err.Error())
	}
	segSet := toStrSet(segs)
	if _, ok := segSet["test.0"]; !ok {
		t.Fatal("test.0 should exists")
	}
	expected := fmt.Sprintf("test.%d", bstest_segment_interval)
	if _, ok := segSet[expected]; !ok {
		t.Fatalf("%s should exists", expected)
	}
	expected = fmt.Sprintf("test.%d", bstest_segment_interval*2)
	if _, ok := segSet[expected]; !ok {
		t.Fatalf("%s should exists", expected)
	}
	rkvs.rdb.FlushAll(ctx)
}

func TestShouldNotCreateSegmentThatIsAlreadyExpired(t *testing.T) {
	rkvs := getRedisKeyValueStore()
	segments := NewRedisKeyValueSegments("test", bstest_retention_period, bstest_segment_interval, rkvs)
	ctx := context.Background()
	rkvs.rdb.FlushAll(ctx)
	streamTime := updateStreamTimeAndCreateSegment(ctx, 7, segments, t)
	ret, err := segments.GetOrCreateSegmentIfLive(ctx, 0, streamTime, segments.GetOrCreateSegment, segments.CleanupExpiredMeta)
	if err != nil {
		t.Fatal(err.Error())
	}
	if ret != nil {
		t.Fatalf("expired segment should not be created. got: %v", ret)
	}
	rkvs.rdb.FlushAll(ctx)
}

func TestShouldCleanupSegmentsThatHaveExpired(t *testing.T) {
	rkvs := getRedisKeyValueStore()
	segments := NewRedisKeyValueSegments("test", bstest_retention_period, bstest_segment_interval, rkvs)
	ctx := context.Background()
	rkvs.rdb.FlushAll(ctx)
	_, err := segments.GetOrCreateSegmentIfLive(ctx, 0, -1,
		segments.GetOrCreateSegment, segments.CleanupExpiredMeta)
	if err != nil {
		t.Fatal(err.Error())
	}

	_, err = segments.GetOrCreateSegmentIfLive(ctx, 1, -1,
		segments.GetOrCreateSegment, segments.CleanupExpiredMeta)
	if err != nil {
		t.Fatal(err.Error())
	}

	_, err = segments.GetOrCreateSegmentIfLive(ctx, 7, bstest_segment_interval*7,
		segments.GetOrCreateSegment, segments.CleanupExpiredMeta)
	if err != nil {
		t.Fatal(err.Error())
	}

	segs, err := segments.getSegments(ctx)
	if err != nil {
		t.Fatal(err.Error())
	}
	segSet := toStrSet(segs)

	if _, ok := segSet["test.0"]; ok {
		t.Fatal("test.0 should not exist")
	}
	expected := fmt.Sprintf("test.%d", bstest_segment_interval)
	if _, ok := segSet[expected]; ok {
		t.Fatalf("%s should not exists", expected)
	}
	expected = fmt.Sprintf("test.%d", bstest_segment_interval*7)
	if _, ok := segSet[expected]; !ok {
		t.Fatalf("%s should exists", expected)
	}
	rkvs.rdb.FlushAll(ctx)
}
*/
func updateStreamTimeAndCreateSegment(ctx context.Context, segment int64, segments Segments, t *testing.T) int64 {
	streamTime := bstest_segment_interval * segment
	_, err := segments.GetOrCreateSegmentIfLive(ctx, segment,
		int64(streamTime), segments.GetOrCreateSegment, segments.CleanupExpiredMeta)
	if err != nil {
		t.Fatal(err.Error())
	}
	return streamTime
}
