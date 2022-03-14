package store

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"sharedlog-stream/pkg/debug"
	"testing"
)

func toStrSet(data []string) map[string]struct{} {
	d := make(map[string]struct{})
	for _, s := range data {
		d[s] = struct{}{}
	}
	return d
}

func checkPut(ctx context.Context, store *SegmentedWindowStore,
	segments Segments, t testing.TB, expected map[string]struct{}) {
	segs, err := segments.GetSegmentNamesFromRemote(ctx)
	if err != nil {
		t.Fatalf("get segs failed: %v", err)
	}
	segs_s := toStrSet(segs)
	if eq := reflect.DeepEqual(expected, segs_s); !eq {
		t.Fatalf("should equal. expected: %v, got: %v", expected, segs_s)
	}
}

func RollingTest(ctx context.Context, store *SegmentedWindowStore, segments Segments, t testing.TB) {
	startTime := TEST_SEGMENT_INTERVAL * 2
	increment := TEST_SEGMENT_INTERVAL / 2

	store.Put(ctx, uint32(0), "zero", startTime)
	names := map[string]struct{}{segments.SegmentName(2): {}}
	debug.Fprint(os.Stderr, "1\n")
	checkPut(ctx, store, segments, t, names)

	store.Put(ctx, uint32(1), "one", startTime+increment)
	names = map[string]struct{}{segments.SegmentName(2): {}}
	debug.Fprint(os.Stderr, "2\n")
	checkPut(ctx, store, segments, t, names)

	store.Put(ctx, uint32(2), "two", startTime+increment*2)
	names = map[string]struct{}{segments.SegmentName(2): {}, segments.SegmentName(3): {}}
	debug.Fprint(os.Stderr, "3\n")
	checkPut(ctx, store, segments, t, names)

	store.Put(ctx, uint32(4), "four", startTime+increment*4)
	names = map[string]struct{}{
		segments.SegmentName(2): {},
		segments.SegmentName(3): {},
		segments.SegmentName(4): {},
	}
	debug.Fprint(os.Stderr, "4\n")
	checkPut(ctx, store, segments, t, names)

	fmt.Fprint(os.Stderr, "4\n")
	store.Put(ctx, uint32(5), "five", startTime+increment*5)
	names = map[string]struct{}{
		segments.SegmentName(2): {},
		segments.SegmentName(3): {},
		segments.SegmentName(4): {},
	}
	checkPut(ctx, store, segments, t, names)

	resM, err := assertFetch(ctx, store, 0, startTime-TEST_WINDOW_SIZE, startTime+TEST_WINDOW_SIZE)
	if err != nil {
		t.Fatalf("fail to fetch: %v", err)
	}
	expected := map[string]struct{}{"zero": {}}
	if !reflect.DeepEqual(expected, resM) {
		t.Fatalf("should equal. expected: %v, got: %v", expected, resM)
	}

	resM, err = assertFetch(ctx, store, 1, startTime-TEST_WINDOW_SIZE+increment, startTime+TEST_WINDOW_SIZE+increment)
	if err != nil {
		t.Fatalf("fail to fetch: %v", err)
	}
	expected = map[string]struct{}{"one": {}}
	if !reflect.DeepEqual(expected, resM) {
		t.Fatal("should equal")
	}

	resM, err = assertFetch(ctx, store, 2, startTime-TEST_WINDOW_SIZE+increment*2, startTime+TEST_WINDOW_SIZE+increment*2)
	if err != nil {
		t.Fatalf("fail to fetch: %v", err)
	}
	expected = map[string]struct{}{"two": {}}
	if !reflect.DeepEqual(expected, resM) {
		t.Fatal("should equal")
	}

	resM, err = assertFetch(ctx, store, 3, startTime-TEST_WINDOW_SIZE+increment*3, startTime+TEST_WINDOW_SIZE+increment*3)
	if err != nil {
		t.Fatalf("fail to fetch: %v", err)
	}
	expected = map[string]struct{}{}
	if !reflect.DeepEqual(expected, resM) {
		t.Fatal("should equal")
	}

	resM, err = assertFetch(ctx, store, 4, startTime-TEST_WINDOW_SIZE+increment*4, startTime+TEST_WINDOW_SIZE+increment*4)
	if err != nil {
		t.Fatalf("fail to fetch: %v", err)
	}
	expected = map[string]struct{}{"four": {}}
	if !reflect.DeepEqual(expected, resM) {
		t.Fatal("should equal")
	}

	resM, err = assertFetch(ctx, store, 5, startTime-TEST_WINDOW_SIZE+increment*5, startTime+TEST_WINDOW_SIZE+increment*5)
	if err != nil {
		t.Fatalf("fail to fetch: %v", err)
	}
	expected = map[string]struct{}{"five": {}}
	if !reflect.DeepEqual(expected, resM) {
		t.Fatal("should equal")
	}

	store.Put(ctx, uint32(6), "six", startTime+increment*6)
	names = map[string]struct{}{
		segments.SegmentName(3): {},
		segments.SegmentName(4): {},
		segments.SegmentName(5): {},
	}
	checkPut(ctx, store, segments, t, names)

	resM, err = assertFetch(ctx, store, 0, startTime-TEST_WINDOW_SIZE, startTime+TEST_WINDOW_SIZE)
	if err != nil {
		t.Fatalf("fail to fetch: %v", err)
	}
	expected = map[string]struct{}{}
	if !reflect.DeepEqual(expected, resM) {
		t.Fatal("should equal")
	}

	resM, err = assertFetch(ctx, store, 1, startTime-TEST_WINDOW_SIZE+increment, startTime+TEST_WINDOW_SIZE+increment)
	if err != nil {
		t.Fatalf("fail to fetch: %v", err)
	}
	expected = map[string]struct{}{}
	if !reflect.DeepEqual(expected, resM) {
		t.Fatal("should equal")
	}

	resM, err = assertFetch(ctx, store, 2, startTime-TEST_WINDOW_SIZE+increment*2, startTime+TEST_WINDOW_SIZE+increment*2)
	if err != nil {
		t.Fatalf("fail to fetch: %v", err)
	}
	expected = map[string]struct{}{"two": {}}
	if !reflect.DeepEqual(expected, resM) {
		t.Fatal("should equal")
	}

	resM, err = assertFetch(ctx, store, 3, startTime-TEST_WINDOW_SIZE+increment*3, startTime+TEST_WINDOW_SIZE+increment*3)
	if err != nil {
		t.Fatalf("fail to fetch: %v", err)
	}
	expected = map[string]struct{}{}
	if !reflect.DeepEqual(expected, resM) {
		t.Fatal("should equal")
	}

	resM, err = assertFetch(ctx, store, 4, startTime-TEST_WINDOW_SIZE+increment*4, startTime+TEST_WINDOW_SIZE+increment*4)
	if err != nil {
		t.Fatalf("fail to fetch: %v", err)
	}
	expected = map[string]struct{}{"four": {}}
	if !reflect.DeepEqual(expected, resM) {
		t.Fatal("should equal")
	}

	resM, err = assertFetch(ctx, store, 5, startTime-TEST_WINDOW_SIZE+increment*5, startTime+TEST_WINDOW_SIZE+increment*5)
	if err != nil {
		t.Fatalf("fail to fetch: %v", err)
	}
	expected = map[string]struct{}{"five": {}}
	if !reflect.DeepEqual(expected, resM) {
		t.Fatal("should equal")
	}

	resM, err = assertFetch(ctx, store, 6, startTime-TEST_WINDOW_SIZE+increment*6, startTime+TEST_WINDOW_SIZE+increment*6)
	if err != nil {
		t.Fatalf("fail to fetch: %v", err)
	}
	expected = map[string]struct{}{"six": {}}
	if !reflect.DeepEqual(expected, resM) {
		t.Fatal("should equal")
	}

	store.Put(ctx, uint32(7), "seven", startTime+increment*7)
	names = map[string]struct{}{
		segments.SegmentName(3): {},
		segments.SegmentName(4): {},
		segments.SegmentName(5): {},
	}
	checkPut(ctx, store, segments, t, names)

	resM, err = assertFetch(ctx, store, 0, startTime-TEST_WINDOW_SIZE, startTime+TEST_WINDOW_SIZE)
	if err != nil {
		t.Fatalf("fail to fetch: %v", err)
	}
	expected = map[string]struct{}{}
	if !reflect.DeepEqual(expected, resM) {
		t.Fatal("should equal")
	}

	resM, err = assertFetch(ctx, store, 1, startTime-TEST_WINDOW_SIZE+increment, startTime+TEST_WINDOW_SIZE+increment)
	if err != nil {
		t.Fatalf("fail to fetch: %v", err)
	}
	expected = map[string]struct{}{}
	if !reflect.DeepEqual(expected, resM) {
		t.Fatal("should equal")
	}

	resM, err = assertFetch(ctx, store, 2, startTime-TEST_WINDOW_SIZE+increment*2, startTime+TEST_WINDOW_SIZE+increment*2)
	if err != nil {
		t.Fatalf("fail to fetch: %v", err)
	}
	expected = map[string]struct{}{"two": {}}
	if !reflect.DeepEqual(expected, resM) {
		t.Fatal("should equal")
	}

	resM, err = assertFetch(ctx, store, 3, startTime-TEST_WINDOW_SIZE+increment*3, startTime+TEST_WINDOW_SIZE+increment*3)
	if err != nil {
		t.Fatalf("fail to fetch: %v", err)
	}
	expected = map[string]struct{}{}
	if !reflect.DeepEqual(expected, resM) {
		t.Fatal("should equal")
	}

	resM, err = assertFetch(ctx, store, 4, startTime-TEST_WINDOW_SIZE+increment*4, startTime+TEST_WINDOW_SIZE+increment*4)
	if err != nil {
		t.Fatalf("fail to fetch: %v", err)
	}
	expected = map[string]struct{}{"four": {}}
	if !reflect.DeepEqual(expected, resM) {
		t.Fatal("should equal")
	}

	resM, err = assertFetch(ctx, store, 5, startTime-TEST_WINDOW_SIZE+increment*5, startTime+TEST_WINDOW_SIZE+increment*5)
	if err != nil {
		t.Fatalf("fail to fetch: %v", err)
	}
	expected = map[string]struct{}{"five": {}}
	if !reflect.DeepEqual(expected, resM) {
		t.Fatal("should equal")
	}

	resM, err = assertFetch(ctx, store, 6, startTime-TEST_WINDOW_SIZE+increment*6, startTime+TEST_WINDOW_SIZE+increment*6)
	if err != nil {
		t.Fatalf("fail to fetch: %v", err)
	}
	expected = map[string]struct{}{"six": {}}
	if !reflect.DeepEqual(expected, resM) {
		t.Fatal("should equal")
	}

	resM, err = assertFetch(ctx, store, 7, startTime-TEST_WINDOW_SIZE+increment*7, startTime+TEST_WINDOW_SIZE+increment*7)
	if err != nil {
		t.Fatalf("fail to fetch: %v", err)
	}
	expected = map[string]struct{}{"seven": {}}
	if !reflect.DeepEqual(expected, resM) {
		t.Fatal("should equal")
	}

	store.Put(ctx, uint32(8), "eight", startTime+increment*8)
	names = map[string]struct{}{
		segments.SegmentName(4): {},
		segments.SegmentName(5): {},
		segments.SegmentName(6): {},
	}
	checkPut(ctx, store, segments, t, names)

	resM, err = assertFetch(ctx, store, 0, startTime-TEST_WINDOW_SIZE, startTime+TEST_WINDOW_SIZE)
	if err != nil {
		t.Fatalf("fail to fetch: %v", err)
	}
	expected = map[string]struct{}{}
	if !reflect.DeepEqual(expected, resM) {
		t.Fatal("should equal")
	}

	resM, err = assertFetch(ctx, store, 1, startTime-TEST_WINDOW_SIZE+increment, startTime+TEST_WINDOW_SIZE+increment)
	if err != nil {
		t.Fatalf("fail to fetch: %v", err)
	}
	expected = map[string]struct{}{}
	if !reflect.DeepEqual(expected, resM) {
		t.Fatal("should equal")
	}

	resM, err = assertFetch(ctx, store, 2, startTime-TEST_WINDOW_SIZE+increment*2, startTime+TEST_WINDOW_SIZE+increment*2)
	if err != nil {
		t.Fatalf("fail to fetch: %v", err)
	}
	expected = map[string]struct{}{}
	if !reflect.DeepEqual(expected, resM) {
		t.Fatal("should equal")
	}

	resM, err = assertFetch(ctx, store, 3, startTime-TEST_WINDOW_SIZE+increment*3, startTime+TEST_WINDOW_SIZE+increment*3)
	if err != nil {
		t.Fatalf("fail to fetch: %v", err)
	}
	expected = map[string]struct{}{}
	if !reflect.DeepEqual(expected, resM) {
		t.Fatal("should equal")
	}

	resM, err = assertFetch(ctx, store, 4, startTime-TEST_WINDOW_SIZE+increment*4, startTime+TEST_WINDOW_SIZE+increment*4)
	if err != nil {
		t.Fatalf("fail to fetch: %v", err)
	}
	expected = map[string]struct{}{"four": {}}
	if !reflect.DeepEqual(expected, resM) {
		t.Fatal("should equal")
	}

	resM, err = assertFetch(ctx, store, 5, startTime-TEST_WINDOW_SIZE+increment*5, startTime+TEST_WINDOW_SIZE+increment*5)
	if err != nil {
		t.Fatalf("fail to fetch: %v", err)
	}
	expected = map[string]struct{}{"five": {}}
	if !reflect.DeepEqual(expected, resM) {
		t.Fatal("should equal")
	}

	resM, err = assertFetch(ctx, store, 6, startTime-TEST_WINDOW_SIZE+increment*6, startTime+TEST_WINDOW_SIZE+increment*6)
	if err != nil {
		t.Fatalf("fail to fetch: %v", err)
	}
	expected = map[string]struct{}{"six": {}}
	if !reflect.DeepEqual(expected, resM) {
		t.Fatal("should equal")
	}

	resM, err = assertFetch(ctx, store, 7, startTime-TEST_WINDOW_SIZE+increment*7, startTime+TEST_WINDOW_SIZE+increment*7)
	if err != nil {
		t.Fatalf("fail to fetch: %v", err)
	}
	expected = map[string]struct{}{"seven": {}}
	if !reflect.DeepEqual(expected, resM) {
		t.Fatal("should equal")
	}

	resM, err = assertFetch(ctx, store, 8, startTime-TEST_WINDOW_SIZE+increment*8, startTime+TEST_WINDOW_SIZE+increment*8)
	if err != nil {
		t.Fatalf("fail to fetch: %v", err)
	}
	expected = map[string]struct{}{"eight": {}}
	if !reflect.DeepEqual(expected, resM) {
		t.Fatal("should equal")
	}

	names = map[string]struct{}{
		segments.SegmentName(4): {},
		segments.SegmentName(5): {},
		segments.SegmentName(6): {},
	}
	checkPut(ctx, store, segments, t, names)
}
