package store

import (
	"sharedlog-stream/pkg/stream/processor/commtypes"
)

func getRedisWindowStore(retainDuplicates bool) (*SegmentedWindowStore, *RedisKeyValueStore) {
	rkvs := NewRedisKeyValueStore(&RedisConfig{
		Addr:           "127.0.0.1",
		Port:           6379,
		StoreName:      "test1",
		Userid:         0,
		CollectionName: "a",
		KeySerde:       commtypes.Uint32Serde{},
		ValueSerde:     commtypes.StringSerde{},
	})
	byteStore := NewRedisSegmentedBytesStore("test1",
		TEST_RETENTION_PERIOD, &WindowKeySchema{}, rkvs)
	store := NewSegmentedWindowStore(byteStore, retainDuplicates, TEST_WINDOW_SIZE, commtypes.Uint32Serde{}, commtypes.StringSerde{})
	return store, rkvs
}

/*
func TestRedisGetAndRange(t *testing.T) {
	store, rkvs := getRedisWindowStore(false)
	ctx := context.Background()
	rkvs.rdb.FlushAll(ctx)
	GetAndRangeTest(ctx, store, t)
	rkvs.rdb.FlushAll(ctx)
}


func TestRedisShouldGetAllNonDeletedMsgs(t *testing.T) {
	ctx := context.Background()
	store, rkvs := getRedisWindowStore(false)
	rkvs.rdb.FlushAll(ctx)
	ShouldGetAllNonDeletedMsgsTest(ctx, store, t)
	rkvs.rdb.FlushAll(ctx)
}

func TestRedisExpiration(t *testing.T) {
	ctx := context.Background()
	store, rkvs := getRedisWindowStore(false)
	rkvs.rdb.FlushAll(ctx)
	ExpirationTest(ctx, store, t)
	rkvs.rdb.FlushAll(ctx)
}
*/

/*
func TestRedisRedisShouldGetAll(t *testing.T) {
	ctx := context.Background()
	store, rkvs := getRedisWindowStore(false)
	ShouldGetAllTest(ctx, store, t)
	rkvs.rdb.FlushAll(ctx)
}


func TestRedisShouldGetAllReturnTimestampOrdered(t *testing.T) {
	ctx := context.Background()
	store, rkvs := getRedisWindowStore(false)
	ShouldGetAllReturnTimestampOrderedTest(ctx, store, t)
	rkvs.rdb.FlushAll(ctx)
}
*/

/*
func TestRedisFetchRange(t *testing.T) {
	store, rkvs := getRedisWindowStore(false)
	ctx := context.Background()
	rkvs.rdb.FlushAll(ctx)
	FetchRangeTest(ctx, store, t)
	rkvs.rdb.FlushAll(ctx)
}

func TestRedisPutAndFetchBefore(t *testing.T) {
	store, rkvs := getRedisWindowStore(false)
	ctx := context.Background()
	rkvs.rdb.FlushAll(ctx)
	PutAndFetchBeforeTest(ctx, store, t)
	rkvs.rdb.FlushAll(ctx)
}

func TestRedisPutAndFetchAfter(t *testing.T) {
	store, rkvs := getRedisWindowStore(false)
	ctx := context.Background()
	rkvs.rdb.FlushAll(ctx)
	PutAndFetchAfterTest(ctx, store, t)
	rkvs.rdb.FlushAll(ctx)
}

func TestRedisPutSameKeyTs(t *testing.T) {
	store, rkvs := getRedisWindowStore(true)
	ctx := context.Background()
	rkvs.rdb.FlushAll(ctx)
	PutSameKeyTsTest(ctx, store, t)
	rkvs.rdb.FlushAll(ctx)
}

func TestRolling(t *testing.T) {
	store, rkvs := getRedisWindowStore(false)
	ctx := context.Background()
	rkvs.rdb.FlushAll(ctx)
	segmentInterval := TEST_RETENTION_PERIOD / 2
	if segmentInterval < 60_000 {
		segmentInterval = 60_000
	}
	segments := NewRedisKeyValueSegments("test1", TEST_RETENTION_PERIOD, segmentInterval, rkvs)
	RollingTest(ctx, store, segments, t)
	rkvs.rdb.FlushAll(ctx)
}
*/
