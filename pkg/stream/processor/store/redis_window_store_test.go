package store

/*
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
	byteStore := NewRedisSegmentedBytesStore("test1", "test1",
		TEST_RETENTION_PERIOD, &WindowKeySchema{}, rkvs)
	store := NewSegmentedWindowStore(byteStore, retainDuplicates, TEST_WINDOW_SIZE, commtypes.Uint32Serde{}, commtypes.StringSerde{})
	return store, rkvs
}

func TestRedisGetAndRange(t *testing.T) {
	store, rkvs := getRedisWindowStore(false)
	ctx := context.Background()
	GetAndRangeTest(ctx, store, t)
	rkvs.rdb.FlushAll(ctx)
}

func TestRedisShouldGetAllNonDeletedMsgs(t *testing.T) {
	ctx := context.Background()
	store, rkvs := getRedisWindowStore(false)
	ShouldGetAllNonDeletedMsgsTest(ctx, store, t)
	rkvs.rdb.FlushAll(ctx)
}

func TestRedisExpiration(t *testing.T) {
	ctx := context.Background()
	store, rkvs := getRedisWindowStore(false)
	ExpirationTest(ctx, store, t)
	rkvs.rdb.FlushAll(ctx)
}

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

func TestRedisFetchRange(t *testing.T) {
	store, rkvs := getRedisWindowStore(false)
	ctx := context.Background()
	FetchRangeTest(ctx, store, t)
	rkvs.rdb.FlushAll(ctx)
}

func TestRedisPutAndFetchBefore(t *testing.T) {
	store, rkvs := getRedisWindowStore(false)
	ctx := context.Background()
	PutAndFetchBeforeTest(ctx, store, t)
	rkvs.rdb.FlushAll(ctx)
}

func TestRedisPutAndFetchAfter(t *testing.T) {
	store, rkvs := getRedisWindowStore(false)
	ctx := context.Background()
	PutAndFetchAfterTest(ctx, store, t)
	rkvs.rdb.FlushAll(ctx)
}

func TestRedisPutSameKeyTs(t *testing.T) {
	store, rkvs := getRedisWindowStore(true)
	ctx := context.Background()
	PutSameKeyTsTest(ctx, store, t)
	rkvs.rdb.FlushAll(ctx)
}
*/
