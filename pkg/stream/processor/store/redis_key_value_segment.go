package store

import (
	"context"
	"fmt"
	"sharedlog-stream/pkg/stream/processor/commtypes"

	"github.com/go-redis/redis/v8"
)

type RedisKeyValueSegment struct {
	rkvs        *RedisKeyValueStore
	seg         string
	seg_key     string
	segmentName string
	windowName  string
}

var _ = Segment(&RedisKeyValueSegment{})

func NewRedisKeyValueSegment(ctx context.Context, rkvs *RedisKeyValueStore, segmentName string, windowName string,
	state_name string,
) (*RedisKeyValueSegment, error) {
	err := rkvs.rdb.ZAdd(ctx, windowName, &redis.Z{Score: 0, Member: segmentName}).Err()
	if err != nil {
		return nil, err
	}
	err = rkvs.rdb.ZAdd(ctx, state_name, &redis.Z{Score: 0, Member: windowName}).Err()
	if err != nil {
		return nil, err
	}
	return &RedisKeyValueSegment{
		rkvs:        rkvs,
		seg:         fmt.Sprintf("%s##%s", segmentName, windowName),
		seg_key:     fmt.Sprintf("%s##%s-keys", segmentName, windowName),
		segmentName: segmentName,
		windowName:  windowName,
	}, nil
}

func (rkvs *RedisKeyValueSegment) Init(ctx StoreContext) {}
func (rkvs *RedisKeyValueSegment) IsOpen() bool          { return true }
func (rkvs *RedisKeyValueSegment) Name() string {
	return rkvs.rkvs.Name()
}
func (rkvs *RedisKeyValueSegment) Get(ctx context.Context, key KeyT) (ValueT, bool, error) {
	return rkvs.rkvs.GetWithCollection(ctx, key, rkvs.seg)
}

func (rkvs *RedisKeyValueSegment) Range(from KeyT, to KeyT, iterFunc func(KeyT, ValueT) error) error {
	panic("not implemented")
}

func (rkvs *RedisKeyValueSegment) ReverseRange(from KeyT, to KeyT, iterFunc func(KeyT, ValueT) error) error {
	panic("not implemented")
}

func (rkvs *RedisKeyValueSegment) PrefixScan(prefix interface{}, prefixKeyEncoder commtypes.Encoder, iterFunc func(KeyT, ValueT) error) error {
	panic("not implemented")
}

func (rkvs *RedisKeyValueSegment) ApproximateNumEntries(ctx context.Context) (uint64, error) {
	return rkvs.rkvs.ApproximateNumEntriesWithCollection(ctx, rkvs.seg)
}

func (rkvs *RedisKeyValueSegment) Put(ctx context.Context, key KeyT, value ValueT) error {
	return rkvs.rkvs.PutWithCollection(ctx, key, value, rkvs.seg, rkvs.seg_key)
}

func (rkvs *RedisKeyValueSegment) PutIfAbsent(ctx context.Context, key KeyT, value ValueT) (ValueT, error) {
	panic("not implemented")
}

func (rkvs *RedisKeyValueSegment) PutAll(context.Context, []*commtypes.Message) error {
	panic("not implemented")
}

func (rkvs *RedisKeyValueSegment) Delete(ctx context.Context, key KeyT) error {
	return rkvs.rkvs.DeleteWithCollection(ctx, key, rkvs.seg, rkvs.rkvs.key_collection_name)
}

func (rkvs *RedisKeyValueSegment) Destroy(ctx context.Context) error {
	keys, err := rkvs.rkvs.rdb.ZRange(ctx, rkvs.seg_key, 0, -1).Result()
	if err != nil {
		return err
	}
	err = rkvs.rkvs.rdb.HDel(ctx, rkvs.seg, keys...).Err()
	if err != nil {
		return err
	}
	err = rkvs.rkvs.rdb.ZRem(ctx, rkvs.seg_key, keys).Err()
	return err
}

func (rkvs *RedisKeyValueSegment) DeleteRange(ctx context.Context, keyFrom interface{}, keyTo interface{}) error {
	kFromBytes := keyFrom.([]byte)
	kToBytes := keyTo.([]byte)
	keys, err := rkvs.rkvs.rdb.ZRangeArgs(ctx, redis.ZRangeArgs{
		ByLex: true,
		Key:   rkvs.seg_key,
		Start: fmt.Sprintf("[%v", kFromBytes),
		Stop:  fmt.Sprintf("[%v", kToBytes),
	}).Result()
	if err != nil {
		return err
	}
	err = rkvs.rkvs.rdb.HDel(ctx, rkvs.seg, keys...).Err()
	if err != nil {
		return err
	}
	err = rkvs.rkvs.rdb.ZRem(ctx, rkvs.seg_key, keys).Err()
	return err
}
