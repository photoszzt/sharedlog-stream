package store

import (
	"context"
	"fmt"
	"sharedlog-stream/pkg/stream/processor/commtypes"

	"github.com/go-redis/redis/v8"
)

type RedisKeyValueSegment struct {
	rkvs        *RedisKeyValueStore
	seg_key     string
	segmentName string
	windowName  string
}

var _ = Segment(&RedisKeyValueSegment{})

func NewRedisKeyValueSegment(ctx context.Context,
	rkvs *RedisKeyValueStore,
	segmentName string,
	name string,
) (*RedisKeyValueSegment, error) {
	err := rkvs.rdb.ZAdd(ctx, name, &redis.Z{Score: 0, Member: segmentName}).Err()
	if err != nil {
		return nil, err
	}
	return &RedisKeyValueSegment{
		rkvs:        rkvs,
		seg_key:     fmt.Sprintf("%s-keys", segmentName),
		segmentName: segmentName,
		windowName:  name,
	}, nil
}

func (rkvs *RedisKeyValueSegment) Init(ctx StoreContext) {}
func (rkvs *RedisKeyValueSegment) IsOpen() bool          { return true }
func (rkvs *RedisKeyValueSegment) Name() string {
	return rkvs.rkvs.Name()
}
func (rkvs *RedisKeyValueSegment) Get(ctx context.Context, key commtypes.KeyT) (commtypes.ValueT, bool, error) {
	return rkvs.rkvs.GetWithCollection(ctx, key, rkvs.segmentName)
}

func (rkvs *RedisKeyValueSegment) Range(ctx context.Context, from commtypes.KeyT, to commtypes.KeyT, iterFunc func(commtypes.KeyT, commtypes.ValueT) error) error {
	return rkvs.rkvs.Range(ctx, from, to, iterFunc)
}

func (rkvs *RedisKeyValueSegment) ReverseRange(from commtypes.KeyT, to commtypes.KeyT, iterFunc func(commtypes.KeyT, commtypes.ValueT) error) error {
	panic("not implemented")
}

func (rkvs *RedisKeyValueSegment) PrefixScan(prefix interface{}, prefixKeyEncoder commtypes.Encoder, iterFunc func(commtypes.KeyT, commtypes.ValueT) error) error {
	panic("not implemented")
}

func (rkvs *RedisKeyValueSegment) ApproximateNumEntries(ctx context.Context) (uint64, error) {
	return rkvs.rkvs.ApproximateNumEntriesWithCollection(ctx, rkvs.segmentName)
}

func (rkvs *RedisKeyValueSegment) Put(ctx context.Context, key commtypes.KeyT, value commtypes.ValueT) error {
	return rkvs.rkvs.PutWithCollection(ctx, key, value, rkvs.segmentName, rkvs.seg_key)
}

func (rkvs *RedisKeyValueSegment) PutIfAbsent(ctx context.Context, key commtypes.KeyT, value commtypes.ValueT) (commtypes.ValueT, error) {
	panic("not implemented")
}

func (rkvs *RedisKeyValueSegment) PutAll(context.Context, []*commtypes.Message) error {
	panic("not implemented")
}

func (rkvs *RedisKeyValueSegment) Delete(ctx context.Context, key commtypes.KeyT) error {
	return rkvs.rkvs.DeleteWithCollection(ctx, key, rkvs.segmentName, rkvs.rkvs.key_collection_name)
}

func (rkvs *RedisKeyValueSegment) Destroy(ctx context.Context) error {
	keys, err := rkvs.rkvs.rdb.ZRange(ctx, rkvs.seg_key, 0, -1).Result()
	if err != nil {
		return err
	}
	err = rkvs.rkvs.rdb.HDel(ctx, rkvs.segmentName, keys...).Err()
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
	err = rkvs.rkvs.rdb.HDel(ctx, rkvs.segmentName, keys...).Err()
	if err != nil {
		return err
	}
	err = rkvs.rkvs.rdb.ZRem(ctx, rkvs.seg_key, keys).Err()
	return err
}
