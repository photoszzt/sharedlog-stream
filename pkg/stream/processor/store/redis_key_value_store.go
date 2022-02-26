package store

import (
	"context"
	"fmt"
	"sharedlog-stream/pkg/stream/processor/commtypes"

	"github.com/go-redis/redis/v8"
)

type RedisKeyValueStore struct {
	name string
	rdb  *redis.Client
}

var _ = KeyValueStore(&RedisKeyValueStore{})

func NewRedisKeyValueStore(name string, addr string, port uint32) *RedisKeyValueStore {
	rdb := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", addr, port),
		Password: "",
		DB:       0,
	})
	return &RedisKeyValueStore{
		name: name,
		rdb:  rdb,
	}
}

func (s *RedisKeyValueStore) IsOpen() bool {
	return true
}

func (s *RedisKeyValueStore) Init(ctx StoreContext) {
}

func (s *RedisKeyValueStore) Name() string {
	return s.name
}

func (s *RedisKeyValueStore) Get(ctx context.Context, key KeyT) (ValueT, bool, error) {
	keyS := key.(fmt.Stringer)
	key_str := keyS.String()
	val, err := s.rdb.Get(ctx, key_str).Result()
	if err == redis.Nil {
		return nil, false, nil
	} else if err != nil {
		return nil, false, err
	}
	return val, true, nil
}

func (s *RedisKeyValueStore) Range(from KeyT, to KeyT, iterFunc func(KeyT, ValueT) error) error {
	panic("not implemented")
}

func (s *RedisKeyValueStore) ReverseRange(from KeyT, to KeyT, iterFunc func(KeyT, ValueT) error) error {
	panic("not implemented")
}

func (s *RedisKeyValueStore) PrefixScan(prefix interface{}, prefixKeyEncoder commtypes.Encoder,
	iterFunc func(KeyT, ValueT) error,
) error {
	panic("not implemented")
}

func (s *RedisKeyValueStore) ApproximateNumEntries(ctx context.Context) (uint64, error) {
	ret, err := s.rdb.DBSize(ctx).Result()
	if err != nil {
		return 0, err
	}
	return uint64(ret), nil
}

func (s *RedisKeyValueStore) Put(ctx context.Context, key KeyT, value ValueT) error {

	return nil
}

func (s *RedisKeyValueStore) PutIfAbsent(ctx context.Context, key KeyT, value ValueT) (ValueT, error) {
	return nil, nil
}

func (s *RedisKeyValueStore) PutAll(context.Context, []*commtypes.Message) error {
	return nil
}

func (s *RedisKeyValueStore) Delete(ctx context.Context, key KeyT) error {
	return nil
}
