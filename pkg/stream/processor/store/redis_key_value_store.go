package store

import (
	"context"
	"fmt"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"time"

	"github.com/go-redis/redis/v8"
)

type RedisKeyValueStore struct {
	name string
	rdb  *redis.Client
	addr string
	port uint32
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
		addr: addr,
		port: port,
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
	if value == nil {
		return s.Delete(ctx, key)
	} else {
		// assume key and value to be bytes
		k := key.([]byte)
		v := value.([]byte)
		key_str := string(k)
		err := s.rdb.Set(ctx, key_str, v, 0).Err()
		if err != nil {
			return err
		}
		return nil
	}
}

func (s *RedisKeyValueStore) PutIfAbsent(ctx context.Context, key KeyT, value ValueT) (ValueT, error) {
	// assume key and value to be bytes
	k := key.([]byte)
	v := value.([]byte)
	key_str := string(k)
	ret, err := s.rdb.SetArgs(ctx, key_str, v, redis.SetArgs{
		Mode: "NX",
		TTL:  time.Duration(0),
		Get:  true,
	}).Result()
	if err != nil {
		return nil, err
	}
	return []byte(ret), nil
}

func (s *RedisKeyValueStore) PutAll(ctx context.Context, entries []*commtypes.Message) error {
	for _, msg := range entries {
		err := s.Put(ctx, msg.Key, msg.Value)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *RedisKeyValueStore) Delete(ctx context.Context, key KeyT) error {
	k := key.([]byte)
	key_str := string(k)
	err := s.rdb.Del(ctx, key_str).Err()
	return err
}
