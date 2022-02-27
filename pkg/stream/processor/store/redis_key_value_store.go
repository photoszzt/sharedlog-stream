package store

import (
	"context"
	"fmt"
	"sharedlog-stream/pkg/stream/processor/commtypes"

	"github.com/go-redis/redis/v8"
)

type RedisKeyValueStore struct {
	rdb                 *redis.Client
	config              *RedisConfig
	key_collection_name string
	collection_name     string
}

type RedisConfig struct {
	StoreName      string
	Addr           string
	Port           uint32
	Userid         int
	CollectionName string
	KeySerde       commtypes.Serde
	ValueSerde     commtypes.Serde
}

func NewRedisKeyValueStore(config *RedisConfig) *RedisKeyValueStore {
	rdb := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", config.Addr, config.Port),
		Password: "",
		DB:       config.Userid,
	})
	return &RedisKeyValueStore{
		rdb:                 rdb,
		config:              config,
		key_collection_name: fmt.Sprintf("%d-%s-keys", config.Userid, config.CollectionName),
		collection_name:     fmt.Sprintf("%d-%s", config.Userid, config.CollectionName),
	}
}

func (s *RedisKeyValueStore) IsOpen() bool {
	return true
}

func (s *RedisKeyValueStore) Init(ctx StoreContext) {
}

func (s *RedisKeyValueStore) Name() string {
	return s.config.StoreName
}

func (s *RedisKeyValueStore) Get(ctx context.Context, key KeyT) (ValueT, bool, error) {
	return s.GetWithCollection(ctx, key, s.collection_name)
}

func (s *RedisKeyValueStore) GetWithCollection(ctx context.Context, key KeyT, collection string) (ValueT, bool, error) {
	kBytes, err := s.config.KeySerde.Encode(key)
	if err != nil {
		return nil, false, err
	}
	vBytes, err := s.rdb.HGet(ctx, collection, string(kBytes)).Result()
	if err == redis.Nil {
		return nil, false, nil
	} else if err != nil {
		return nil, false, err
	}
	val, err := s.config.ValueSerde.Decode([]byte(vBytes))
	if err != nil {
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
	return s.ApproximateNumEntriesWithCollection(ctx, s.collection_name)
}

func (s *RedisKeyValueStore) ApproximateNumEntriesWithCollection(
	ctx context.Context, collection_name string,
) (uint64, error) {
	ret, err := s.rdb.HLen(ctx, collection_name).Result()
	if err != nil {
		return 0, err
	}
	return uint64(ret), nil
}

func (s *RedisKeyValueStore) Put(ctx context.Context, key KeyT, value ValueT) error {
	return s.PutWithCollection(ctx, key, value, s.collection_name, s.key_collection_name)
}

func (s *RedisKeyValueStore) PutWithCollection(ctx context.Context, key KeyT,
	value ValueT, collection string, key_collection string,
) error {
	if value == nil {
		return s.Delete(ctx, key)
	} else {
		// assume key and value to be bytes
		kBytes, err := s.config.KeySerde.Encode(key)
		if err != nil {
			return err
		}
		vBytes, err := s.config.ValueSerde.Encode(value)
		if err != nil {
			return err
		}
		key_str := string(kBytes)
		err = s.rdb.HSet(ctx, collection,
			map[string]interface{}{key_str: vBytes}).Err()
		if err != nil {
			return err
		}
		err = s.rdb.ZAdd(ctx, key_collection,
			&redis.Z{Score: 0, Member: key_str}).Err()
		return err
	}
}

func (s *RedisKeyValueStore) PutIfAbsent(ctx context.Context, key KeyT, value ValueT) (ValueT, error) {
	// assume key and value to be bytes
	panic("not implemented")
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
	return s.DeleteWithCollection(ctx, key, s.collection_name, s.key_collection_name)
}

func (s *RedisKeyValueStore) DeleteWithCollection(ctx context.Context,
	key KeyT, collection string, key_collection string,
) error {
	kBytes, err := s.config.KeySerde.Encode(key)
	if err != nil {
		return err
	}
	err = s.rdb.HDel(ctx, collection, string(kBytes)).Err()
	if err != nil {
		return err
	}
	err = s.rdb.ZRem(ctx, key_collection, string(kBytes)).Err()
	return err
}
