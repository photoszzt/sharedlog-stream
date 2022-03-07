package store

import (
	"context"
	"fmt"
	"os"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/utils"

	"github.com/go-redis/redis/v8"
)

type RedisKeyValueStore struct {
	rdb                 *redis.Client
	config              *RedisConfig
	key_collection_name string
	collection_name     string
}

type RedisConfig struct {
	ValueSerde     commtypes.Serde
	KeySerde       commtypes.Serde
	Addr           string
	CollectionName string
	StoreName      string
	Userid         int
	Port           uint32
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

func (s *RedisKeyValueStore) Get(ctx context.Context, key commtypes.KeyT) (commtypes.ValueT, bool, error) {
	return s.GetWithCollection(ctx, key, s.collection_name)
}

func (s *RedisKeyValueStore) GetWithCollection(ctx context.Context, key commtypes.KeyT, collection string) (commtypes.ValueT, bool, error) {
	var err error
	kBytes, ok := key.([]byte)
	if !ok {
		kBytes, err = s.config.KeySerde.Encode(key)
		if err != nil {
			return nil, false, err
		}
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

func (s *RedisKeyValueStore) Range(ctx context.Context, from commtypes.KeyT, to commtypes.KeyT,
	iterFunc func(commtypes.KeyT, commtypes.ValueT) error,
) error {
	fromBytes, err := utils.ConvertToBytes(from, s.config.KeySerde)
	if err != nil {
		return err
	}
	toBytes, err := utils.ConvertToBytes(to, s.config.KeySerde)
	if err != nil {
		return err
	}
	fBytes := make([]byte, 0)
	fBytes = append(fBytes, byte('['))
	fBytes = append(fBytes, fromBytes...)
	tBytes := make([]byte, 0)
	tBytes = append(tBytes, byte('['))
	tBytes = append(tBytes, toBytes...)
	fmt.Fprintf(os.Stderr, "range from %v to %v\n", fBytes, tBytes)
	kRange, err := s.rdb.ZRangeArgs(ctx, redis.ZRangeArgs{
		Key:   s.key_collection_name,
		ByLex: true,
		Start: fBytes,
		Stop:  tBytes,
	}).Result()
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "return kRange: %v\n", kRange)

	ret, err := s.rdb.HMGet(ctx, s.collection_name, kRange...).Result()
	if err != nil {
		return err
	}
	for idx, k := range kRange {
		val := ret[idx]

		err := iterFunc(k, val)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *RedisKeyValueStore) ReverseRange(from commtypes.KeyT, to commtypes.KeyT, iterFunc func(commtypes.KeyT, commtypes.ValueT) error) error {
	panic("not implemented")
}

func (s *RedisKeyValueStore) PrefixScan(prefix interface{}, prefixKeyEncoder commtypes.Encoder,
	iterFunc func(commtypes.KeyT, commtypes.ValueT) error,
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

func (s *RedisKeyValueStore) Put(ctx context.Context, key commtypes.KeyT, value commtypes.ValueT) error {
	return s.PutWithCollection(ctx, key, value, s.collection_name, s.key_collection_name)
}

func (s *RedisKeyValueStore) PutWithCollection(ctx context.Context, key commtypes.KeyT,
	value commtypes.ValueT, collection string, key_collection string,
) error {
	if value == nil {
		return s.Delete(ctx, key)
	} else {
		// assume key and value to be bytes
		kBytes, ok := key.([]byte)
		var err error
		if !ok {
			kBytes, err = s.config.KeySerde.Encode(key)
			if err != nil {
				return err
			}
		}
		vBytes, ok := value.([]byte)
		if !ok {
			vBytes, err = s.config.ValueSerde.Encode(value)
			if err != nil {
				return err
			}
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

func (s *RedisKeyValueStore) PutIfAbsent(ctx context.Context, key commtypes.KeyT, value commtypes.ValueT) (commtypes.ValueT, error) {
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

func (s *RedisKeyValueStore) Delete(ctx context.Context, key commtypes.KeyT) error {
	return s.DeleteWithCollection(ctx, key, s.collection_name, s.key_collection_name)
}

func (s *RedisKeyValueStore) DeleteWithCollection(ctx context.Context,
	key commtypes.KeyT, collection string, key_collection string,
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
