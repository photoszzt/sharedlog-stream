package stream_task

import (
	"context"
	"os"
	"sharedlog-stream/pkg/commtypes"
	"strconv"

	"cs.utexas.edu/zjia/faas/types"
	"github.com/go-redis/redis/v9"
)

func getRedisAddr() string {
	return os.Getenv("REDIS_ADDR")
}

type RedisSnapshotStore struct {
	rdb *redis.Client
}

func NewRedisSnapshotStore() RedisSnapshotStore {
	if CREATE_SNAPSHOT {
		return RedisSnapshotStore{
			rdb: redis.NewClient(&redis.Options{
				Addr:     getRedisAddr(),
				Password: "", // no password set
				DB:       0,  // use default DB
			}),
		}
	} else {
		return RedisSnapshotStore{}
	}
}

func (rs *RedisSnapshotStore) StoreSnapshot(ctx context.Context, env types.Environment, snapshot []byte, logOff uint64) error {
	var uint64Serde commtypes.Uint16Serde
	hasData := uint16(1)
	enc, err := uint64Serde.Encode(hasData)
	if err != nil {
		return err
	}
	err = env.SharedLogSetAuxData(ctx, logOff, enc)
	if err != nil {
		return err
	}
	return rs.rdb.Set(ctx, strconv.FormatUint(logOff, 10), snapshot, 0).Err()
}

func (rs *RedisSnapshotStore) GetSnapshot(ctx context.Context, logOff uint64) ([]byte, error) {
	return rs.rdb.Get(ctx, strconv.FormatUint(logOff, 10)).Bytes()
}
