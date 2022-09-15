package snapshot_store

import (
	"context"
	"fmt"
	"os"
	"sharedlog-stream/pkg/commtypes"

	"cs.utexas.edu/zjia/faas/types"
	"github.com/go-redis/redis/v9"
)

func getRedisAddr() string {
	return os.Getenv("REDIS_ADDR")
}

type RedisSnapshotStore struct {
	rdb *redis.Client
}

func NewRedisSnapshotStore(createSnapshot bool) RedisSnapshotStore {
	if createSnapshot {
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

func (rs *RedisSnapshotStore) StoreSnapshot(ctx context.Context, env types.Environment,
	snapshot []byte, changelogTpName string, logOff uint64,
) error {
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
	return rs.rdb.Set(ctx, fmt.Sprintf("%s_%d", changelogTpName, logOff), snapshot, 0).Err()
}

func (rs *RedisSnapshotStore) GetSnapshot(ctx context.Context, changelogTpName string, logOff uint64) ([]byte, error) {
	return rs.rdb.Get(ctx, fmt.Sprintf("%s_%d", changelogTpName, logOff)).Bytes()
}
