package snapshot_store

import (
	"context"
	"fmt"
	"os"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/hashfuncs"
	"strings"
	"time"

	"cs.utexas.edu/zjia/faas/types"
	"github.com/go-redis/redis/v9"
)

func getRedisAddr() []string {
	raw_addr := os.Getenv("REDIS_ADDR")
	return strings.Split(raw_addr, ",")
}

type RedisSnapshotStore struct {
	rdb_arr []*redis.Client
}

func NewRedisSnapshotStore(createSnapshot bool) RedisSnapshotStore {
	if createSnapshot {
		addr_arr := getRedisAddr()
		rdb_arr := make([]*redis.Client, len(addr_arr))
		for i := 0; i < len(addr_arr); i++ {
			rdb_arr[i] = redis.NewClient(&redis.Options{
				Addr:     addr_arr[i],
				Password: "", // no password set
				DB:       0,  // use default DB
			})
		}
		return RedisSnapshotStore{rdb_arr: rdb_arr}
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
	key := fmt.Sprintf("%s_%d", changelogTpName, logOff)
	idx := hashfuncs.NameHash(key) % uint64(len(rs.rdb_arr))
	return rs.rdb_arr[idx].Set(ctx, key, snapshot, time.Duration(13)*time.Second).Err()
}

func (rs *RedisSnapshotStore) GetSnapshot(ctx context.Context, changelogTpName string, logOff uint64) ([]byte, error) {
	key := fmt.Sprintf("%s_%d", changelogTpName, logOff)
	idx := hashfuncs.NameHash(key) % uint64(len(rs.rdb_arr))
	return rs.rdb_arr[idx].Get(ctx, key).Bytes()
}
