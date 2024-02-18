package snapshot_store

import (
	"context"
	"fmt"
	"os"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/hashfuncs"
	"sharedlog-stream/pkg/redis_client"
	"strconv"
	"strings"
	"time"

	"cs.utexas.edu/zjia/faas/types"
	"github.com/go-redis/redis/v9"
)

type RedisSnapshotStore struct {
	rdb_arr []*redis.Client
}

func NewRedisSnapshotStore(createSnapshot bool) RedisSnapshotStore {
	if createSnapshot {
		return RedisSnapshotStore{rdb_arr: redis_client.GetRedisClients()}
	} else {
		return RedisSnapshotStore{}
	}
}

func (rs *RedisSnapshotStore) StoreAlignChkpt(ctx context.Context, snapshot []byte, srcLogOff []commtypes.TpLogOff, storeName string) error {
	var keys []string
	for _, tpLogOff := range srcLogOff {
		idx := hashfuncs.NameHash(tpLogOff.Tp) % uint64(len(rs.rdb_arr))
		err := rs.rdb_arr[idx].RPush(ctx, tpLogOff.Tp, tpLogOff.LogOff).Err()
		if err != nil {
			return err
		}
		keys = append(keys, fmt.Sprintf("%s_%#x", tpLogOff.Tp, tpLogOff.LogOff))
	}
	keys = append(keys, storeName)
	key := strings.Join(keys, "-")
	idx := hashfuncs.NameHash(key) % uint64(len(rs.rdb_arr))
	fmt.Fprintf(os.Stderr, "store snapshot key: %s at redis[%d]\n", key, idx)
	return rs.rdb_arr[idx].Set(ctx, key, snapshot, 0).Err()
}

func (rs *RedisSnapshotStore) GetAlignChkpt(ctx context.Context, srcs []string, storeName string) ([]byte, error) {
	var keys []string
	for _, tp := range srcs {
		idx := hashfuncs.NameHash(tp) % uint64(len(rs.rdb_arr))
		logOffStrs, err := rs.rdb_arr[idx].LRange(ctx, tp, -1, -1).Result()
		if err != nil {
			return nil, err
		}
		logOff, err := strconv.Atoi(logOffStrs[0])
		if err != nil {
			return nil, err
		}
		keys = append(keys, fmt.Sprintf("%s_%#x", tp, logOff))
	}
	keys = append(keys, storeName)
	key := strings.Join(keys, "-")
	idx := hashfuncs.NameHash(key) % uint64(len(rs.rdb_arr))
	fmt.Fprintf(os.Stderr, "get snapshot key: %s at redis[%d]\n", key, idx)
	return rs.rdb_arr[idx].Get(ctx, key).Bytes()
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
	key := fmt.Sprintf("%s_%#x", changelogTpName, logOff)
	idx := hashfuncs.NameHash(key) % uint64(len(rs.rdb_arr))
	fmt.Fprintf(os.Stderr, "store snapshot key: %s at redis[%d]\n", key, idx)
	return rs.rdb_arr[idx].Set(ctx, key, snapshot, time.Duration(60)*time.Second).Err()
}

func (rs *RedisSnapshotStore) GetSnapshot(ctx context.Context, changelogTpName string, logOff uint64) ([]byte, error) {
	key := fmt.Sprintf("%s_%#x", changelogTpName, logOff)
	idx := hashfuncs.NameHash(key) % uint64(len(rs.rdb_arr))
	fmt.Fprintf(os.Stderr, "get snapshot key: %s at redis[%d]\n", key, idx)
	return rs.rdb_arr[idx].Get(ctx, key).Bytes()
}
