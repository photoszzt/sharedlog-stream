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

func (rs *RedisSnapshotStore) StoreAlignChkpt(ctx context.Context, snapshot []byte, srcLogOff []commtypes.TpLogOff) error {
	var keys []string
	for _, tpLogOff := range srcLogOff {
		idx := hashfuncs.NameHash(tpLogOff.Tp) % uint64(len(rs.rdb_arr))
		err := rs.rdb_arr[idx].Set(ctx, tpLogOff.Tp, tpLogOff.LogOff, 0).Err()
		if err != nil {
			return err
		}
		keys = append(keys, fmt.Sprintf("%s_%#x", tpLogOff.Tp, tpLogOff.LogOff))
	}
	key := strings.Join(keys, "-")
	idx := hashfuncs.NameHash(key) % uint64(len(rs.rdb_arr))
	fmt.Fprintf(os.Stderr, "store snapshot key: %s at redis[%d]\n", key, idx)
	return rs.rdb_arr[idx].Set(ctx, key, snapshot, 0).Err()
}

func (rs *RedisSnapshotStore) GetAlignChkpt(ctx context.Context, srcs []string) ([]byte, error) {
	var keys []string
	for _, tp := range srcs {
		idx := hashfuncs.NameHash(tp) % uint64(len(rs.rdb_arr))
		logOff, err := rs.rdb_arr[idx].Get(ctx, tp).Uint64()
		if err != nil {
			return nil, err
		}
		keys = append(keys, fmt.Sprintf("%s_%#x", tp, logOff))
	}
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
