package snapshot_store

import (
	"context"
	"fmt"
	"os"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/hashfuncs"
	"sharedlog-stream/pkg/redis_client"
	"sharedlog-stream/pkg/stats"
	"strconv"
	"strings"
	"time"

	"cs.utexas.edu/zjia/faas/types"
	"github.com/go-redis/redis/v9"
	"golang.org/x/sync/errgroup"
)

type SnapshotStore interface {
	StoreSrcLogoff(ctx context.Context, srcLogOff []commtypes.TpLogOff) error
	StoreAlignChkpt(ctx context.Context, snapshot []byte,
		srcLogOff []commtypes.TpLogOff, storeName string,
	) error
	GetAlignChkpt(ctx context.Context, srcs []string, storeName string) ([]byte, error)
	StoreSnapshot(ctx context.Context,
		snapshot []byte, changelogTpName string, logOff uint64,
	) error
	GetSnapshot(ctx context.Context, changelogTpName string, logOff uint64) ([]byte, error)
}

var _ = SnapshotStore(&RedisSnapshotStore{})

type RedisSnapshotStore struct {
	rdb_arr  []*redis.Client
	snapSize stats.StatsCollector[int]
}

func NewRedisSnapshotStore(createSnapshot bool) RedisSnapshotStore {
	if createSnapshot {
		return RedisSnapshotStore{
			rdb_arr:  redis_client.GetRedisClients(),
			snapSize: stats.NewStatsCollector[int]("redisStore", stats.DEFAULT_COLLECT_DURATION),
		}
	} else {
		return RedisSnapshotStore{}
	}
}

func (rs *RedisSnapshotStore) PrintRemainingStats() {
	rs.snapSize.PrintRemainingStats()
}

func (rs *RedisSnapshotStore) GetRedisClients() []*redis.Client {
	debug.Assert(len(rs.rdb_arr) != 0, "rdb arr should not be empty")
	return rs.rdb_arr
}

func (rs *RedisSnapshotStore) StoreSrcLogoff(ctx context.Context,
	srcLogOff []commtypes.TpLogOff,
) error {
	bg, ctx := errgroup.WithContext(ctx)
	for _, tpLogOff := range srcLogOff {
		idx := hashfuncs.NameHash(tpLogOff.Tp) % uint64(len(rs.rdb_arr))
		bg.Go(func() error {
			return rs.rdb_arr[idx].RPush(ctx, tpLogOff.Tp, tpLogOff.LogOff).Err()
		})
		debug.Fprintf(os.Stderr, "store src tpoff %s:%#x at redis[%d]\n",
			tpLogOff.Tp, tpLogOff.LogOff, idx)
	}
	return bg.Wait()
}

func (rs *RedisSnapshotStore) StoreAlignChkpt(ctx context.Context, snapshot []byte,
	srcLogOff []commtypes.TpLogOff, storeName string,
) error {
	var keys []string
	bg, ctx := errgroup.WithContext(ctx)
	l := uint64(len(rs.rdb_arr))
	for _, tpLogOff := range srcLogOff {
		keys = append(keys, fmt.Sprintf("%s_%#x", tpLogOff.Tp, tpLogOff.LogOff))
	}
	keys = append(keys, storeName)
	key := strings.Join(keys, "-")
	idx := hashfuncs.NameHash(key) % uint64(len(rs.rdb_arr))
	debug.Fprintf(os.Stderr, "store snapshot key: %s at redis[%d]\n", key, idx)
	rs.snapSize.AddSample(len(snapshot))
	err := rs.rdb_arr[idx].Set(ctx, key, snapshot, time.Duration(5)*time.Second).Err()
	if err != nil {
		return err
	}
	for _, tpLogOff := range srcLogOff {
		idx := hashfuncs.NameHash(tpLogOff.Tp) % l
		tp := tpLogOff.Tp
		logOff := tpLogOff.LogOff
		bg.Go(func() error {
			return rs.rdb_arr[idx].RPush(ctx, tp, logOff).Err()
		})
	}
	return bg.Wait()
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
	debug.Fprintf(os.Stderr, "get snapshot key: %s at redis[%d]\n", key, idx)
	return rs.rdb_arr[idx].Get(ctx, key).Bytes()
}

func (rs *RedisSnapshotStore) StoreSnapshot(ctx context.Context,
	snapshot []byte, changelogTpName string, logOff uint64,
) error {
	var uint16Serde commtypes.Uint16Serde
	env := ctx.Value(commtypes.ENVID{}).(types.Environment)
	debug.Assert(env != nil, "env should be set")
	key := fmt.Sprintf("%s_%#x", changelogTpName, logOff)
	idx := hashfuncs.NameHash(key) % uint64(len(rs.rdb_arr))
	fmt.Fprintf(os.Stderr, "store snapshot key: %s at redis[%d]\n", key, idx)
	err := rs.rdb_arr[idx].Set(ctx, key, snapshot, time.Duration(60)*time.Second).Err()
	if err != nil {
		return err
	}
	rs.snapSize.AddSample(len(snapshot))
	hasData := uint16(1)
	enc, _, err := uint16Serde.Encode(hasData)
	if err != nil {
		return err
	}
	return env.SharedLogSetAuxData(ctx, logOff, enc)
}

func (rs *RedisSnapshotStore) GetSnapshot(ctx context.Context, changelogTpName string, logOff uint64) ([]byte, error) {
	key := fmt.Sprintf("%s_%#x", changelogTpName, logOff)
	idx := hashfuncs.NameHash(key) % uint64(len(rs.rdb_arr))
	fmt.Fprintf(os.Stderr, "get snapshot key: %s at redis[%d]\n", key, idx)
	return rs.rdb_arr[idx].Get(ctx, key).Bytes()
}
