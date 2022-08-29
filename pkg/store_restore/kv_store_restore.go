package store_restore

import (
	"context"
	"fmt"
	"os"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/exactly_once_intr"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/store"
	"sharedlog-stream/pkg/store_with_changelog"
	"time"

	"golang.org/x/xerrors"
)

type KVStoreChangelog[K, V any] struct {
	kvStore          store.CoreKeyValueStore
	changelogManager *store_with_changelog.ChangelogManager[K, V]
}

var _ = store.KeyValueStoreOpWithChangelog(&KVStoreChangelog[int, string]{})

func NewKVStoreChangelog[K, V any](
	kvStore store.CoreKeyValueStore,
	changelogManager *store_with_changelog.ChangelogManager[K, V],
) *KVStoreChangelog[K, V] {
	return &KVStoreChangelog[K, V]{
		kvStore:          kvStore,
		changelogManager: changelogManager,
	}
}

func (kvc *KVStoreChangelog[K, V]) SetTrackParFunc(trackParFunc exactly_once_intr.TrackProdSubStreamFunc) {
	kvc.kvStore.SetTrackParFunc(trackParFunc)
}

func (kvc *KVStoreChangelog[K, V]) ChangelogIsSrc() bool {
	return kvc.changelogManager.ChangelogIsSrc()
}

func (kvc *KVStoreChangelog[K, V]) ChangelogTopicName() string {
	return kvc.changelogManager.TopicName()
}

func (kvc *KVStoreChangelog[K, V]) ConsumeOneLogEntry(ctx context.Context, parNum uint8, cutoff uint64) (int, error) {
	msgSeq, err := kvc.changelogManager.Consume(ctx, parNum)
	if err != nil {
		return 0, err
	}
	count := 0
	seqNum := msgSeq.LogSeqNum
	if seqNum >= cutoff && kvc.ChangelogIsSrc() {
		return 0, common_errors.ErrReachCutoffPos
	}
	if msgSeq.MsgArr != nil {
		for _, msg := range msgSeq.MsgArr {
			if msg.Key.IsNone() && msg.Value.IsNone() {
				continue
			}
			count += 1
			k := msg.Key.Unwrap()
			v := msg.Value.Unwrap()
			err = kvc.PutWithoutPushToChangelog(ctx, k, v)
			if err != nil {
				return 0, err
			}
		}
	} else {
		msg := msgSeq.Msg
		if msg.Key.IsNone() && msg.Value.IsNone() {
			return 0, nil
		}
		count += 1
		k := msg.Key.Unwrap()
		v := msg.Value.Unwrap()
		err = kvc.PutWithoutPushToChangelog(ctx, k, v)
		if err != nil {
			return 0, err
		}
	}
	return count, nil
}

func (kvc *KVStoreChangelog[K, V]) Flush(ctx context.Context) error {
	if !kvc.changelogManager.ChangelogIsSrc() {
		return kvc.changelogManager.Flush(ctx)
	}
	return nil
}

func (kvc *KVStoreChangelog[K, V]) PutWithoutPushToChangelog(ctx context.Context, key commtypes.KeyT, value commtypes.ValueT) error {
	return kvc.kvStore.PutWithoutPushToChangelog(ctx, key, value)
}

func (kvc *KVStoreChangelog[K, V]) ConfigureExactlyOnce(rem exactly_once_intr.ReadOnlyExactlyOnceManager, guarantee exactly_once_intr.GuaranteeMth) error {
	return kvc.changelogManager.ConfigExactlyOnce(rem, guarantee)
}

func (kvc *KVStoreChangelog[K, V]) Stream() sharedlog_stream.Stream {
	return kvc.changelogManager.Stream()
}
func (kvc *KVStoreChangelog[K, V]) GetInitialProdSeqNum() uint64 {
	panic("not supported")
}
func (kvc *KVStoreChangelog[K, V]) GetCurrentProdSeqNum() uint64 {
	panic("not supported")
}
func (kvc *KVStoreChangelog[K, V]) ResetInitialProd() {
	panic("not supported")
}
func (kvc *KVStoreChangelog[K, V]) SubstreamNum() uint8 {
	panic("not supported")
}

func RestoreChangelogKVStateStore(
	ctx context.Context,
	kvchangelog store.KeyValueStoreOpWithChangelog,
	consumedOffset uint64,
	parNum uint8,
) error {
	count := 0
	restoreKVStart := time.Now()
	for {
		subC, err := kvchangelog.ConsumeOneLogEntry(ctx, parNum, consumedOffset)
		// nothing to restore
		if common_errors.IsStreamEmptyError(err) {
			elapsed := time.Since(restoreKVStart)
			fmt.Fprintf(os.Stderr, "%s(%d) restore, count: %d, elapsed: %v\n",
				kvchangelog.ChangelogTopicName(), parNum, count, elapsed)
			return nil
		} else if xerrors.Is(err, common_errors.ErrStreamSourceTimeout) {
			elapsed := time.Since(restoreKVStart)
			fmt.Fprintf(os.Stderr, "%s(%d) restore, count: %d, elapsed: %v\n",
				kvchangelog.ChangelogTopicName(), parNum, count, elapsed)
			return nil
		} else if xerrors.Is(err, common_errors.ErrReachCutoffPos) {
			elapsed := time.Since(restoreKVStart)
			fmt.Fprintf(os.Stderr, "%s(%d) restore, count: %d, elapsed: %v\n",
				kvchangelog.ChangelogTopicName(), parNum, count, elapsed)
			return nil
		} else if err != nil {
			return fmt.Errorf("ReadNext failed: %v", err)
		}
		count += subC
	}
}
