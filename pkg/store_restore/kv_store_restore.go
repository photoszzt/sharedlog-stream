package store_restore

import (
	"context"
	"fmt"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/exactly_once_intr"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/store"
	"sharedlog-stream/pkg/store_with_changelog"

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

func (kvc *KVStoreChangelog[K, V]) ConsumeChangelog(ctx context.Context, parNum uint8) (*commtypes.MsgAndSeqs, error) {
	return kvc.changelogManager.Consume(ctx, parNum)
}

func (kvc *KVStoreChangelog[K, V]) FlushChangelog(ctx context.Context) error {
	if !kvc.changelogManager.ChangelogIsSrc() {
		return kvc.changelogManager.Flush(ctx)
	}
	return nil
}

func (kvc *KVStoreChangelog[K, V]) PutWithoutPushToChangelog(ctx context.Context, key commtypes.KeyT, value commtypes.ValueT) error {
	return kvc.kvStore.PutWithoutPushToChangelog(ctx, key, value)
}

func (kvc *KVStoreChangelog[K, V]) ConfigureExactlyOnce(rem exactly_once_intr.ReadOnlyExactlyOnceManager, guarantee exactly_once_intr.GuaranteeMth, serdeFormat commtypes.SerdeFormat) error {
	return kvc.changelogManager.ConfigExactlyOnce(rem, guarantee, serdeFormat)
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
	for {
		gotMsgs, err := kvchangelog.ConsumeChangelog(ctx, parNum)
		// nothing to restore
		if common_errors.IsStreamEmptyError(err) {
			return nil
		} else if xerrors.Is(err, common_errors.ErrStreamSourceTimeout) {
			return nil
		} else if err != nil {
			return fmt.Errorf("ReadNext failed: %v", err)
		}
		for _, msg := range gotMsgs.Msgs {
			seqNum := msg.LogSeqNum
			if seqNum >= consumedOffset && kvchangelog.ChangelogIsSrc() {
				return nil
			}
			if msg.MsgArr != nil {
				for _, msg := range msg.MsgArr {
					if msg.Key == nil && msg.Value == nil {
						continue
					}
					val := msg.Value
					if kvchangelog.ChangelogIsSrc() {
						val = commtypes.CreateValueTimestamp(msg.Value, msg.Timestamp)
					}
					err = kvchangelog.PutWithoutPushToChangelog(ctx, msg.Key, val)
					if err != nil {
						return err
					}
				}
			} else {
				if msg.Msg.Key == nil && msg.Msg.Value == nil {
					continue
				}
				val := msg.Msg.Value
				if kvchangelog.ChangelogIsSrc() {
					val = commtypes.CreateValueTimestamp(msg.Msg.Value, msg.Msg.Timestamp)
				}
				err = kvchangelog.PutWithoutPushToChangelog(ctx, msg.Msg.Key, val)
				if err != nil {
					return err
				}
			}
		}
	}
}
