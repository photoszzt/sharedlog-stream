package store_restore

import (
	"context"
	"fmt"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/exactly_once_intr"
	"sharedlog-stream/pkg/store"
	"sharedlog-stream/pkg/store_with_changelog"

	"golang.org/x/xerrors"
)

type KVStoreChangelog struct {
	kvStore          store.KeyValueStore
	changelogManager *store_with_changelog.ChangelogManager
}

func NewKVStoreChangelog(
	kvStore store.KeyValueStore,
	changelogManager *store_with_changelog.ChangelogManager,
) *KVStoreChangelog {
	return &KVStoreChangelog{
		kvStore:          kvStore,
		changelogManager: changelogManager,
	}
}

func (kvc *KVStoreChangelog) SetTrackParFunc(trackParFunc exactly_once_intr.TrackProdSubStreamFunc) {
	kvc.kvStore.SetTrackParFunc(trackParFunc)
}

func (kvc *KVStoreChangelog) TableType() store.TABLE_TYPE {
	return kvc.kvStore.TableType()
}

func (kvc *KVStoreChangelog) ChangelogManager() *store_with_changelog.ChangelogManager {
	return kvc.changelogManager
}

/*
func NewKVStoreChangelogForExternalStore(
	kvStore store.KeyValueStore,
	inputStream sharedlog_stream.Stream,
	restoreFunc func(ctx context.Context, args interface{}) error,
	restoreArg interface{},
	tabTranRepr string,
	parNum uint8,
) *KVStoreChangelog {
	return &KVStoreChangelog{
		kvStore:     kvStore,
		inputStream: inputStream,
		restoreFunc: restoreFunc,
		restoreArg:  restoreArg,
		parNum:      parNum,
		tabTranRepr: tabTranRepr,
	}
}

func BeginKVStoreTransaction(ctx context.Context, kvstores []*KVStoreChangelog) error {
	for _, kvstorelog := range kvstores {
		if kvstorelog.kvStore.TableType() == store.MONGODB {
			debug.Fprintf(os.Stderr, "id %s %d start mongo transaction for db %s\n",
				ctx.Value("id"), kvstorelog.parNum, kvstorelog.kvStore.Name())
			if err := kvstorelog.kvStore.StartTransaction(ctx); err != nil {
				return err
			}
		}
	}
	return nil
}

func CommitKVStoreTransaction(ctx context.Context, kvstores []*KVStoreChangelog, transactionID uint64) error {
	for _, kvstorelog := range kvstores {
		if kvstorelog.kvStore.TableType() == store.MONGODB {
			debug.Fprintf(os.Stderr, "id %s %d commit mongo transaction for db %s\n",
				ctx.Value("id"), kvstorelog.parNum, kvstorelog.kvStore.Name())
			if err := kvstorelog.kvStore.CommitTransaction(ctx, kvstorelog.tabTranRepr, transactionID); err != nil {
				return err
			}
		}
	}
	return nil
}

func AbortKVStoreTransaction(ctx context.Context, kvstores []*KVStoreChangelog) error {
	for _, kvstorelog := range kvstores {
		if kvstorelog.kvStore.TableType() == store.MONGODB {
			if err := kvstorelog.kvStore.AbortTransaction(ctx); err != nil {
				if err == session.ErrAbortTwice {
					fmt.Fprint(os.Stderr, "transaction already aborted\n")
					return nil
				} else {
					return err
				}
			}
		}
	}
	return nil
}
*/

func RestoreChangelogKVStateStore(
	ctx context.Context,
	kvchangelog *KVStoreChangelog,
	consumedOffset uint64,
	parNum uint8,
) error {
	kvStore := kvchangelog.kvStore
	changelogManager := kvchangelog.changelogManager
	for {
		gotMsgs, err := changelogManager.Consume(ctx, parNum)
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
			if seqNum >= consumedOffset && changelogManager.ChangelogIsSrc() {
				return nil
			}
			if msg.MsgArr != nil {
				for _, msg := range msg.MsgArr {
					if msg.Key == nil && msg.Value == nil {
						continue
					}
					val := msg.Value
					if changelogManager.ChangelogIsSrc() {
						val = commtypes.CreateValueTimestamp(msg.Value, msg.Timestamp)
					}
					err = kvStore.PutWithoutPushToChangelog(ctx, msg.Key, val)
					if err != nil {
						return err
					}
				}
			} else {
				if msg.Msg.Key == nil && msg.Msg.Value == nil {
					continue
				}
				val := msg.Msg.Value
				if changelogManager.ChangelogIsSrc() {
					val = commtypes.CreateValueTimestamp(msg.Msg.Value, msg.Msg.Timestamp)
				}
				err = kvStore.PutWithoutPushToChangelog(ctx, msg.Msg.Key, val)
				if err != nil {
					return err
				}
			}
		}
	}
}
