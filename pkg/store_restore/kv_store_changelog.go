package store_restore

import (
	"context"
	"fmt"
	"os"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/store"
	"sharedlog-stream/pkg/store_with_changelog"
	"sharedlog-stream/pkg/exactly_once_intr"

	"go.mongodb.org/mongo-driver/x/mongo/driver/session"
	"golang.org/x/xerrors"
)

type KVStoreChangelog struct {
	kvStore          store.KeyValueStore
	changelogManager *store_with_changelog.ChangelogManager
	inputStream      sharedlog_stream.Stream
	restoreFunc      func(ctx context.Context, args interface{}) error
	restoreArg       interface{}
	// this is used to identify the db and collection to store the transaction id
	tabTranRepr string
	parNum      uint8
}

func NewKVStoreChangelog(
	kvStore store.KeyValueStore,
	changelogManager *store_with_changelog.ChangelogManager,
	parNum uint8,
) *KVStoreChangelog {
	return &KVStoreChangelog{
		kvStore:          kvStore,
		changelogManager: changelogManager,
		parNum:           parNum,
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

func (kvc *KVStoreChangelog) KVExternalStore() store.KeyValueStoreOpForExternalStore {
	return kvc.kvStore
}

func (kvc *KVStoreChangelog) ParNum() uint8 {
	return kvc.parNum
}

func (kvc *KVStoreChangelog) TabTranRepr() string {
	return kvc.tabTranRepr
}

func (kvc *KVStoreChangelog) InputStream() sharedlog_stream.Stream {
	return kvc.inputStream
}

func (kvc *KVStoreChangelog) ExecuteRestoreFunc(ctx context.Context) error {
	return kvc.restoreFunc(ctx, kvc.restoreArg)
}

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

func RestoreChangelogKVStateStore(
	ctx context.Context,
	kvchangelog *KVStoreChangelog,
	consumedOffset uint64,
) error {
	kvStore := kvchangelog.kvStore
	for {
		gotMsgs, err := kvchangelog.changelogManager.Consume(ctx, kvchangelog.parNum)
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
			if seqNum >= consumedOffset && kvchangelog.changelogManager.ChangelogIsSrc() {
				return nil
			}
			if msg.MsgArr != nil {
				for _, msg := range msg.MsgArr {
					if msg.Key == nil && msg.Value == nil {
						continue
					}
					err = kvStore.PutWithoutPushToChangelog(ctx, msg.Key, msg.Value)
					if err != nil {
						return err
					}
				}
			} else {
				if msg.Msg.Key == nil && msg.Msg.Value == nil {
					continue
				}
				err = kvStore.PutWithoutPushToChangelog(ctx, msg.Msg.Key, msg.Msg.Value)
				if err != nil {
					return err
				}
			}
		}
	}
}
