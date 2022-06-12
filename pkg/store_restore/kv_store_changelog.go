package store_restore

import (
	"context"
	"fmt"
	"os"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/store"
	"sharedlog-stream/pkg/store_with_changelog"
	"sharedlog-stream/pkg/transaction/tran_interface"

	"go.mongodb.org/mongo-driver/x/mongo/driver/session"
)

type KVStoreChangelog struct {
	kvStore          store.KeyValueStore
	changelogManager *store_with_changelog.ChangelogManager
	kvmsgSerdes      commtypes.KVMsgSerdes
	inputStream      store.Stream
	restoreFunc      func(ctx context.Context, args interface{}) error
	restoreArg       interface{}
	// this is used to identify the db and collection to store the transaction id
	tabTranRepr string
	// when used by changelog backed kv store, parnum is the parnum of changelog
	// when used by mongodb, parnum is the parnum of the input streams
	parNum uint8
}

func NewKVStoreChangelog(
	kvStore store.KeyValueStore,
	changelogManager *store_with_changelog.ChangelogManager,
	kvmsgSerdes commtypes.KVMsgSerdes,
	parNum uint8,
) *KVStoreChangelog {
	return &KVStoreChangelog{
		kvStore:          kvStore,
		changelogManager: changelogManager,
		kvmsgSerdes:      kvmsgSerdes,
		parNum:           parNum,
	}
}

func (kvc *KVStoreChangelog) SetTrackParFunc(trackParFunc tran_interface.TrackKeySubStreamFunc) {
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

func (kvc *KVStoreChangelog) InputStream() store.Stream {
	return kvc.inputStream
}

func (kvc *KVStoreChangelog) ExecuteRestoreFunc(ctx context.Context) error {
	return kvc.restoreFunc(ctx, kvc.restoreArg)
}

func NewKVStoreChangelogForExternalStore(
	kvStore store.KeyValueStore,
	inputStream store.Stream,
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
	offset uint64,
) error {
	currentOffset := uint64(0)
	debug.Assert(kvchangelog.kvmsgSerdes.ValSerde != nil, "val serde should not be nil")
	debug.Assert(kvchangelog.kvmsgSerdes.KeySerde != nil, "key serde should not be nil")
	debug.Assert(kvchangelog.kvmsgSerdes.MsgSerde != nil, "msg serde should not be nil")
	for {
		msg, err := kvchangelog.changelogManager.ReadNext(ctx, kvchangelog.parNum)
		// nothing to restore
		if common_errors.IsStreamEmptyError(err) {
			return nil
		} else if err != nil {
			return fmt.Errorf("ReadNext failed: %v", err)
		}
		currentOffset = msg.LogSeqNum
		if msg.Payload == nil {
			continue
		}
		msgAndSeqs, err := commtypes.DecodeRawMsg(msg,
			kvchangelog.kvmsgSerdes,
			sharedlog_stream.DEFAULT_PAYLOAD_ARR_SERDE,
			commtypes.DecodeMsg,
		)
		if err != nil {
			return err
		}
		if msgAndSeqs.MsgArr != nil {
			for _, msg := range msgAndSeqs.MsgArr {
				if msg.Key == nil && msg.Value == nil {
					continue
				}
				err = kvchangelog.kvStore.Put(ctx, msg.Key, msg.Value)
				if err != nil {
					return err
				}
			}
		} else {
			if msgAndSeqs.Msg.Key == nil && msgAndSeqs.Msg.Value == nil {
				continue
			}
			err = kvchangelog.kvStore.Put(ctx, msgAndSeqs.Msg.Key, msgAndSeqs.Msg.Value)
			if err != nil {
				return err
			}
		}

		if currentOffset == offset {
			return nil
		}
	}
}
