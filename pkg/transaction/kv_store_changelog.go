package transaction

import (
	"context"
	"fmt"
	"os"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/errors"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/stream/processor/store"
	"sharedlog-stream/pkg/stream/processor/store_with_changelog"

	"go.mongodb.org/mongo-driver/x/mongo/driver/session"
)

type KVStoreChangelog struct {
	KVStore          store.KeyValueStore
	ChangelogManager *store_with_changelog.ChangelogManager
	kvmsgSerdes      commtypes.KVMsgSerdes
	InputStream      store.Stream
	RestoreFunc      func(ctx context.Context, args interface{}) error
	RestoreArg       interface{}
	// this is used to identify the db and collection to store the transaction id
	TabTranRepr string
	// when used by changelog backed kv store, parnum is the parnum of changelog
	// when used by mongodb, parnum is the parnum of the input streams
	ParNum uint8
}

func NewKVStoreChangelog(
	kvStore store.KeyValueStore,
	changelogManager *store_with_changelog.ChangelogManager,
	kvmsgSerdes commtypes.KVMsgSerdes,
	parNum uint8,
) *KVStoreChangelog {
	return &KVStoreChangelog{
		KVStore:          kvStore,
		ChangelogManager: changelogManager,
		kvmsgSerdes:      kvmsgSerdes,
		ParNum:           parNum,
	}
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
		KVStore:     kvStore,
		InputStream: inputStream,
		RestoreFunc: restoreFunc,
		RestoreArg:  restoreArg,
		ParNum:      parNum,
		TabTranRepr: tabTranRepr,
	}
}

func BeginKVStoreTransaction(ctx context.Context, kvstores []*KVStoreChangelog) error {
	for _, kvstorelog := range kvstores {
		if kvstorelog.KVStore.TableType() == store.MONGODB {
			debug.Fprintf(os.Stderr, "id %s %d start mongo transaction for db %s\n",
				ctx.Value("id"), kvstorelog.ParNum, kvstorelog.KVStore.Name())
			if err := kvstorelog.KVStore.StartTransaction(ctx); err != nil {
				return err
			}
		}
	}
	return nil
}

func CommitKVStoreTransaction(ctx context.Context, kvstores []*KVStoreChangelog, transactionID uint64) error {
	for _, kvstorelog := range kvstores {
		if kvstorelog.KVStore.TableType() == store.MONGODB {
			debug.Fprintf(os.Stderr, "id %s %d commit mongo transaction for db %s\n",
				ctx.Value("id"), kvstorelog.ParNum, kvstorelog.KVStore.Name())
			if err := kvstorelog.KVStore.CommitTransaction(ctx, kvstorelog.TabTranRepr, transactionID); err != nil {
				return err
			}
		}
	}
	return nil
}

func AbortKVStoreTransaction(ctx context.Context, kvstores []*KVStoreChangelog) error {
	for _, kvstorelog := range kvstores {
		if kvstorelog.KVStore.TableType() == store.MONGODB {
			if err := kvstorelog.KVStore.AbortTransaction(ctx); err != nil {
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
		msg, err := kvchangelog.ChangelogManager.ReadNext(ctx, kvchangelog.ParNum)
		// nothing to restore
		if errors.IsStreamEmptyError(err) {
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
				err = kvchangelog.KVStore.Put(ctx, msg.Key, msg.Value)
				if err != nil {
					return err
				}
			}
		} else {
			if msgAndSeqs.Msg.Key == nil && msgAndSeqs.Msg.Value == nil {
				continue
			}
			err = kvchangelog.KVStore.Put(ctx, msgAndSeqs.Msg.Key, msgAndSeqs.Msg.Value)
			if err != nil {
				return err
			}
		}

		if currentOffset == offset {
			return nil
		}
	}
}
