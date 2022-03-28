package store

import (
	"context"
	"sharedlog-stream/pkg/stream/processor/commtypes"
)

type KVStoreChangelog struct {
	KVStore     KeyValueStore
	Changelog   Stream
	keySerde    commtypes.Serde
	valSerde    commtypes.Serde
	InputStream Stream
	RestoreFunc func(ctx context.Context, args interface{}) error
	RestoreArg  interface{}
	// when used by changelog backed kv store, parnum is the parnum of changelog
	// when used by mongodb, parnum is the parnum of the input streams
	ParNum uint8
}

func NewKVStoreChangelog(
	kvStore KeyValueStore,
	changelog Stream,
	keySerde commtypes.Serde,
	valSerde commtypes.Serde,
	parNum uint8,
) *KVStoreChangelog {
	return &KVStoreChangelog{
		KVStore:   kvStore,
		Changelog: changelog,
		keySerde:  keySerde,
		valSerde:  valSerde,
		ParNum:    parNum,
	}
}

func NewKVStoreChangelogForExternalStore(
	kvStore KeyValueStore,
	inputStream Stream,
	restoreFunc func(ctx context.Context, args interface{}) error,
	restoreArg interface{},
	parNum uint8,
) *KVStoreChangelog {
	return &KVStoreChangelog{
		KVStore:     kvStore,
		InputStream: inputStream,
		RestoreFunc: restoreFunc,
		RestoreArg:  restoreArg,
		ParNum:      parNum,
	}
}

func BeginKVStoreTransaction(ctx context.Context, kvstores []*KVStoreChangelog) error {
	for _, kvstorelog := range kvstores {
		if kvstorelog.KVStore.TableType() == MONGODB {
			if err := kvstorelog.KVStore.StartTransaction(ctx); err != nil {
				return err
			}
		}
	}
	return nil
}

func CommitKVStoreTransaction(ctx context.Context, kvstores []*KVStoreChangelog,
	taskRepr string, transactionID uint64,
) error {
	for _, kvstorelog := range kvstores {
		if kvstorelog.KVStore.TableType() == MONGODB {
			if err := kvstorelog.KVStore.CommitTransaction(ctx, taskRepr, transactionID); err != nil {
				return err
			}
		}
	}
	return nil
}

func AbortKVStoreTransaction(ctx context.Context, kvstores []*KVStoreChangelog) error {
	for _, kvstorelog := range kvstores {
		if kvstorelog.KVStore.TableType() == MONGODB {
			if err := kvstorelog.KVStore.AbortTransaction(ctx); err != nil {
				return err
			}
		}
	}
	return nil
}
