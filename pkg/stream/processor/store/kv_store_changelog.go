package store

import (
	"context"
	"sharedlog-stream/pkg/stream/processor/commtypes"
)

type KVStoreChangelog struct {
	kvStore   KeyValueStore
	Changelog Stream
	keySerde  commtypes.Serde
	valSerde  commtypes.Serde
	ParNum    uint8
}

func NewKVStoreChangelog(
	kvStore KeyValueStore,
	changelog Stream,
	keySerde commtypes.Serde,
	valSerde commtypes.Serde,
	parNum uint8,
) *KVStoreChangelog {
	return &KVStoreChangelog{
		kvStore:   kvStore,
		Changelog: changelog,
		keySerde:  keySerde,
		valSerde:  valSerde,
		ParNum:    parNum,
	}
}

func BeginKVStoreTransaction(ctx context.Context, kvstores []*KVStoreChangelog) error {
	for _, kvstorelog := range kvstores {
		if kvstorelog.kvStore.TableType() == MONGODB {
			if err := kvstorelog.kvStore.StartTransaction(ctx); err != nil {
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
		if kvstorelog.kvStore.TableType() == MONGODB {
			if err := kvstorelog.kvStore.CommitTransaction(ctx, taskRepr, transactionID); err != nil {
				return err
			}
		}
	}
	return nil
}

func AbortKVStoreTransaction(ctx context.Context, kvstores []*KVStoreChangelog) error {
	for _, kvstorelog := range kvstores {
		if kvstorelog.kvStore.TableType() == MONGODB {
			if err := kvstorelog.kvStore.AbortTransaction(ctx); err != nil {
				return err
			}
		}
	}
	return nil
}
