package transaction

import (
	"context"
	"fmt"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/errors"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/stream/processor/store"
)

type KVStoreChangelog struct {
	KVStore     store.KeyValueStore
	Changelog   store.Stream
	keySerde    commtypes.Serde
	valSerde    commtypes.Serde
	InputStream store.Stream
	RestoreFunc func(ctx context.Context, args interface{}) error
	RestoreArg  interface{}
	// when used by changelog backed kv store, parnum is the parnum of changelog
	// when used by mongodb, parnum is the parnum of the input streams
	ParNum uint8
}

func NewKVStoreChangelog(
	kvStore store.KeyValueStore,
	changelog store.Stream,
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
	kvStore store.KeyValueStore,
	inputStream store.Stream,
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
		if kvstorelog.KVStore.TableType() == store.MONGODB {
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
		if kvstorelog.KVStore.TableType() == store.MONGODB {
			if err := kvstorelog.KVStore.CommitTransaction(ctx, taskRepr, transactionID); err != nil {
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
				return err
			}
		}
	}
	return nil
}

func RestoreChangelogKVStateStore(
	ctx context.Context,
	kvchangelog *KVStoreChangelog,
	msgSerde commtypes.MsgSerde,
	offset uint64,
) error {
	currentOffset := uint64(0)
	debug.Assert(kvchangelog.valSerde != nil, "val serde should not be nil")
	debug.Assert(kvchangelog.keySerde != nil, "key serde should not be nil")
	for {
		_, msgs, err := kvchangelog.Changelog.ReadNext(ctx, kvchangelog.ParNum)
		// nothing to restore
		if errors.IsStreamEmptyError(err) {
			return nil
		} else if err != nil {
			return fmt.Errorf("ReadNext failed: %v", err)
		}
		for _, msg := range msgs {
			currentOffset = msg.LogSeqNum
			if msg.Payload == nil {
				continue
			}
			keyBytes, valBytes, err := msgSerde.Decode(msg.Payload)
			if err != nil {
				// fmt.Fprintf(os.Stderr, "msg payload is %v", string(msg.Payload))
				return fmt.Errorf("MsgSerde decode failed: %v", err)
			}
			key, err := kvchangelog.keySerde.Decode(keyBytes)
			if err != nil {
				return fmt.Errorf("key serde3 failed: %v", err)
			}
			valTmp, err := kvchangelog.valSerde.Decode(valBytes)
			if err != nil {
				return fmt.Errorf("val serde decode failed: %v", err)
			}
			val := valTmp.(commtypes.StreamTimeExtractor)
			ts, err := val.ExtractStreamTime()
			if err != nil {
				return fmt.Errorf("extract stream time failed: %v", err)
			}
			err = kvchangelog.KVStore.Put(ctx, key, commtypes.ValueTimestamp{Value: val, Timestamp: ts})
			if err != nil {
				return fmt.Errorf("kvstore put failed: %v", err)
			}
		}
		if currentOffset == offset {
			return nil
		}
	}
}
