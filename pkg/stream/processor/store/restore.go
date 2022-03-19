package store

import (
	"context"
	"fmt"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/errors"
	"sharedlog-stream/pkg/stream/processor/commtypes"
)

func storeToWindowStore(ctx context.Context, keyBytes []byte, ts int64,
	valBytes []byte, keySerde commtypes.Serde,
	valSerde commtypes.Serde, winTab WindowStore,
) error {
	key, err := keySerde.Decode(keyBytes)
	if err != nil {
		return fmt.Errorf("keySerde decode failed: %v", err)
	}
	val, err := valSerde.Decode(valBytes)
	if err != nil {
		return fmt.Errorf("valSerde decode failed: %v", err)
	}
	err = winTab.Put(ctx, key, val, ts)
	return err
}

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

type WindowStoreChangelog struct {
	windowStore      WindowStore
	Changelog        Stream
	keyWindowTsSerde commtypes.Serde
	keySerde         commtypes.Serde
	valSerde         commtypes.Serde
	ParNum           uint8
}

func NewWindowStoreChangelog(
	wsStore WindowStore,
	changelog Stream,
	keyWindowTsSerde commtypes.Serde,
	keySerde commtypes.Serde,
	valSerde commtypes.Serde,
	parNum uint8,
) *WindowStoreChangelog {
	return &WindowStoreChangelog{
		windowStore:      wsStore,
		Changelog:        changelog,
		keyWindowTsSerde: keyWindowTsSerde,
		keySerde:         keySerde,
		valSerde:         valSerde,
		ParNum:           parNum,
	}
}

func RestoreWindowStateStore(
	ctx context.Context,
	wschangelog *WindowStoreChangelog,
	msgSerde commtypes.MsgSerde,
	offset uint64,
) error {
	debug.Assert(wschangelog.valSerde != nil, "val serde should not be nil")
	debug.Assert(wschangelog.keySerde != nil, "key serde should not be nil")
	currentOffset := uint64(0)
	for {
		_, msgs, err := wschangelog.Changelog.ReadNext(ctx, wschangelog.ParNum)
		// nothing to restore
		if errors.IsStreamEmptyError(err) {
			return nil
		} else if err != nil {
			return fmt.Errorf("ReadNext failed: %v", err)
		}
		for _, msg := range msgs {
			currentOffset = msg.LogSeqNum
			if len(msg.Payload) == 0 {
				continue
			}
			keyWinBytes, valBytes, err := msgSerde.Decode(msg.Payload)
			if err != nil {
				return fmt.Errorf("msg serde decode failed: %v", err)
			}
			if wschangelog.keyWindowTsSerde != nil {
				keyWinTmp, err := wschangelog.keyWindowTsSerde.Decode(keyWinBytes)
				if err != nil {
					return fmt.Errorf("keyWindowTsSerde decode failed: %v", err)
				}
				keyWin := keyWinTmp.(commtypes.KeyAndWindowStartTs)
				err = storeToWindowStore(ctx, keyWin.Key, keyWin.WindowStartTs,
					valBytes, wschangelog.keySerde, wschangelog.valSerde, wschangelog.windowStore)
				if err != nil {
					return err
				}
			} else {
				key, err := wschangelog.keySerde.Decode(keyWinBytes)
				if err != nil {
					return fmt.Errorf("key serde2 failed: %v", err)
				}
				valTmp, err := wschangelog.valSerde.Decode(valBytes)
				if err != nil {
					return fmt.Errorf("val serde decode failed: %v", err)
				}
				val := valTmp.(commtypes.StreamTimeExtractor)
				ts, err := val.ExtractStreamTime()
				if err != nil {
					return fmt.Errorf("extract stream time failed: %v", err)
				}
				err = wschangelog.windowStore.Put(ctx, key, val, ts)
				if err != nil {
					return fmt.Errorf("window store put failed: %v", err)
				}
			}
		}
		if currentOffset == offset {
			return nil
		}
	}
}

func RestoreKVStateStore(
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
			err = kvchangelog.kvStore.Put(ctx, key, commtypes.ValueTimestamp{Value: val, Timestamp: ts})
			if err != nil {
				return fmt.Errorf("kvstore put failed: %v", err)
			}
		}
		if currentOffset == offset {
			return nil
		}
	}
}
