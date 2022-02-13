package store

import (
	"context"
	"fmt"
	"sharedlog-stream/pkg/errors"
	"sharedlog-stream/pkg/stream/processor/commtypes"
)

func RestoreWindowStateStore(
	ctx context.Context,
	windowStore WindowStore,
	changeLog Stream,
	parNum uint8,
	msgSerde commtypes.MsgSerde,
	keyWindowTsSerde commtypes.Serde,
	keySerde commtypes.Serde,
	valSerde commtypes.Serde,
	offset uint64,
) error {
	currentOffset := uint64(0)
	for {
		_, msgs, err := changeLog.ReadNext(ctx, parNum)
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
			if keyWindowTsSerde != nil {
				keyWinTmp, err := keyWindowTsSerde.Decode(keyWinBytes)
				if err != nil {
					return fmt.Errorf("keyWindowTsSerde decode failed: %v", err)
				}
				keyWin := keyWinTmp.(commtypes.KeyAndWindowStartTs)
				key, err := keySerde.Decode(keyWin.Key)
				if err != nil {
					return fmt.Errorf("keySerde decode failed: %v", err)
				}
				val, err := valSerde.Decode(valBytes)
				if err != nil {
					return fmt.Errorf("valSerde decode failed: %v", err)
				}
				err = windowStore.Put(ctx, key, val, keyWin.WindowStartTs)
				if err != nil {
					return fmt.Errorf("windowStore put failed: %v", err)
				}
			} else {
				key, err := keySerde.Decode(keyWinBytes)
				if err != nil {
					return fmt.Errorf("key serde2 failed: %v", err)
				}
				valTmp, err := valSerde.Decode(valBytes)
				if err != nil {
					return fmt.Errorf("val serde decode failed: %v", err)
				}
				val := valTmp.(commtypes.StreamTimeExtractor)
				ts, err := val.ExtractStreamTime()
				if err != nil {
					return fmt.Errorf("extract stream time failed: %v", err)
				}
				err = windowStore.Put(ctx, key, val, ts)
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
	kvstore KeyValueStore,
	changeLog Stream,
	parNum uint8,
	msgSerde commtypes.MsgSerde,
	offset uint64,
) error {
	currentOffset := uint64(0)
	for {
		_, msgs, err := changeLog.ReadNext(ctx, parNum)
		// nothing to restore
		if errors.IsStreamEmptyError(err) {
			return nil
		} else if err != nil {
			return err
		}
		for _, msg := range msgs {
			currentOffset = msg.LogSeqNum
			keyBytes, valBytes, err := msgSerde.Decode(msg.Payload)
			if err != nil {
				return err
			}
			err = kvstore.Put(ctx, keyBytes, valBytes)
			if err != nil {
				return err
			}
		}
		if currentOffset == offset {
			return nil
		}
	}
}
