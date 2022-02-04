package store

import (
	"context"
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
			return err
		}
		for _, msg := range msgs {
			currentOffset = msg.LogSeqNum
			keyWinBytes, valBytes, err := msgSerde.Decode(msg.Payload)
			if err != nil {
				return err
			}
			if keyWindowTsSerde != nil {
				keyWinTmp, err := keyWindowTsSerde.Decode(keyWinBytes)
				if err != nil {
					return err
				}
				keyWin := keyWinTmp.(commtypes.KeyAndWindowStartTs)
				key, err := keySerde.Decode(keyWin.Key)
				if err != nil {
					return err
				}
				val, err := valSerde.Decode(valBytes)
				if err != nil {
					return err
				}
				err = windowStore.Put(ctx, key, val, keyWin.WindowStartTs)
				if err != nil {
					return err
				}
			} else {
				key, err := keySerde.Decode(keyWinBytes)
				if err != nil {
					return err
				}
				valTmp, err := valSerde.Decode(valBytes)
				if err != nil {
					return err
				}
				val := valTmp.(commtypes.StreamTimeExtractor)
				ts, err := val.ExtractStreamTime()
				if err != nil {
					return err
				}
				err = windowStore.Put(ctx, key, val, ts)
				if err != nil {
					return err
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
