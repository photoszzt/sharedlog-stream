package transaction

import (
	"context"
	"fmt"
	"os"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/errors"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/stream/processor/store"
)

type WindowStoreChangelog struct {
	WinStore         store.WindowStore
	Changelog        store.Stream
	keyWindowTsSerde commtypes.Serde
	keySerde         commtypes.Serde
	valSerde         commtypes.Serde
	InputStream      store.Stream
	RestoreFunc      func(ctx context.Context, args interface{}) error
	RestoreArg       interface{}
	ParNum           uint8
}

func NewWindowStoreChangelog(
	wStore store.WindowStore,
	changelog store.Stream,
	keyWindowTsSerde commtypes.Serde,
	keySerde commtypes.Serde,
	valSerde commtypes.Serde,
	parNum uint8,
) *WindowStoreChangelog {
	return &WindowStoreChangelog{
		WinStore:         wStore,
		Changelog:        changelog,
		keyWindowTsSerde: keyWindowTsSerde,
		keySerde:         keySerde,
		valSerde:         valSerde,
		ParNum:           parNum,
	}
}

func NewWindowStoreChangelogForExternalStore(
	wStore store.WindowStore,
	inputStream store.Stream,
	restoreFunc func(ctx context.Context, args interface{}) error,
	restoreArg interface{},
	parNum uint8,
) *WindowStoreChangelog {
	return &WindowStoreChangelog{
		WinStore:    wStore,
		InputStream: inputStream,
		RestoreFunc: restoreFunc,
		RestoreArg:  restoreArg,
		ParNum:      parNum,
	}
}

func BeginWindowStoreTransaction(ctx context.Context, winstores []*WindowStoreChangelog) error {
	for _, winstorelog := range winstores {
		if winstorelog.WinStore.TableType() == store.MONGODB {
			if err := winstorelog.WinStore.StartTransaction(ctx); err != nil {
				return err
			}
		}
	}
	return nil
}

func CommitWindowStoreTransaction(ctx context.Context, winstores []*WindowStoreChangelog, taskRepr string, transactionID uint64) error {
	for _, winstorelog := range winstores {
		if winstorelog.WinStore.TableType() == store.MONGODB {
			if err := winstorelog.WinStore.CommitTransaction(ctx, taskRepr, transactionID); err != nil {
				return err
			}
		}
	}
	return nil
}

func AbortWindowStoreTransaction(ctx context.Context, winstores []*WindowStoreChangelog) error {
	for _, winstorelog := range winstores {
		if winstorelog.WinStore.TableType() == store.MONGODB {
			if err := winstorelog.WinStore.AbortTransaction(ctx); err != nil {
				return err
			}
		}
	}
	return nil
}

func storeToWindowStore(ctx context.Context, keyBytes []byte, ts int64,
	valBytes []byte, keySerde commtypes.Serde,
	valSerde commtypes.Serde, winTab store.WindowStore,
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

func RestoreChangelogWindowStateStore(
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
				debug.Fprintf(os.Stderr, "offset: %x\n", currentOffset)
				debug.Fprint(os.Stderr, "RestoreWindowStateStore encoded\n")
				debug.PrintByteSlice(msg.Payload)
				debug.Fprint(os.Stderr, "RestoreWindowStateStore kts: \n")
				debug.PrintByteSlice(keyWinBytes)
				debug.Fprint(os.Stderr, "RestoreWindowStateStore val: \n")
				debug.PrintByteSlice(valBytes)
				keyWinTmp, err := wschangelog.keyWindowTsSerde.Decode(keyWinBytes)
				if err != nil {
					return fmt.Errorf("keyWindowTsSerde decode failed: %v", err)
				}
				keyWin := keyWinTmp.(commtypes.KeyAndWindowStartTs)
				err = storeToWindowStore(ctx, keyWin.Key, keyWin.WindowStartTs,
					valBytes, wschangelog.keySerde, wschangelog.valSerde, wschangelog.WinStore)
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
				err = wschangelog.WinStore.Put(ctx, key, val, ts)
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
