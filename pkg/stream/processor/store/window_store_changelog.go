package store

import (
	"context"
	"sharedlog-stream/pkg/stream/processor/commtypes"
)

type WindowStoreChangelog struct {
	WinStore         WindowStore
	Changelog        Stream
	keyWindowTsSerde commtypes.Serde
	keySerde         commtypes.Serde
	valSerde         commtypes.Serde
	InputStream      Stream
	RestoreFunc      func(ctx context.Context, args interface{}) error
	RestoreArg       interface{}
	ParNum           uint8
}

func NewWindowStoreChangelog(
	wStore WindowStore,
	changelog Stream,
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
	wStore WindowStore,
	inputStream Stream,
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
		if winstorelog.WinStore.TableType() == MONGODB {
			if err := winstorelog.WinStore.StartTransaction(ctx); err != nil {
				return err
			}
		}
	}
	return nil
}

func CommitWindowStoreTransaction(ctx context.Context, winstores []*WindowStoreChangelog, taskRepr string, transactionID uint64) error {
	for _, winstorelog := range winstores {
		if winstorelog.WinStore.TableType() == MONGODB {
			if err := winstorelog.WinStore.CommitTransaction(ctx, taskRepr, transactionID); err != nil {
				return err
			}
		}
	}
	return nil
}

func AbortWindowStoreTransaction(ctx context.Context, winstores []*WindowStoreChangelog) error {
	for _, winstorelog := range winstores {
		if winstorelog.WinStore.TableType() == MONGODB {
			if err := winstorelog.WinStore.AbortTransaction(ctx); err != nil {
				return err
			}
		}
	}
	return nil
}
