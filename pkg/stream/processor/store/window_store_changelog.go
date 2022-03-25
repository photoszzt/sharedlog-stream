package store

import (
	"context"
	"sharedlog-stream/pkg/stream/processor/commtypes"
)

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

func BeginWindowStoreTransaction(ctx context.Context, winstores []*WindowStoreChangelog) error {
	for _, winstorelog := range winstores {
		if winstorelog.windowStore.TableType() == MONGODB {
			if err := winstorelog.windowStore.StartTransaction(ctx); err != nil {
				return err
			}
		}
	}
	return nil
}

func CommitWindowStoreTransaction(ctx context.Context, winstores []*WindowStoreChangelog, taskRepr string, transactionID uint64) error {
	for _, winstorelog := range winstores {
		if winstorelog.windowStore.TableType() == MONGODB {
			if err := winstorelog.windowStore.CommitTransaction(ctx, taskRepr, transactionID); err != nil {
				return err
			}
		}
	}
	return nil
}

func AbortWindowStoreTransaction(ctx context.Context, winstores []*WindowStoreChangelog) error {
	for _, winstorelog := range winstores {
		if winstorelog.windowStore.TableType() == MONGODB {
			if err := winstorelog.windowStore.AbortTransaction(ctx); err != nil {
				return err
			}
		}
	}
	return nil
}
