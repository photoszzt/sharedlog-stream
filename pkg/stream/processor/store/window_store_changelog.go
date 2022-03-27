package store

import (
	"context"
	"sharedlog-stream/pkg/stream/processor/commtypes"
)

type WindowStoreChangelog struct {
	WindowStore      WindowStore
	Changelog        Stream
	keyWindowTsSerde commtypes.Serde
	keySerde         commtypes.Serde
	valSerde         commtypes.Serde
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
		WindowStore:      wStore,
		Changelog:        changelog,
		keyWindowTsSerde: keyWindowTsSerde,
		keySerde:         keySerde,
		valSerde:         valSerde,
		ParNum:           parNum,
	}
}

func BeginWindowStoreTransaction(ctx context.Context, winstores []*WindowStoreChangelog) error {
	for _, winstorelog := range winstores {
		if winstorelog.WindowStore.TableType() == MONGODB {
			if err := winstorelog.WindowStore.StartTransaction(ctx); err != nil {
				return err
			}
		}
	}
	return nil
}

func CommitWindowStoreTransaction(ctx context.Context, winstores []*WindowStoreChangelog, taskRepr string, transactionID uint64) error {
	for _, winstorelog := range winstores {
		if winstorelog.WindowStore.TableType() == MONGODB {
			if err := winstorelog.WindowStore.CommitTransaction(ctx, taskRepr, transactionID); err != nil {
				return err
			}
		}
	}
	return nil
}

func AbortWindowStoreTransaction(ctx context.Context, winstores []*WindowStoreChangelog) error {
	for _, winstorelog := range winstores {
		if winstorelog.WindowStore.TableType() == MONGODB {
			if err := winstorelog.WindowStore.AbortTransaction(ctx); err != nil {
				return err
			}
		}
	}
	return nil
}
