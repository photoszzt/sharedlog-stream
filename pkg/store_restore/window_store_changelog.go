package store_restore

import (
	"context"
	"fmt"
	"os"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/store"
	"sharedlog-stream/pkg/store_with_changelog"
	"sharedlog-stream/pkg/transaction/tran_interface"

	"go.mongodb.org/mongo-driver/x/mongo/driver/session"
	"golang.org/x/xerrors"
)

type WindowStoreChangelog struct {
	winStore         store.WindowStore
	changelogManager *store_with_changelog.ChangelogManager
	inputStream      sharedlog_stream.Stream
	restoreFunc      func(ctx context.Context, args interface{}) error
	restoreArg       interface{}
	// this is used to identify the db and collection to store the transaction id
	tabTranRepr string
	parNum      uint8
}

func NewWindowStoreChangelog(
	wStore store.WindowStore,
	changelogManager *store_with_changelog.ChangelogManager,
	parNum uint8,
) *WindowStoreChangelog {
	return &WindowStoreChangelog{
		winStore:         wStore,
		changelogManager: changelogManager,
		parNum:           parNum,
	}
}

func (wsc *WindowStoreChangelog) SetTrackParFunc(trackParFunc tran_interface.TrackProdSubStreamFunc) {
	wsc.winStore.SetTrackParFunc(trackParFunc)
}

func (wsc *WindowStoreChangelog) TableType() store.TABLE_TYPE {
	return wsc.winStore.TableType()
}

func (wsc *WindowStoreChangelog) ChangelogManager() *store_with_changelog.ChangelogManager {
	return wsc.changelogManager
}

func (wsc *WindowStoreChangelog) WinExternalStore() store.WindowStoreOpForExternalStore {
	return wsc.winStore
}

func (wsc *WindowStoreChangelog) ParNum() uint8 {
	return wsc.parNum
}

func (wsc *WindowStoreChangelog) TabTranRepr() string {
	return wsc.tabTranRepr
}

func (wsc *WindowStoreChangelog) InputStream() sharedlog_stream.Stream {
	return wsc.inputStream
}

func (wsc *WindowStoreChangelog) ExecuteRestoreFunc(ctx context.Context) error {
	return wsc.restoreFunc(ctx, wsc.restoreArg)
}

func NewWindowStoreChangelogForExternalStore(
	wStore store.WindowStore,
	inputStream sharedlog_stream.Stream,
	restoreFunc func(ctx context.Context, args interface{}) error,
	restoreArg interface{},
	tabTranRepr string,
	parNum uint8,
) *WindowStoreChangelog {
	return &WindowStoreChangelog{
		winStore:    wStore,
		inputStream: inputStream,
		restoreFunc: restoreFunc,
		restoreArg:  restoreArg,
		parNum:      parNum,
		tabTranRepr: tabTranRepr,
	}
}

func BeginWindowStoreTransaction(ctx context.Context, winstores []*WindowStoreChangelog) error {
	for _, winstorelog := range winstores {
		if winstorelog.winStore.TableType() == store.MONGODB {
			if err := winstorelog.winStore.StartTransaction(ctx); err != nil {
				return err
			}
		}
	}
	return nil
}

func CommitWindowStoreTransaction(ctx context.Context, winstores []*WindowStoreChangelog, transactionID uint64) error {
	for _, winstorelog := range winstores {
		if winstorelog.winStore.TableType() == store.MONGODB {
			if err := winstorelog.winStore.CommitTransaction(ctx, winstorelog.tabTranRepr, transactionID); err != nil {
				return err
			}
		}
	}
	return nil
}

func AbortWindowStoreTransaction(ctx context.Context, winstores []*WindowStoreChangelog) error {
	for _, winstorelog := range winstores {
		if winstorelog.winStore.TableType() == store.MONGODB {
			if err := winstorelog.winStore.AbortTransaction(ctx); err != nil {
				if err == session.ErrAbortTwice {
					fmt.Fprint(os.Stderr, "transaction already aborted\n")
					return nil
				} else {
					return err
				}
			}
		}
	}
	return nil
}

func RestoreChangelogWindowStateStore(
	ctx context.Context,
	wschangelog *WindowStoreChangelog,
) error {
	for {
		gotMsgs, err := wschangelog.changelogManager.Consume(ctx, wschangelog.parNum)
		// nothing to restore
		if common_errors.IsStreamEmptyError(err) {
			return nil
		} else if xerrors.Is(err, common_errors.ErrStreamSourceTimeout) {
			return nil
		} else if err != nil {
			return fmt.Errorf("ReadNext failed: %v", err)
		}

		for _, msg := range gotMsgs.Msgs {
			if msg.MsgArr != nil {
				for _, msg := range msg.MsgArr {
					if msg.Key == nil && msg.Value == nil {
						continue
					}
					err = wschangelog.winStore.PutWithoutPushToChangelog(ctx, msg.Key, msg.Value, msg.Timestamp)
					if err != nil {
						return fmt.Errorf("window store put failed: %v", err)
					}
				}
			} else {
				msg := msg.Msg
				if msg.Key == nil && msg.Value == nil {
					continue
				}
				err = wschangelog.winStore.PutWithoutPushToChangelog(ctx, msg.Key, msg.Value, msg.Timestamp)
				if err != nil {
					return fmt.Errorf("window store put failed: %v", err)
				}
			}
		}
	}
}
