package store_restore

import (
	"context"
	"fmt"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/store"

	"golang.org/x/xerrors"
)

/*
type WindowStoreChangelog[K, V any] struct {
	winStore         store.WindowStore
	changelogManager *store_with_changelog.ChangelogManager[commtypes.KeyAndWindowStartTsG[K], V]
}

func NewWindowStoreChangelog[K, V any](
	wStore store.WindowStore,
	changelogManager *store_with_changelog.ChangelogManager[commtypes.KeyAndWindowStartTsG[K], V],
) *WindowStoreChangelog[K, V] {
	return &WindowStoreChangelog[K, V]{
		winStore:         wStore,
		changelogManager: changelogManager,
	}
}

func (wsc *WindowStoreChangelog[K, V]) SetTrackParFunc(trackParFunc exactly_once_intr.TrackProdSubStreamFunc) {
	wsc.winStore.SetTrackParFunc(trackParFunc)
}

func (wsc *WindowStoreChangelog[K, V]) TableType() store.TABLE_TYPE {
	return wsc.winStore.TableType()
}

func (wsc *WindowStoreChangelog[K, V]) ChangelogManager() *store_with_changelog.ChangelogManager[commtypes.KeyAndWindowStartTsG[K], V] {
	return wsc.changelogManager
}
*/

func RestoreChangelogWindowStateStore(
	ctx context.Context,
	wschangelog store.WindowStoreOpWithChangelog,
	parNum uint8,
) error {
	for {
		gotMsgs, err := wschangelog.ConsumeChangelog(ctx, parNum)
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
					err = wschangelog.PutWithoutPushToChangelog(ctx, msg.Key, msg.Value, msg.Timestamp)
					if err != nil {
						return fmt.Errorf("window store put failed: %v", err)
					}
				}
			} else {
				msg := msg.Msg
				if msg.Key == nil && msg.Value == nil {
					continue
				}
				err = wschangelog.PutWithoutPushToChangelog(ctx, msg.Key, msg.Value, msg.Timestamp)
				if err != nil {
					return fmt.Errorf("window store put failed: %v", err)
				}
			}
		}
	}
}
