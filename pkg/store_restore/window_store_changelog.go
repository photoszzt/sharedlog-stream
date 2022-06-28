package store_restore

import (
	"context"
	"fmt"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/exactly_once_intr"
	"sharedlog-stream/pkg/store"
	"sharedlog-stream/pkg/store_with_changelog"

	"golang.org/x/xerrors"
)

type WindowStoreChangelog struct {
	winStore         store.WindowStore
	changelogManager *store_with_changelog.ChangelogManager
}

func NewWindowStoreChangelog(
	wStore store.WindowStore,
	changelogManager *store_with_changelog.ChangelogManager,
) *WindowStoreChangelog {
	return &WindowStoreChangelog{
		winStore:         wStore,
		changelogManager: changelogManager,
	}
}

func (wsc *WindowStoreChangelog) SetTrackParFunc(trackParFunc exactly_once_intr.TrackProdSubStreamFunc) {
	wsc.winStore.SetTrackParFunc(trackParFunc)
}

func (wsc *WindowStoreChangelog) TableType() store.TABLE_TYPE {
	return wsc.winStore.TableType()
}

func (wsc *WindowStoreChangelog) ChangelogManager() *store_with_changelog.ChangelogManager {
	return wsc.changelogManager
}

func RestoreChangelogWindowStateStore(
	ctx context.Context,
	wschangelog *WindowStoreChangelog,
	parNum uint8,
) error {
	for {
		gotMsgs, err := wschangelog.changelogManager.Consume(ctx, parNum)
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
