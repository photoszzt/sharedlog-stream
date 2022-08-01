package store_restore

import (
	"context"
	"fmt"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/store"

	"golang.org/x/xerrors"
)

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

		msgs := gotMsgs.Msgs
		if msgs.MsgArr != nil {
			for _, msg := range msgs.MsgArr {
				if msg.Key == nil && msg.Value == nil {
					continue
				}
				err = wschangelog.PutWithoutPushToChangelog(ctx, msg.Key, msg.Value, msg.Timestamp)
				if err != nil {
					return fmt.Errorf("window store put failed: %v", err)
				}
			}
		} else {
			msg := msgs.Msg
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
