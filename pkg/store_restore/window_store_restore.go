package store_restore

import (
	"context"
	"fmt"
	"os"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/store"
	"time"

	"golang.org/x/xerrors"
)

func RestoreChangelogWindowStateStore(
	ctx context.Context,
	wschangelog store.WindowStoreOpWithChangelog,
	parNum uint8,
) error {
	count := 0
	restoreStart := time.Now()
	for {
		gotMsgs, err := wschangelog.ConsumeChangelog(ctx, parNum)
		// nothing to restore
		if common_errors.IsStreamEmptyError(err) {
			elapsed := time.Since(restoreStart)
			fmt.Fprintf(os.Stderr, "%s(%d) restore, count: %d, elapsed: %v\n",
				wschangelog.ChangelogTopicName(), parNum, count, elapsed)
			return nil
		} else if xerrors.Is(err, common_errors.ErrStreamSourceTimeout) {
			elapsed := time.Since(restoreStart)
			fmt.Fprintf(os.Stderr, "%s(%d) restore, count: %d, elapsed: %v\n",
				wschangelog.ChangelogTopicName(), parNum, count, elapsed)
			return nil
		} else if err != nil {
			return fmt.Errorf("ReadNext failed: %v", err)
		}

		msgs := gotMsgs.Msgs
		if msgs.MsgArr != nil {
			// debug.Fprintf(os.Stderr, "Got msg with %d msgs\n", len(msgs.MsgArr))
			for _, msg := range msgs.MsgArr {
				if msg.Key == nil && msg.Value == nil {
					continue
				}
				count += 1
				err = wschangelog.PutWithoutPushToChangelog(ctx, msg.Key, msg.Value)
				if err != nil {
					return fmt.Errorf("window store put failed: %v", err)
				}
			}
		} else {
			msg := msgs.Msg
			// debug.Fprintf(os.Stderr, "Got 1 msg\n")
			if msg.Key == nil && msg.Value == nil {
				continue
			}
			count += 1
			valTs := msg.Value.(commtypes.ValueTimestamp)
			err = wschangelog.PutWithoutPushToChangelog(ctx, msg.Key, valTs.Value)
			if err != nil {
				return fmt.Errorf("window store put failed: %v", err)
			}
		}
	}
}
