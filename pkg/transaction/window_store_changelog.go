package transaction

import (
	"context"
	"fmt"
	"os"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/errors"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/stream/processor/store"
	"sharedlog-stream/pkg/stream/processor/store_with_changelog"

	"go.mongodb.org/mongo-driver/x/mongo/driver/session"
)

type WindowStoreChangelog struct {
	WinStore         store.WindowStore
	ChangelogManager *store_with_changelog.ChangelogManager
	keyWindowTsSerde commtypes.Serde
	kvmsgSerdes      commtypes.KVMsgSerdes
	InputStream      store.Stream
	RestoreFunc      func(ctx context.Context, args interface{}) error
	RestoreArg       interface{}
	// this is used to identify the db and collection to store the transaction id
	TabTranRepr string
	ParNum      uint8
}

func NewWindowStoreChangelog(
	wStore store.WindowStore,
	changelogManager *store_with_changelog.ChangelogManager,
	keyWindowTsSerde commtypes.Serde,
	kvmsgSerdes commtypes.KVMsgSerdes,
	parNum uint8,
) *WindowStoreChangelog {
	return &WindowStoreChangelog{
		WinStore:         wStore,
		ChangelogManager: changelogManager,
		keyWindowTsSerde: keyWindowTsSerde,
		kvmsgSerdes:      kvmsgSerdes,
		ParNum:           parNum,
	}
}

func NewWindowStoreChangelogForExternalStore(
	wStore store.WindowStore,
	inputStream store.Stream,
	restoreFunc func(ctx context.Context, args interface{}) error,
	restoreArg interface{},
	tabTranRepr string,
	parNum uint8,
) *WindowStoreChangelog {
	return &WindowStoreChangelog{
		WinStore:    wStore,
		InputStream: inputStream,
		RestoreFunc: restoreFunc,
		RestoreArg:  restoreArg,
		ParNum:      parNum,
		TabTranRepr: tabTranRepr,
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

func CommitWindowStoreTransaction(ctx context.Context, winstores []*WindowStoreChangelog, transactionID uint64) error {
	for _, winstorelog := range winstores {
		if winstorelog.WinStore.TableType() == store.MONGODB {
			if err := winstorelog.WinStore.CommitTransaction(ctx, winstorelog.TabTranRepr, transactionID); err != nil {
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

func decodeKV(keyBytes []byte, valBytes []byte, serdes winstoreSerdes) (commtypes.Message, error) {
	key, err := serdes.kvmsgSerdes.KeySerde.Decode(keyBytes)
	if err != nil {
		return commtypes.EmptyMessage, fmt.Errorf("keySerde decode failed: %v", err)
	}
	val, err := serdes.kvmsgSerdes.ValSerde.Decode(valBytes)
	if err != nil {
		return commtypes.EmptyMessage, fmt.Errorf("valSerde decode failed: %v", err)
	}
	return commtypes.Message{Key: key, Value: val}, nil
}

type winstoreSerdes struct {
	kvmsgSerdes      commtypes.KVMsgSerdes
	keyWindowTsSerde commtypes.Serde
}

func RestoreChangelogWindowStateStore(
	ctx context.Context,
	wschangelog *WindowStoreChangelog,
	offset uint64,
) error {
	debug.Assert(wschangelog.kvmsgSerdes.ValSerde != nil, "val serde should not be nil")
	debug.Assert(wschangelog.kvmsgSerdes.KeySerde != nil, "key serde should not be nil")
	currentOffset := uint64(0)
	serdes := winstoreSerdes{
		kvmsgSerdes:      wschangelog.kvmsgSerdes,
		keyWindowTsSerde: wschangelog.keyWindowTsSerde,
	}
	for {
		msg, err := wschangelog.ChangelogManager.ReadNext(ctx, wschangelog.ParNum)
		// nothing to restore
		if errors.IsStreamEmptyError(err) {
			return nil
		} else if err != nil {
			return fmt.Errorf("ReadNext failed: %v", err)
		}

		currentOffset = msg.LogSeqNum
		if len(msg.Payload) == 0 {
			continue
		}
		msgAndSeqs, err := commtypes.DecodeRawMsg(msg, serdes, sharedlog_stream.DEFAULT_PAYLOAD_ARR_SERDE, decodeMsgWinstore)
		if err != nil {
			return err
		}
		if msgAndSeqs.MsgArr != nil {
			for _, msg := range msgAndSeqs.MsgArr {
				if msg.Key == nil && msg.Value == nil {
					continue
				}
				err = wschangelog.WinStore.Put(ctx, msg.Key, msg.Value, msg.Timestamp)
				if err != nil {
					return fmt.Errorf("window store put failed: %v", err)
				}
			}
		} else {
			msg := msgAndSeqs.Msg
			if msg.Key == nil && msg.Value == nil {
				continue
			}
			err = wschangelog.WinStore.Put(ctx, msg.Key, msg.Value, msg.Timestamp)
			if err != nil {
				return fmt.Errorf("window store put failed: %v", err)
			}
		}
		if currentOffset == offset {
			return nil
		}
	}
}

func decodeMsgWinstore(payload []byte, serdesTmp interface{}) (commtypes.Message, error) {
	serdes := serdesTmp.(winstoreSerdes)
	keyWinBytes, valBytes, err := serdes.kvmsgSerdes.MsgSerde.Decode(payload)
	if err != nil {
		return commtypes.EmptyMessage, fmt.Errorf("msg serde decode failed: %v", err)
	}
	if serdes.keyWindowTsSerde != nil {
		debug.Fprint(os.Stderr, "RestoreWindowStateStore encoded\n")
		debug.PrintByteSlice(payload)
		debug.Fprint(os.Stderr, "RestoreWindowStateStore kts: \n")
		debug.PrintByteSlice(keyWinBytes)
		debug.Fprint(os.Stderr, "RestoreWindowStateStore val: \n")
		debug.PrintByteSlice(valBytes)
		keyWinTmp, err := serdes.keyWindowTsSerde.Decode(keyWinBytes)
		if err != nil {
			return commtypes.EmptyMessage, fmt.Errorf("keyWindowTsSerde decode failed: %v", err)
		}
		keyWin := keyWinTmp.(commtypes.KeyAndWindowStartTs)
		msg, err := decodeKV(keyWin.Key, valBytes, serdes)
		if err != nil {
			return commtypes.EmptyMessage, err
		}
		msg.Timestamp = keyWin.WindowStartTs
		return msg, nil
	} else {
		msg, err := decodeKV(keyWinBytes, valBytes, serdes)
		if err != nil {
			return commtypes.EmptyMessage, err
		}
		val := msg.Value.(commtypes.StreamTimeExtractor)
		ts, err := val.ExtractStreamTime()
		if err != nil {
			return commtypes.EmptyMessage, fmt.Errorf("extract stream time failed: %v", err)
		}
		msg.Timestamp = ts
		return msg, nil
	}
}
