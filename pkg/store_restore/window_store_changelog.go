package store_restore

import (
	"context"
	"fmt"
	"os"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/store"
	"sharedlog-stream/pkg/store_with_changelog"
	"sharedlog-stream/pkg/transaction/tran_interface"

	"go.mongodb.org/mongo-driver/x/mongo/driver/session"
)

type WindowStoreChangelog struct {
	winStore         store.WindowStore
	changelogManager *store_with_changelog.ChangelogManager
	keyWindowTsSerde commtypes.Serde
	kvmsgSerdes      commtypes.KVMsgSerdes
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
	keyWindowTsSerde commtypes.Serde,
	kvmsgSerdes commtypes.KVMsgSerdes,
	parNum uint8,
) *WindowStoreChangelog {
	return &WindowStoreChangelog{
		winStore:         wStore,
		changelogManager: changelogManager,
		keyWindowTsSerde: keyWindowTsSerde,
		kvmsgSerdes:      kvmsgSerdes,
		parNum:           parNum,
	}
}

func (wsc *WindowStoreChangelog) SetTrackParFunc(trackParFunc tran_interface.TrackKeySubStreamFunc) {
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
	debug.Assert(wschangelog.kvmsgSerdes.MsgSerde != nil, "msg serde should not be nil")
	currentOffset := uint64(0)
	serdes := winstoreSerdes{
		kvmsgSerdes:      wschangelog.kvmsgSerdes,
		keyWindowTsSerde: wschangelog.keyWindowTsSerde,
	}
	for {
		msg, err := wschangelog.changelogManager.ReadNext(ctx, wschangelog.parNum)
		// nothing to restore
		if common_errors.IsStreamEmptyError(err) {
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
				err = wschangelog.winStore.Put(ctx, msg.Key, msg.Value, msg.Timestamp)
				if err != nil {
					return fmt.Errorf("window store put failed: %v", err)
				}
			}
		} else {
			msg := msgAndSeqs.Msg
			if msg.Key == nil && msg.Value == nil {
				continue
			}
			err = wschangelog.winStore.Put(ctx, msg.Key, msg.Value, msg.Timestamp)
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
		val := msg.Value.(commtypes.EventTimeExtractor)
		ts, err := val.ExtractEventTime()
		if err != nil {
			return commtypes.EmptyMessage, fmt.Errorf("extract stream time failed: %v", err)
		}
		msg.Timestamp = ts
		return msg, nil
	}
}
