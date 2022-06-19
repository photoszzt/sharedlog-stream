package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sharedlog-stream/benchmark/tests/pkg/tests/test_types"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/concurrent_skiplist"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/store"
	"sharedlog-stream/pkg/store_restore"
	"sharedlog-stream/pkg/store_with_changelog"
	"sharedlog-stream/pkg/treemap"
	"time"

	"cs.utexas.edu/zjia/faas/types"
)

type tableRestoreHandler struct {
	env              types.Environment
	kSerde           commtypes.Serde
	vSerde           commtypes.Serde
	keyWindowTsSerde commtypes.Serde
	msgSerde         commtypes.MsgSerde
}

func NewTableRestoreHandler(env types.Environment) types.FuncHandler {
	return &tableRestoreHandler{
		env:              env,
		kSerde:           commtypes.IntSerde{},
		vSerde:           strTsJSONSerde{},
		msgSerde:         commtypes.MessageSerializedJSONSerde{},
		keyWindowTsSerde: commtypes.KeyAndWindowStartTsJSONSerde{},
	}
}

func (h *tableRestoreHandler) Call(ctx context.Context, input []byte) ([]byte, error) {
	parsedInput := &test_types.TestInput{}
	err := json.Unmarshal(input, parsedInput)
	if err != nil {
		return nil, err
	}
	output := h.tests(ctx, parsedInput)
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		panic(err)
	}
	return utils.CompressData(encodedOutput), nil
}

func (h *tableRestoreHandler) tests(ctx context.Context, sp *test_types.TestInput) *common.FnOutput {
	switch sp.TestName {
	case "restoreKV":
		h.testRestoreKVTable(ctx)
	case "restoreWin":
		h.testRestoreWindowTable(ctx)
	default:
		return &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("test not recognized: %s\n", sp.TestName),
		}
	}
	return &common.FnOutput{
		Success: true,
		Message: fmt.Sprintf("%s done", sp.TestName),
	}
}

type strTs struct {
	Val string
	Ts  int64
	commtypes.BaseInjTime
}

var _ = commtypes.EventTimeExtractor(&strTs{})

func (s *strTs) ExtractEventTime() (int64, error) {
	return s.Ts, nil
}

type strTsJSONSerde struct{}

func (s strTsJSONSerde) Encode(value interface{}) ([]byte, error) {
	val, ok := value.(*strTs)
	if !ok {
		valTmp := value.(strTs)
		val = &valTmp
	}
	return json.Marshal(val)
}

func (s strTsJSONSerde) Decode(value []byte) (interface{}, error) {
	val := strTs{}
	if err := json.Unmarshal(value, &val); err != nil {
		return nil, err
	}
	return val, nil
}

func (h *tableRestoreHandler) pushToLog(ctx context.Context, key int, val string, ts int64, log *sharedlog_stream.ShardedSharedLogStream) (uint64, error) {
	keyBytes, err := h.kSerde.Encode(key)
	if err != nil {
		return 0, err
	}
	valBytes, err := h.vSerde.Encode(&strTs{Val: val, Ts: ts})
	if err != nil {
		return 0, err
	}
	encoded, err := h.msgSerde.Encode(keyBytes, valBytes)
	if err != nil {
		return 0, err
	}
	return log.Push(ctx, encoded, 0, sharedlog_stream.SingleDataRecordMeta, commtypes.EmptyProducerId)
}

func checkMapEqual(expected map[int]string, got map[int]string) {
	if len(expected) != len(got) {
		panic(fmt.Sprintf("expected and got have different length, expected. expected: %v, got: %v", expected, got))
	}
	for k, v := range expected {
		vgot := got[k]
		if vgot != v {
			panic(fmt.Sprintf("k: %d, expected: %s, got: %s", k, v, vgot))
		}
	}
}

func (h *tableRestoreHandler) testRestoreKVTable(ctx context.Context) {
	changelog, err := sharedlog_stream.NewShardedSharedLogStream(h.env, "c1", 1, commtypes.JSON)
	if err != nil {
		panic(err)
	}
	offset := uint64(0)
	if _, err = h.pushToLog(ctx, 0, "zero", 0, changelog); err != nil {
		panic(err)
	}
	if _, err = h.pushToLog(ctx, 1, "one", 0, changelog); err != nil {
		panic(err)
	}
	if _, err = h.pushToLog(ctx, 2, "two", 0, changelog); err != nil {
		panic(err)
	}
	if offset, err = h.pushToLog(ctx, 3, "three", 0, changelog); err != nil {
		panic(err)
	}
	kvstore := store.NewInMemoryKeyValueStore("test1", func(a, b treemap.Key) int {
		ka := a.(int)
		kb := b.(int)
		if ka < kb {
			return -1
		} else if ka == kb {
			return 0
		} else {
			return 1
		}
	})
	err = store_restore.RestoreChangelogKVStateStore(ctx,
		store_restore.NewKVStoreChangelog(kvstore,
			store_with_changelog.NewChangelogManagerForSrc(changelog, commtypes.KVMsgSerdes{
				KeySerde: commtypes.IntSerde{},
				ValSerde: strTsJSONSerde{},
				MsgSerde: h.msgSerde,
			}, common.SrcConsumeTimeout), 0), offset)
	if err != nil {
		panic(err)
	}
	ret := make(map[int]string)
	err = kvstore.Range(ctx, nil, nil, func(kt commtypes.KeyT, vt commtypes.ValueT) error {
		vts := vt.(commtypes.ValueTimestamp)
		ret[kt.(int)] = vts.Value.(strTs).Val
		return nil
	})
	if err != nil {
		panic(err)
	}
	expected := map[int]string{
		0: "zero", 1: "one", 2: "two", 3: "three",
	}
	checkMapEqual(expected, ret)
}

func (h *tableRestoreHandler) pushToWindowLog(ctx context.Context, key int, val string,
	vSerde commtypes.Serde,
	windowStartTimestamp int64, log *sharedlog_stream.ShardedSharedLogStream,
) (uint64, error) {
	keyBytes, err := h.kSerde.Encode(key)
	if err != nil {
		return 0, err
	}
	valBytes, err := vSerde.Encode(val)
	if err != nil {
		return 0, err
	}

	keyAndTs := &commtypes.KeyAndWindowStartTsSerialized{
		KeySerialized: keyBytes,
		WindowStartTs: windowStartTimestamp,
	}

	ktsBytes, err := h.keyWindowTsSerde.Encode(keyAndTs)
	if err != nil {
		return 0, err
	}
	encoded, err := h.msgSerde.Encode(ktsBytes, valBytes)
	if err != nil {
		return 0, err
	}
	debug.Fprintf(os.Stderr, "pushToWindowLog: keyAndTs %v\n", keyAndTs)
	debug.Fprint(os.Stderr, "pushToWindowLog: ktsBytes\n")
	debug.PrintByteSlice(ktsBytes)
	debug.Fprint(os.Stderr, "pushToWindowLog: valBytes\n")
	debug.PrintByteSlice(valBytes)
	debug.Fprint(os.Stderr, "pushToWindowLog: encoded\n")
	debug.PrintByteSlice(encoded)
	off, err := log.Push(ctx, encoded, 0, sharedlog_stream.SingleDataRecordMeta, commtypes.EmptyProducerId)
	debug.Fprintf(os.Stderr, "offset: %x\n", off)
	return off, err
}

func (h *tableRestoreHandler) testRestoreWindowTable(ctx context.Context) {
	changelog, err := sharedlog_stream.NewShardedSharedLogStream(h.env, "c2", 1, commtypes.JSON)
	if err != nil {
		panic(err)
	}
	wstore := store.NewInMemoryWindowStore("test1", store.TEST_RETENTION_PERIOD, store.TEST_WINDOW_SIZE, false,
		concurrent_skiplist.CompareFunc(store.CompareNoDup))
	if err != nil {
		panic(err)
	}
	ss := commtypes.StringSerde{}
	_, err = h.pushToWindowLog(ctx, 1, "one", ss, 0, changelog)
	if err != nil {
		panic(err)
	}
	_, err = h.pushToWindowLog(ctx, 2, "two", ss, store.TEST_WINDOW_SIZE, changelog)
	if err != nil {
		panic(err)
	}
	_, err = h.pushToWindowLog(ctx, 3, "three", ss, store.TEST_WINDOW_SIZE*2, changelog)
	if err != nil {
		panic(err)
	}
	err = store_restore.RestoreChangelogWindowStateStore(ctx,
		store_restore.NewWindowStoreChangelog(wstore,
			store_with_changelog.NewChangelogManager(changelog, commtypes.KVMsgSerdes{
				KeySerde: commtypes.IntSerde{},
				ValSerde: commtypes.StringSerde{},
				MsgSerde: h.msgSerde,
			}, common.SrcConsumeTimeout, time.Duration(5)*time.Millisecond), 0))
	if err != nil {
		panic(err)
	}
	ret := make(map[int]commtypes.ValueTimestamp)
	err = wstore.FetchAll(ctx, time.UnixMilli(0), time.UnixMilli(store.TEST_WINDOW_SIZE*2),
		func(i int64, kt commtypes.KeyT, vt commtypes.ValueT) error {
			ret[kt.(int)] = commtypes.ValueTimestamp{Value: vt, Timestamp: i}
			return nil
		})
	if err != nil {
		panic(err)
	}
	expected := map[int]commtypes.ValueTimestamp{
		1: {Value: "one", Timestamp: 0},
		2: {Value: "two", Timestamp: store.TEST_WINDOW_SIZE},
		3: {Value: "three", Timestamp: store.TEST_WINDOW_SIZE * 2},
	}
	if len(expected) != len(ret) {
		panic(fmt.Sprintf("expected and got have different length. expected: %v, got: %v", expected, ret))
	}
	for k, v := range expected {
		vgot := ret[k]
		if vgot != v {
			panic(fmt.Sprintf("k: %d, expected: %v, got: %v", k, v, vgot))
		}
	}
}
