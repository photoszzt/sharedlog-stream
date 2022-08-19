package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/tests/pkg/tests/test_types"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/data_structure/treemap"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/store"
	"sharedlog-stream/pkg/store_restore"
	"sharedlog-stream/pkg/store_with_changelog"
	"time"

	"cs.utexas.edu/zjia/faas/types"
)

type tableRestoreHandler struct {
	env types.Environment
}

func NewTableRestoreHandler(env types.Environment) types.FuncHandler {
	return &tableRestoreHandler{
		env: env,
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
	return common.CompressData(encodedOutput), nil
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
}

var _ = commtypes.EventTimeExtractor(&strTs{})

func (s *strTs) ExtractEventTime() (int64, error) {
	return s.Ts, nil
}

type strTsJSONSerde struct{}
type strTsJSONSerdeG struct{}

var _ = commtypes.Serde(strTsJSONSerde{})
var _ = commtypes.SerdeG[strTs](strTsJSONSerdeG{})

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

func (s strTsJSONSerdeG) Encode(value strTs) ([]byte, error) {
	return json.Marshal(&value)
}

func (s strTsJSONSerdeG) Decode(value []byte) (strTs, error) {
	val := strTs{}
	if err := json.Unmarshal(value, &val); err != nil {
		return strTs{}, err
	}
	return val, nil
}

func (h *tableRestoreHandler) pushToLog(ctx context.Context,
	key int, val string, ts int64,
	msgSerde commtypes.MessageSerde,
	log *sharedlog_stream.ShardedSharedLogStream,
) (uint64, error) {
	msg := commtypes.Message{
		Key: key,
		Value: &strTs{
			Val: val,
			Ts:  ts,
		},
		Timestamp: ts,
	}
	encoded, err := msgSerde.Encode(&msg)
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
	msgSerde := commtypes.MessageJSONSerde{
		KeySerde: commtypes.IntSerde{},
		ValSerde: commtypes.StringSerde{},
	}
	offset := uint64(0)
	if _, err = h.pushToLog(ctx, 0, "zero", 0, msgSerde, changelog); err != nil {
		panic(err)
	}
	if _, err = h.pushToLog(ctx, 1, "one", 0, msgSerde, changelog); err != nil {
		panic(err)
	}
	if _, err = h.pushToLog(ctx, 2, "two", 0, msgSerde, changelog); err != nil {
		panic(err)
	}
	if offset, err = h.pushToLog(ctx, 3, "three", 0, msgSerde, changelog); err != nil {
		panic(err)
	}
	kvstore := store.NewInMemoryKeyValueStore("test1", func(a, b treemap.Key) bool {
		ka := a.(int)
		kb := b.(int)
		return ka < kb
	})
	changelogSerde, err := commtypes.GetMsgSerdeG[int, strTs](commtypes.JSON, commtypes.IntSerdeG{}, strTsJSONSerdeG{})
	if err != nil {
		panic(err)
	}
	changelogManager, err := store_with_changelog.NewChangelogManagerForSrc(changelog,
		changelogSerde, common.SrcConsumeTimeout, commtypes.JSON, 0)
	if err != nil {
		panic(err)
	}
	err = store_restore.RestoreChangelogKVStateStore(ctx,
		store_restore.NewKVStoreChangelog(kvstore, changelogManager), offset, 0)
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
	msgSerde commtypes.MessageSerde,
	windowStartTimestamp int64, log *sharedlog_stream.ShardedSharedLogStream,
) (uint64, error) {
	keyAndTs := &commtypes.KeyAndWindowStartTs{
		Key:           key,
		WindowStartTs: windowStartTimestamp,
	}
	msg := commtypes.Message{
		Key:   keyAndTs,
		Value: val,
	}
	encoded, err := msgSerde.Encode(&msg)
	if err != nil {
		return 0, err
	}
	debug.Fprintf(os.Stderr, "pushToWindowLog: keyAndTs %v\n", keyAndTs)
	debug.Fprint(os.Stderr, "pushToWindowLog: encoded\n")
	debug.PrintByteSlice(encoded)
	off, err := log.Push(ctx, encoded, 0, sharedlog_stream.SingleDataRecordMeta, commtypes.EmptyProducerId)
	debug.Fprintf(os.Stderr, "offset: %x\n", off)
	return off, err
}

func (h *tableRestoreHandler) testRestoreWindowTable(ctx context.Context) {
	storeMsgSerde, err := commtypes.GetMsgSerdeG[int, string](commtypes.JSON, commtypes.IntSerdeG{}, commtypes.StringSerdeG{})
	if err != nil {
		panic(err)
	}
	mp, err := store_with_changelog.NewMaterializeParamBuilder[int, string]().
		MessageSerde(storeMsgSerde).
		StoreName("test2").ParNum(0).
		SerdeFormat(commtypes.JSON).ChangelogManagerParam(commtypes.CreateChangelogManagerParam{
		Env:           h.env,
		NumPartition:  1,
		FlushDuration: time.Duration(5) * time.Millisecond,
		TimeOut:       common.SrcConsumeTimeout,
	}).Build()
	if err != nil {
		panic(err)
	}
	wstore, err := store_with_changelog.NewInMemoryWindowStoreWithChangelogForTest(store.TEST_RETENTION_PERIOD, store.TEST_WINDOW_SIZE, false,
		store.Uint32IntrCompare, mp)
	if err != nil {
		panic(err)
	}
	ss := commtypes.MessageJSONSerde{
		KeySerde: commtypes.KeyAndWindowStartTsJSONSerde{
			KeyJSONSerde: commtypes.IntSerde{},
		},
		ValSerde: commtypes.StringSerde{},
	}
	changelog := wstore.ChangelogManager().Stream().(*sharedlog_stream.ShardedSharedLogStream)
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
	wstore2, err := store_with_changelog.NewInMemoryWindowStoreWithChangelogForTest(store.TEST_RETENTION_PERIOD, store.TEST_WINDOW_SIZE, false,
		store.Uint32IntrCompare, mp)
	if err != nil {
		panic(err)
	}
	err = store_restore.RestoreChangelogWindowStateStore(ctx, wstore2, 0)
	if err != nil {
		panic(err)
	}
	ret := make(map[int]*commtypes.ValueTimestamp)
	err = wstore2.FetchAll(ctx, time.UnixMilli(0), time.UnixMilli(store.TEST_WINDOW_SIZE*2),
		func(i int64, kt commtypes.KeyT, vt commtypes.ValueT) error {
			ret[kt.(int)] = commtypes.CreateValueTimestamp(vt, i)
			return nil
		})
	if err != nil {
		panic(err)
	}
	expected := map[int]*commtypes.ValueTimestamp{
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
