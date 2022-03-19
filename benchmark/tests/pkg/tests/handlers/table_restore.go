package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sharedlog-stream/benchmark/tests/pkg/tests/test_types"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/stream/processor/store"
	"sharedlog-stream/pkg/treemap"

	"cs.utexas.edu/zjia/faas/types"
)

type tableRestoreHandler struct {
	env      types.Environment
	kSerde   commtypes.Serde
	vSerde   commtypes.Serde
	msgSerde commtypes.MsgSerde
}

func NewTableRestoreHandler(env types.Environment) types.FuncHandler {
	return &tableRestoreHandler{
		env:      env,
		kSerde:   commtypes.IntSerde{},
		vSerde:   commtypes.StringSerde{},
		msgSerde: commtypes.MessageSerializedJSONSerde{},
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

func (h *tableRestoreHandler) pushToLog(ctx context.Context, key int, val string, log *sharedlog_stream.ShardedSharedLogStream) (uint64, error) {
	keyBytes, err := h.kSerde.Encode(key)
	if err != nil {
		return 0, err
	}
	valBytes, err := h.vSerde.Encode(val)
	if err != nil {
		return 0, err
	}
	encoded, err := h.msgSerde.Encode(keyBytes, valBytes)
	if err != nil {
		return 0, err
	}
	return log.Push(ctx, encoded, 0, false)
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
	if _, err = h.pushToLog(ctx, 0, "zero", changelog); err != nil {
		panic(err)
	}
	if _, err = h.pushToLog(ctx, 1, "one", changelog); err != nil {
		panic(err)
	}
	if _, err = h.pushToLog(ctx, 2, "two", changelog); err != nil {
		panic(err)
	}
	if offset, err = h.pushToLog(ctx, 3, "three", changelog); err != nil {
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
	err = store.RestoreKVStateStore(ctx,
		store.NewKVStoreChangelog(kvstore, changelog, commtypes.IntSerde{}, commtypes.IntSerde{}, 0),
		h.msgSerde, offset)
	if err != nil {
		panic(err)
	}
	ret := make(map[int]string)
	err = kvstore.Range(ctx, nil, nil, func(kt commtypes.KeyT, vt commtypes.ValueT) error {
		ret[kt.(int)] = vt.(string)
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
