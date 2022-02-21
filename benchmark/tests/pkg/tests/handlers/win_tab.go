package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sharedlog-stream/benchmark/tests/pkg/tests"
	"sharedlog-stream/benchmark/tests/pkg/tests/test_types"
	"sharedlog-stream/pkg/concurrent_skiplist"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/stream/processor/store"

	"cs.utexas.edu/zjia/faas/types"
)

type winTabTestsHandler struct {
	env types.Environment
}

func NewWinTabTestsHandler(env types.Environment) types.FuncHandler {
	return &winTabTestsHandler{
		env: env,
	}
}

func getWindowStoreWithChangelog(retainDuplicates bool, changelog store.Stream) *store.InMemoryWindowStoreWithChangelog {
	mp := &store.MaterializeParam{
		KeySerde:    commtypes.Uint32Serde{},
		ValueSerde:  commtypes.StringSerde{},
		MsgSerde:    commtypes.MessageSerializedJSONSerde{},
		StoreName:   "test1",
		SerdeFormat: commtypes.JSON,
		ParNum:      0,
		Changelog:   changelog,
	}
	if !retainDuplicates {
		mp.Comparable = concurrent_skiplist.CompareFunc(store.CompareNoDup)
		store, err := store.NewInMemoryWindowStoreWithChangelog(store.TEST_RETENTION_PERIOD, store.TEST_WINDOW_SIZE,
			retainDuplicates, mp)
		if err != nil {
			panic(err)
		}
		return store
	}
	mp.Comparable = concurrent_skiplist.CompareFunc(store.CompareWithDup)
	store, err := store.NewInMemoryWindowStoreWithChangelog(store.TEST_RETENTION_PERIOD, store.TEST_WINDOW_SIZE,
		retainDuplicates, mp)
	if err != nil {
		panic(err)
	}
	return store
}

func (wt *winTabTestsHandler) Call(ctx context.Context, input []byte) ([]byte, error) {
	parsedInput := &test_types.TestInput{}
	err := json.Unmarshal(input, parsedInput)
	if err != nil {
		return nil, err
	}
	output := wt.WinTests(ctx, parsedInput)
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		panic(err)
	}
	return utils.CompressData(encodedOutput), nil
}

func (wt *winTabTestsHandler) WinTests(ctx context.Context, sp *test_types.TestInput) *common.FnOutput {
	stream, err := sharedlog_stream.NewShardedSharedLogStream(wt.env, sp.TopicName, 1, commtypes.JSON)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}
	t := &tests.MockTesting{}
	switch sp.TestName {
	case "TestGetAndRange":
		winstore := getWindowStoreWithChangelog(false, stream)
		store.GetAndRangeTest(winstore, t)
	case "TestShouldGetAllNonDeletedMsgs":
		winstore := getWindowStoreWithChangelog(false, stream)
		store.ShouldGetAllNonDeletedMsgsTest(winstore, t)
	case "TestExpiration":
		winstore := getWindowStoreWithChangelog(false, stream)
		store.ExpirationTest(winstore, t)
	case "TestShouldGetAll":
		winstore := getWindowStoreWithChangelog(false, stream)
		store.ShouldGetAllTest(winstore, t)
	case "TestShouldGetAllReturnTimestampOrdered":
		winstore := getWindowStoreWithChangelog(false, stream)
		store.ShouldGetAllReturnTimestampOrderedTest(winstore, t)
	case "TestFetchRange":
		winstore := getWindowStoreWithChangelog(false, stream)
		store.FetchRangeTest(ctx, winstore, t)
	case "TestPutAndFetchBefore":
		winstore := getWindowStoreWithChangelog(false, stream)
		store.PutAndFetchBeforeTest(ctx, winstore, t)
	case "TestPutAndFetchAfter":
		winstore := getWindowStoreWithChangelog(false, stream)
		store.PutAndFetchAfterTest(ctx, winstore, t)
	case "TestPutSameKeyTs":
		winstore := getWindowStoreWithChangelog(true, stream)
		store.PutSameKeyTsTest(ctx, winstore, t)
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
