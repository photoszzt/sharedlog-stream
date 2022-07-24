package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sharedlog-stream/benchmark/tests/pkg/tests"
	"sharedlog-stream/benchmark/tests/pkg/tests/test_types"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/concurrent_skiplist"
	"sharedlog-stream/pkg/store"
	"sharedlog-stream/pkg/store_with_changelog"
	"time"

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

func getWindowStoreWithChangelog(env types.Environment, retainDuplicates bool) *store_with_changelog.InMemoryWindowStoreWithChangelog[uint32, string] {
	msgSerde := commtypes.MessageJSONSerdeG[uint32, string]{
		KeySerde: commtypes.Uint32SerdeG{},
		ValSerde: commtypes.StringSerdeG{},
	}
	storeName := "test1"
	var compareFunc concurrent_skiplist.CompareFunc
	if !retainDuplicates {
		compareFunc = store.CompareNoDup
	} else {
		compareFunc = store.CompareWithDup
	}
	mp, err := store_with_changelog.NewMaterializeParamBuilder[uint32, string]().
		MessageSerde(msgSerde).
		StoreName(storeName).
		ParNum(0).
		SerdeFormat(commtypes.JSON).
		ChangelogManagerParam(commtypes.CreateChangelogManagerParam{
			Env:           env,
			NumPartition:  1,
			TimeOut:       common.SrcConsumeTimeout,
			FlushDuration: time.Duration(5) * time.Millisecond,
		}).Build()
	if err != nil {
		panic(err)
	}
	store, err := store_with_changelog.NewInMemoryWindowStoreWithChangelogForTest(
		store.TEST_RETENTION_PERIOD, store.TEST_WINDOW_SIZE, retainDuplicates, compareFunc,
		mp,
	)
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
	t := &tests.MockTesting{}
	var winstore *store_with_changelog.InMemoryWindowStoreWithChangelog[uint32, string]
	if sp.TestName != "TestPutSameKeyTs" {
		winstore = getWindowStoreWithChangelog(wt.env, false)
	} else {
		winstore = getWindowStoreWithChangelog(wt.env, true)
	}
	switch sp.TestName {
	case "TestGetAndRange":
		store.GetAndRangeTest(ctx, winstore, t)
	case "TestShouldGetAllNonDeletedMsgs":
		store.ShouldGetAllNonDeletedMsgsTest(ctx, winstore, t)
	case "TestExpiration":
		store.ExpirationTest(ctx, winstore, t)
	case "TestShouldGetAll":
		store.ShouldGetAllTest(ctx, winstore, t)
	case "TestShouldGetAllReturnTimestampOrdered":
		store.ShouldGetAllReturnTimestampOrderedTest(ctx, winstore, t)
	case "TestFetchRange":
		store.FetchRangeTest(ctx, winstore, t)
	case "TestPutAndFetchBefore":
		store.PutAndFetchBeforeTest(ctx, winstore, t)
	case "TestPutAndFetchAfter":
		store.PutAndFetchAfterTest(ctx, winstore, t)
	case "TestPutSameKeyTs":
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
