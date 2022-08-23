package execution

import (
	"context"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/proc_interface"
	"sharedlog-stream/pkg/processor"
	"sharedlog-stream/pkg/store"
	"sharedlog-stream/pkg/store_with_changelog"
	"sharedlog-stream/pkg/utils"
)

func SetupTableTableJoin[K, VLeft, VRight any](
	mpLeft *store_with_changelog.MaterializeParam[K, VLeft],
	mpRight *store_with_changelog.MaterializeParam[K, VRight],
	compare store.KVStoreLessFunc,
	joiner processor.ValueJoinerWithKeyFunc,
) (proc_interface.ProcessAndReturnFunc,
	proc_interface.ProcessAndReturnFunc,
	[]store.KeyValueStoreOpWithChangelog,
	error,
) {
	toLeftTab, leftTab, err := store_with_changelog.ToInMemKVTableWithChangelog(mpLeft, compare)
	if err != nil {
		return nil, nil, nil, err
	}
	toRightTab, rightTab, err := store_with_changelog.ToInMemKVTableWithChangelog(mpRight, compare)
	if err != nil {
		return nil, nil, nil, err
	}

	leftJoinRight := processor.NewMeteredProcessor(
		processor.NewTableTableJoinProcessor("leftJoinRight", rightTab, joiner))
	rightJoinLeft := processor.NewMeteredProcessor(
		processor.NewTableTableJoinProcessor("rightJoinLeft", leftTab,
			processor.ReverseValueJoinerWithKey(joiner)))
	leftJoinRightFunc := func(ctx context.Context, msg commtypes.Message) ([]commtypes.Message, error) {
		ret, err := toLeftTab.ProcessAndReturn(ctx, msg)
		if err != nil {
			return nil, err
		}
		if ret != nil {
			return leftJoinRight.ProcessAndReturn(ctx, ret[0])
		} else {
			return nil, nil
		}
	}
	rightJoinLeftFunc := func(ctx context.Context, msg commtypes.Message) ([]commtypes.Message, error) {
		ret, err := toRightTab.ProcessAndReturn(ctx, msg)
		if err != nil {
			return nil, err
		}
		if ret != nil {
			return rightJoinLeft.ProcessAndReturn(ctx, ret[0])
		} else {
			return nil, nil
		}
	}
	kvc := []store.KeyValueStoreOpWithChangelog{leftTab, rightTab}
	return leftJoinRightFunc, rightJoinLeftFunc, kvc, nil
}

func SetupTableTableJoinWithSkipmap[K, VLeft, VRight, VR any](
	mpLeft *store_with_changelog.MaterializeParam[K, commtypes.ValueTimestampG[VLeft]],
	mpRight *store_with_changelog.MaterializeParam[K, commtypes.ValueTimestampG[VRight]],
	less store.LessFunc[K],
	joiner processor.ValueJoinerWithKeyFuncG[K, VLeft, VRight, VR],
) (proc_interface.ProcessAndReturnFunc,
	proc_interface.ProcessAndReturnFunc,
	map[string]store.KeyValueStoreOpWithChangelog,
	error,
) {
	toLeftTab, leftTab, err := store_with_changelog.ToInMemSkipmapKVTableWithChangelog[K, VLeft](mpLeft, less)
	if err != nil {
		return nil, nil, nil, err
	}
	toRightTab, rightTab, err := store_with_changelog.ToInMemSkipmapKVTableWithChangelog[K, VRight](mpRight, less)
	if err != nil {
		return nil, nil, nil, err
	}

	leftJoinRight := processor.NewMeteredProcessor(
		processor.NewTableTableJoinProcessorG[K, VLeft, VRight, VR]("leftJoinRight", rightTab, joiner))
	rightJoinLeft := processor.NewMeteredProcessor(
		processor.NewTableTableJoinProcessorG[K, VRight, VLeft, VR]("rightJoinLeft", leftTab,
			processor.ReverseValueJoinerWithKeyG(joiner)))
	nullKeyFilter := processor.NewMeteredProcessor(processor.NewStreamFilterProcessor("filterNullKey",
		processor.PredicateFunc(func(key interface{}, value interface{}) (bool, error) {
			return !utils.IsNil(key), nil
		})))
	leftJoinRightFunc := func(ctx context.Context, msg commtypes.Message) ([]commtypes.Message, error) {
		ret, err := toLeftTab.ProcessAndReturn(ctx, msg)
		if err != nil {
			return nil, err
		}
		if ret != nil {
			msgs, err := leftJoinRight.ProcessAndReturn(ctx, ret[0])
			if err != nil {
				return nil, err
			}
			if msgs != nil {
				return nullKeyFilter.ProcessAndReturn(ctx, msgs[0])
			}
		}
		return nil, nil
	}
	rightJoinLeftFunc := func(ctx context.Context, msg commtypes.Message) ([]commtypes.Message, error) {
		ret, err := toRightTab.ProcessAndReturn(ctx, msg)
		if err != nil {
			return nil, err
		}
		if ret != nil {
			msgs, err := rightJoinLeft.ProcessAndReturn(ctx, ret[0])
			if err != nil {
				return nil, err
			}
			if msgs != nil {
				return nullKeyFilter.ProcessAndReturn(ctx, msgs[0])
			}
		}
		return nil, nil
	}
	kvc := map[string]store.KeyValueStoreOpWithChangelog{leftTab.ChangelogTopicName(): leftTab, rightTab.ChangelogTopicName(): rightTab}
	return leftJoinRightFunc, rightJoinLeftFunc, kvc, nil
}
