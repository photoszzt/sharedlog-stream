package execution

import (
	"context"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/proc_interface"
	"sharedlog-stream/pkg/processor"
	"sharedlog-stream/pkg/store"
	"sharedlog-stream/pkg/store_with_changelog"
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
	mpLeft *store_with_changelog.MaterializeParam[K, *commtypes.ValueTimestamp],
	mpRight *store_with_changelog.MaterializeParam[K, *commtypes.ValueTimestamp],
	less store.LessFunc[K],
	joiner processor.ValueJoinerWithKeyFuncG[K, VLeft, VRight, VR],
) (proc_interface.ProcessAndReturnFunc,
	proc_interface.ProcessAndReturnFunc,
	[]store.KeyValueStoreOpWithChangelog,
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
