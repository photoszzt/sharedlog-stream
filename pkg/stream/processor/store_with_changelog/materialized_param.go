package store_with_changelog

import (
	"sharedlog-stream/pkg/concurrent_skiplist"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/transaction/tran_interface"

	"cs.utexas.edu/zjia/faas/types"
)

type MaterializeParam struct {
	KVMsgSerdes      commtypes.KVMsgSerdes
	Comparable       concurrent_skiplist.Comparable
	ChangelogManager *ChangelogManager
	TrackFunc        tran_interface.TrackKeySubStreamFunc
	StoreName        string
	ParNum           uint8
	SerdeFormat      commtypes.SerdeFormat
}

func NewMaterializeParamForWindowStore(
	env types.Environment,
	kvmsgSerdes commtypes.KVMsgSerdes,
	storeName string, streamParam commtypes.CreateStreamParam, parNum uint8,
	comparable concurrent_skiplist.Comparable,
) (*MaterializeParam, error) {
	changelog, err := CreateChangelog(env, storeName, streamParam.NumPartition, streamParam.Format)
	if err != nil {
		return nil, err
	}
	return &MaterializeParam{
		KVMsgSerdes:      kvmsgSerdes,
		ChangelogManager: NewChangelogManager(changelog, streamParam.Format),
		TrackFunc:        tran_interface.DefaultTrackSubstreamFunc,
		StoreName:        storeName,
		ParNum:           parNum,
		SerdeFormat:      streamParam.Format,
		Comparable:       comparable,
	}, nil
}

func NewMaterializeParamForKeyValueStore(
	env types.Environment,
	kvmsgSerdes commtypes.KVMsgSerdes,
	storeName string, streamParam commtypes.CreateStreamParam, parNum uint8,
) (*MaterializeParam, error) {
	changelog, err := CreateChangelog(env, storeName, streamParam.NumPartition, streamParam.Format)
	if err != nil {
		return nil, err
	}
	return &MaterializeParam{
		KVMsgSerdes:      kvmsgSerdes,
		StoreName:        storeName,
		TrackFunc:        tran_interface.DefaultTrackSubstreamFunc,
		ChangelogManager: NewChangelogManager(changelog, streamParam.Format),
		ParNum:           parNum,
		SerdeFormat:      streamParam.Format,
	}, nil
}
