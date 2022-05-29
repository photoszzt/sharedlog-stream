package store_with_changelog

import (
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/transaction/tran_interface"
)

type MaterializeParam struct {
	kvMsgSerdes      commtypes.KVMsgSerdes
	changelogManager *ChangelogManager
	trackFunc        tran_interface.TrackKeySubStreamFunc
	storeName        string
	parNum           uint8
	serdeFormat      commtypes.SerdeFormat
}

func (m *MaterializeParam) ChangelogManager() *ChangelogManager {
	return m.changelogManager
}

func (m *MaterializeParam) StoreName() string {
	return m.storeName
}

func (m *MaterializeParam) ParNum() uint8 {
	return m.parNum
}

func (m *MaterializeParam) SerdeFormat() commtypes.SerdeFormat {
	return m.serdeFormat
}

func (m *MaterializeParam) KVMsgSerdes() commtypes.KVMsgSerdes {
	return m.kvMsgSerdes
}

func (m *MaterializeParam) TrackFunc() tran_interface.TrackKeySubStreamFunc {
	return m.trackFunc
}

func (m *MaterializeParam) SetTrackParFunc(trackFunc tran_interface.TrackKeySubStreamFunc) {
	m.trackFunc = trackFunc
}

type MaterializeParamBuilder struct {
	mp          *MaterializeParam
	streamParam commtypes.CreateStreamParam
}

func NewMaterializeParamBuilder() SetKVMsgSerdes {
	mb := &MaterializeParamBuilder{}
	return mb
}

type SetKVMsgSerdes interface {
	KVMsgSerdes(kvmsgserdes commtypes.KVMsgSerdes) SetStoreName
}

type SetStoreName interface {
	StoreName(storeName string) SetParNum
}

type SetParNum interface {
	ParNum(parNum uint8) SetSerdeFormat
}

type SetSerdeFormat interface {
	SerdeFormat(serdeFormat commtypes.SerdeFormat) BuildMaterializeParam
}

type BuildMaterializeParam interface {
	StreamParam(streamParam commtypes.CreateStreamParam) BuildMaterializeParam
	ChangelogManager(changelogManager *ChangelogManager) BuildMaterializeParam
	Build() (*MaterializeParam, error)
}

func (mb *MaterializeParamBuilder) KVMsgSerdes(kvmsgserdes commtypes.KVMsgSerdes) SetStoreName {
	mb.mp.kvMsgSerdes = kvmsgserdes
	return mb
}

func (mb *MaterializeParamBuilder) StoreName(storeName string) SetParNum {
	mb.mp.storeName = storeName
	return mb
}

func (mb *MaterializeParamBuilder) ParNum(parNum uint8) SetSerdeFormat {
	mb.mp.parNum = parNum
	return mb
}

func (mb *MaterializeParamBuilder) SerdeFormat(serdeFormat commtypes.SerdeFormat) BuildMaterializeParam {
	mb.mp.serdeFormat = serdeFormat
	return mb
}

func (mb *MaterializeParamBuilder) StreamParam(streamParam commtypes.CreateStreamParam) BuildMaterializeParam {
	mb.streamParam = streamParam
	return mb
}

func (mb *MaterializeParamBuilder) ChangelogManager(changelogManager *ChangelogManager) BuildMaterializeParam {
	mb.mp.changelogManager = changelogManager
	return mb
}

func (mb *MaterializeParamBuilder) Build() (*MaterializeParam, error) {
	mb.mp.trackFunc = tran_interface.DefaultTrackSubstreamFunc
	if mb.mp.changelogManager != nil {
		return mb.mp, nil
	} else {
		changelog, err := CreateChangelog(mb.streamParam.Env,
			mb.mp.storeName, mb.streamParam.NumPartition, mb.mp.serdeFormat)
		if err != nil {
			return nil, err
		}
		mb.mp.changelogManager = NewChangelogManager(changelog, mb.mp.serdeFormat)
		return mb.mp, nil
	}
}

/*
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
		KVMsgSerdes:       kvmsgSerdes,
		ChangelogManager_: NewChangelogManager(changelog, streamParam.Format),
		TrackFunc:         tran_interface.DefaultTrackSubstreamFunc,
		StoreName:         storeName,
		ParNum:            parNum,
		SerdeFormat:       streamParam.Format,
		comparable:        comparable,
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
		KVMsgSerdes:       kvmsgSerdes,
		StoreName:         storeName,
		TrackFunc:         tran_interface.DefaultTrackSubstreamFunc,
		ChangelogManager_: NewChangelogManager(changelog, streamParam.Format),
		ParNum:            parNum,
		SerdeFormat:       streamParam.Format,
	}, nil
}
*/
