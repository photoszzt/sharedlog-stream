package store_with_changelog

import (
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/exactly_once_intr"
	"time"
)

type MaterializeParam struct {
	kvMsgSerdes      commtypes.KVMsgSerdes
	changelogManager *ChangelogManager
	trackFunc        exactly_once_intr.TrackProdSubStreamFunc
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

func (m *MaterializeParam) TrackFunc() exactly_once_intr.TrackProdSubStreamFunc {
	return m.trackFunc
}

func (m *MaterializeParam) SetTrackParFunc(trackFunc exactly_once_intr.TrackProdSubStreamFunc) {
	m.trackFunc = trackFunc
}

type MaterializeParamBuilder struct {
	mp          *MaterializeParam
	streamParam commtypes.CreateStreamParam
}

func NewMaterializeParamBuilder() SetKVMsgSerdes {
	mb := &MaterializeParamBuilder{
		mp: &MaterializeParam{
			trackFunc: exactly_once_intr.DefaultTrackProdSubstreamFunc,
		},
	}
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
	Build(flushDuration time.Duration, timeOut time.Duration) (*MaterializeParam, error)
	BuildForWindowStore(flushDuration time.Duration, timeOut time.Duration) (*MaterializeParam, error)
	BuildWithChangelogManager(changelogManager *ChangelogManager) (*MaterializeParam, error)
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

func (mb *MaterializeParamBuilder) Build(flushDuration time.Duration, timeOut time.Duration) (*MaterializeParam, error) {
	if mb.mp.changelogManager != nil {
		return mb.mp, nil
	} else {
		changelog, err := CreateChangelog(mb.streamParam.Env,
			mb.mp.storeName, mb.streamParam.NumPartition, mb.mp.serdeFormat)
		if err != nil {
			return nil, err
		}
		mb.mp.changelogManager = NewChangelogManager(changelog,
			mb.mp.kvMsgSerdes, timeOut, flushDuration)
		return mb.mp, nil
	}
}

func (mb *MaterializeParamBuilder) BuildWithChangelogManager(changelogManager *ChangelogManager) (*MaterializeParam, error) {
	mb.mp.changelogManager = changelogManager
	return mb.mp, nil
}

func (mb *MaterializeParamBuilder) BuildForWindowStore(flushDuration time.Duration, timeOut time.Duration) (*MaterializeParam, error) {
	if mb.mp.changelogManager != nil {
		return mb.mp, nil
	} else {
		changelog, err := CreateChangelog(mb.streamParam.Env,
			mb.mp.storeName, mb.streamParam.NumPartition, mb.mp.serdeFormat)
		if err != nil {
			return nil, err
		}
		var keyAndWindowStartTsSerde commtypes.Serde
		if mb.mp.serdeFormat == commtypes.JSON {
			keyAndWindowStartTsSerde = commtypes.KeyAndWindowStartTsJSONSerde{
				KeyJSONSerde: mb.mp.kvMsgSerdes.KeySerde,
			}
		} else if mb.mp.serdeFormat == commtypes.MSGP {
			keyAndWindowStartTsSerde = commtypes.KeyAndWindowStartTsMsgpSerde{
				KeyMsgpSerde: mb.mp.kvMsgSerdes.KeySerde,
			}
		} else {
			return nil, common_errors.ErrUnrecognizedSerdeFormat
		}
		kvMsgSerdes := commtypes.KVMsgSerdes{
			KeySerde: keyAndWindowStartTsSerde,
			ValSerde: mb.mp.kvMsgSerdes.ValSerde,
			MsgSerde: mb.mp.kvMsgSerdes.MsgSerde,
		}
		mb.mp.changelogManager = NewChangelogManager(changelog, kvMsgSerdes,
			timeOut, flushDuration)
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
		TrackFunc:         exactly_once_intr.DefaultTrackSubstreamFunc,
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
		TrackFunc:         exactly_once_intr.DefaultTrackSubstreamFunc,
		ChangelogManager_: NewChangelogManager(changelog, streamParam.Format),
		ParNum:            parNum,
		SerdeFormat:       streamParam.Format,
	}, nil
}
*/
