package store_with_changelog

import (
	"sharedlog-stream/pkg/commtypes"
)

type MaterializeParam struct {
	msgSerde       commtypes.MessageSerde
	storeName      string
	changelogParam commtypes.CreateChangelogManagerParam
	parNum         uint8
	serdeFormat    commtypes.SerdeFormat
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

func (m *MaterializeParam) MessageSerde() commtypes.MessageSerde {
	return m.msgSerde
}

type MaterializeParamBuilder struct {
	mp *MaterializeParam
}

func NewMaterializeParamBuilder() SetMessageSerde {
	mb := &MaterializeParamBuilder{
		mp: &MaterializeParam{},
	}
	return mb
}

type SetMessageSerde interface {
	MessageSerde(kvmsgserdes commtypes.MessageSerde) SetStoreName
}

type SetStoreName interface {
	StoreName(storeName string) SetParNum
}

type SetParNum interface {
	ParNum(parNum uint8) SetSerdeFormat
}

type SetSerdeFormat interface {
	SerdeFormat(serdeFormat commtypes.SerdeFormat) SetCreateChangelogManagerParam
}

type SetCreateChangelogManagerParam interface {
	ChangelogManagerParam(streamParam commtypes.CreateChangelogManagerParam) BuildMaterializeParam
}

type BuildMaterializeParam interface {
	Build() (*MaterializeParam, error)
}

func (mb *MaterializeParamBuilder) MessageSerde(msgSerde commtypes.MessageSerde) SetStoreName {
	mb.mp.msgSerde = msgSerde
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

func (mb *MaterializeParamBuilder) SerdeFormat(serdeFormat commtypes.SerdeFormat) SetCreateChangelogManagerParam {
	mb.mp.serdeFormat = serdeFormat
	return mb
}

func (mb *MaterializeParamBuilder) ChangelogManagerParam(changelogParam commtypes.CreateChangelogManagerParam) BuildMaterializeParam {
	mb.mp.changelogParam = changelogParam
	return mb
}

func (mb *MaterializeParamBuilder) Build() (*MaterializeParam, error) {
	return mb.mp, nil
}
