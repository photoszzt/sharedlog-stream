package store_with_changelog

import (
	"sharedlog-stream/pkg/commtypes"
)

type MaterializeParam[K, V any] struct {
	msgSerde       commtypes.MessageSerdeG[K, V]
	storeName      string
	changelogParam commtypes.CreateChangelogManagerParam
	parNum         uint8
	serdeFormat    commtypes.SerdeFormat
}

func (m *MaterializeParam[K, V]) StoreName() string {
	return m.storeName
}

func (m *MaterializeParam[K, V]) ParNum() uint8 {
	return m.parNum
}

func (m *MaterializeParam[K, V]) SerdeFormat() commtypes.SerdeFormat {
	return m.serdeFormat
}

func (m *MaterializeParam[K, V]) MessageSerde() commtypes.MessageSerdeG[K, V] {
	return m.msgSerde
}

type MaterializeParamBuilder[K, V any] struct {
	mp *MaterializeParam[K, V]
}

func NewMaterializeParamBuilder[K, V any]() SetMessageSerde[K, V] {
	mb := &MaterializeParamBuilder[K, V]{
		mp: &MaterializeParam[K, V]{},
	}
	return mb
}

type SetMessageSerde[K, V any] interface {
	MessageSerde(kvmsgserdes commtypes.MessageSerdeG[K, V]) SetStoreName[K, V]
}

type SetStoreName[K, V any] interface {
	StoreName(storeName string) SetParNum[K, V]
}

type SetParNum[K, V any] interface {
	ParNum(parNum uint8) SetSerdeFormat[K, V]
}

type SetSerdeFormat[K, V any] interface {
	SerdeFormat(serdeFormat commtypes.SerdeFormat) SetCreateChangelogManagerParam[K, V]
}

type SetCreateChangelogManagerParam[K, V any] interface {
	ChangelogManagerParam(streamParam commtypes.CreateChangelogManagerParam) BuildMaterializeParam[K, V]
}

type BuildMaterializeParam[K, V any] interface {
	Build() (*MaterializeParam[K, V], error)
}

func (mb *MaterializeParamBuilder[K, V]) MessageSerde(msgSerde commtypes.MessageSerdeG[K, V]) SetStoreName[K, V] {
	mb.mp.msgSerde = msgSerde
	return mb
}

func (mb *MaterializeParamBuilder[K, V]) StoreName(storeName string) SetParNum[K, V] {
	mb.mp.storeName = storeName
	return mb
}

func (mb *MaterializeParamBuilder[K, V]) ParNum(parNum uint8) SetSerdeFormat[K, V] {
	mb.mp.parNum = parNum
	return mb
}

func (mb *MaterializeParamBuilder[K, V]) SerdeFormat(serdeFormat commtypes.SerdeFormat) SetCreateChangelogManagerParam[K, V] {
	mb.mp.serdeFormat = serdeFormat
	return mb
}

func (mb *MaterializeParamBuilder[K, V]) ChangelogManagerParam(changelogParam commtypes.CreateChangelogManagerParam) BuildMaterializeParam[K, V] {
	mb.mp.changelogParam = changelogParam
	return mb
}

func (mb *MaterializeParamBuilder[K, V]) Build() (*MaterializeParam[K, V], error) {
	return mb.mp, nil
}
