package store

import "sharedlog-stream/pkg/stream/processor/commtypes"

type StateStore interface {
	Name() string
}

type MaterializeParam struct {
	KeySerde   commtypes.Serde
	ValueSerde commtypes.Serde
	MsgSerde   commtypes.MsgSerde
	Changelog  Stream
	StoreName  string
	ParNum     uint8
}

type JoinParam struct {
	KeySerde             commtypes.Serde
	ValueSerde           commtypes.Serde
	OtherValueSerde      commtypes.Serde
	MsgSerde             commtypes.MsgSerde
	LeftWindowStoreName  string
	RightWindowStoreName string
}
