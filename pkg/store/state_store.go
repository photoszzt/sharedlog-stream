package store

import (
	"sharedlog-stream/pkg/commtypes"
)

type StateStore interface {
	Name() string
	IsOpen() bool
}

type JoinParam struct {
	KeySerde             commtypes.Serde
	ValueSerde           commtypes.Serde
	OtherValueSerde      commtypes.Serde
	MsgSerde             commtypes.MsgSerde
	LeftWindowStoreName  string
	RightWindowStoreName string
}
