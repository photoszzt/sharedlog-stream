package store

import (
	"sharedlog-stream/pkg/concurrent_skiplist"
	"sharedlog-stream/pkg/stream/processor/commtypes"
)

type StateStore interface {
	Name() string
	IsOpen() bool
}

type MaterializeParam struct {
	KeySerde    commtypes.Serde
	ValueSerde  commtypes.Serde
	MsgSerde    commtypes.MsgSerde
	Changelog   Stream
	Comparable  concurrent_skiplist.Comparable
	StoreName   string
	ParNum      uint8
	SerdeFormat commtypes.SerdeFormat
}

type JoinParam struct {
	KeySerde             commtypes.Serde
	ValueSerde           commtypes.Serde
	OtherValueSerde      commtypes.Serde
	MsgSerde             commtypes.MsgSerde
	LeftWindowStoreName  string
	RightWindowStoreName string
}
