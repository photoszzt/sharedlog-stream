package store

import (
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/stream/processor/concurrent_skiplist"
)

type StateStore interface {
	Name() string
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
