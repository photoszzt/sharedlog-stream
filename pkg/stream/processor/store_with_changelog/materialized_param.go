package store_with_changelog

import (
	"sharedlog-stream/pkg/concurrent_skiplist"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/transaction"
)

type MaterializeParam struct {
	KeySerde    commtypes.Serde
	ValueSerde  commtypes.Serde
	MsgSerde    commtypes.MsgSerde
	Comparable  concurrent_skiplist.Comparable
	Changelog   *sharedlog_stream.ShardedSharedLogStream
	TrackFunc   transaction.TrackKeySubStreamFunc
	StoreName   string
	ParNum      uint8
	SerdeFormat commtypes.SerdeFormat
}

func NewMaterializeParamForWindowStore(
	keySerde commtypes.Serde, valueSerde commtypes.Serde,
	msgSerde commtypes.MsgSerde, changelog *sharedlog_stream.ShardedSharedLogStream,
	trackFunc transaction.TrackKeySubStreamFunc, StoreName string,
	parNum uint8, serdeFormat commtypes.SerdeFormat,
) *MaterializeParam {
	return &MaterializeParam{
		KeySerde:    keySerde,
		ValueSerde:  valueSerde,
		MsgSerde:    msgSerde,
		Changelog:   changelog,
		TrackFunc:   trackFunc,
		StoreName:   StoreName,
		ParNum:      parNum,
		SerdeFormat: serdeFormat,
	}
}
