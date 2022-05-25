//go:generate msgp
//msgp:ignore AuctionIdCountJSONEncoder AuctionIdCountJSONDecoder AuctionIdCountJSONSerde
//msgp:ignore AuctionIdCountMsgpEncoder AuctionIdCountMsgpDecoder AuctionIdCountMsgpSerde
package types

import (
	"encoding/json"

	"sharedlog-stream/pkg/stream/processor/commtypes"
)

type AuctionIdCount struct {
	AucId uint64 `json:"aucId,omitempty" msg:"aucId,omitempty"`
	Count uint64 `json:"cnt,omitempty" msg:"cnt,omitempty"`

	BaseTs      `msg:"bTs"`
	BaseInjTime `msg:"bInjT"`
}

type AuctionIdCountJSONEncoder struct{}

var _ = commtypes.Encoder(AuctionIdCountJSONEncoder{})

func (e AuctionIdCountJSONEncoder) Encode(value interface{}) ([]byte, error) {
	se := value.(*AuctionIdCount)
	return json.Marshal(se)
}

type AuctionIdCountJSONDecoder struct{}

var _ = commtypes.Decoder(AuctionIdCountJSONDecoder{})

func (d AuctionIdCountJSONDecoder) Decode(value []byte) (interface{}, error) {
	se := &AuctionIdCount{}
	err := json.Unmarshal(value, se)
	if err != nil {
		return nil, err
	}
	return se, nil
}

type AuctionIdCountJSONSerde struct {
	AuctionIdCountJSONEncoder
	AuctionIdCountJSONDecoder
}

type AuctionIdCountMsgpEncoder struct{}

var _ = commtypes.Encoder(AuctionIdCountMsgpEncoder{})

func (e AuctionIdCountMsgpEncoder) Encode(value interface{}) ([]byte, error) {
	se := value.(*AuctionIdCount)
	return se.MarshalMsg(nil)
}

type AuctionIdCountMsgpDecoder struct{}

var _ = commtypes.Decoder(AuctionIdCountMsgpDecoder{})

func (d AuctionIdCountMsgpDecoder) Decode(value []byte) (interface{}, error) {
	se := &AuctionIdCount{}
	_, err := se.UnmarshalMsg(value)
	if err != nil {
		return nil, err
	}
	return se, nil
}

type AuctionIdCountMsgpSerde struct {
	AuctionIdCountMsgpEncoder
	AuctionIdCountMsgpDecoder
}
