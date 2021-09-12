package types

import (
	"encoding/json"

	"cs.utexas.edu/zhitingz/sharedlog-stream/pkg/stream/processor"
)

type AuctionIdCount struct {
	AucId uint64 `zid:"0"`
	Count uint64 `zid:"1"`
}

type AuctionIdCountJSONEncoder struct{}

var _ = processor.Encoder(AuctionIdCountJSONEncoder{})

func (e AuctionIdCountJSONEncoder) Encode(value interface{}) ([]byte, error) {
	se := value.(*AuctionIdCount)
	return json.Marshal(se)
}

type AuctionIdCountJSONDecoder struct{}

var _ = processor.Decoder(AuctionIdCountJSONDecoder{})

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

var _ = processor.Encoder(AuctionIdCountMsgpEncoder{})

func (e AuctionIdCountMsgpEncoder) Encode(value interface{}) ([]byte, error) {
	se := value.(*AuctionIdCount)
	return se.MarshalMsg(nil)
}

type AuctionIdCountMsgpDecoder struct{}

var _ = processor.Decoder(AuctionIdCountMsgpDecoder{})

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
