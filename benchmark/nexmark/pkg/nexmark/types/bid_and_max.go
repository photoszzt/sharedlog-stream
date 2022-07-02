//go:generate msgp
//msgp:ignore BidAndMaxJSONSerde BidAndMaxMsgpSerde
package types

import (
	"encoding/json"
	"fmt"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/commtypes"
)

type BidAndMax struct {
	Price    uint64 `json:"price" msg:"price"`
	Auction  uint64 `json:"auction" msg:"auction"`
	Bidder   uint64 `json:"bidder" msg:"bidder"`
	BidTs    int64  `json:"bTs" msg:"bTs"`
	WStartMs int64  `json:"wStartMs" msg:"wStartMs"`
	WEndMs   int64  `json:"wEndMs" msg:"wEndMs"`
}

var _ = fmt.Stringer(BidAndMax{})

func (bm BidAndMax) String() string {
	return fmt.Sprintf("BidAndMax: {Price: %d, Auction: %d, Bidder: %d, BidTs: %d, WinStartMs: %d, WinEndMs: %d}",
		bm.Price, bm.Auction, bm.Bidder, bm.BidTs, bm.WStartMs, bm.WEndMs)
}

type BidAndMaxJSONSerde struct{}

func (s BidAndMaxJSONSerde) Encode(value interface{}) ([]byte, error) {
	bm := value.(*BidAndMax)
	return json.Marshal(bm)
}

func (s BidAndMaxJSONSerde) Decode(value []byte) (interface{}, error) {
	bm := BidAndMax{}
	if err := json.Unmarshal(value, &bm); err != nil {
		return nil, err
	}
	return bm, nil
}

type BidAndMaxMsgpSerde struct{}

func (s BidAndMaxMsgpSerde) Encode(value interface{}) ([]byte, error) {
	bm := value.(*BidAndMax)
	return bm.MarshalMsg(nil)
}

func (s BidAndMaxMsgpSerde) Decode(value []byte) (interface{}, error) {
	bm := BidAndMax{}
	if _, err := bm.UnmarshalMsg(value); err != nil {
		return nil, err
	}
	return bm, nil
}

func GetBidAndMaxSerde(serdeFormat commtypes.SerdeFormat) (commtypes.Serde, error) {
	if serdeFormat == commtypes.JSON {
		return BidAndMaxJSONSerde{}, nil
	} else if serdeFormat == commtypes.MSGP {
		return BidAndMaxMsgpSerde{}, nil
	} else {
		return nil, common_errors.ErrUnrecognizedSerdeFormat
	}
}
