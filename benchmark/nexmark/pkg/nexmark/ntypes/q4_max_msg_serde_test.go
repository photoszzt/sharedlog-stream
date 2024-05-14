package ntypes

import (
	"reflect"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/optional"
	"testing"
)

func TestQ4MaxMsgSerde(t *testing.T) {
	var abSerde commtypes.SerdeG[*AuctionBid]
	var aicSerde commtypes.SerdeG[AuctionIdCategory]
	var msgSerSerde commtypes.SerdeG[commtypes.MessageSerialized]
	var payloadArrSerde commtypes.SerdeG[commtypes.PayloadArr]
	abSerde = AuctionBidMsgpSerdeG{}
	aicSerde = AuctionIdCategoryMsgpSerdeG{}
	msgSerSerde = commtypes.MessageSerializedMsgpSerdeG{}
	payloadArrSerde = commtypes.PayloadArrMsgpSerdeG{}

	msgSerde, err := commtypes.GetMsgGSerdeG(commtypes.MSGP, aicSerde, abSerde)
	if err != nil {
		t.Fatal(err)
	}
	msg := commtypes.MessageG[AuctionIdCategory, *AuctionBid]{
		Value: optional.Some(&AuctionBid{
			BidDateTime: 1,
			AucDateTime: 2,
			AucExpires:  5,
			BidPrice:    3,
			AucCategory: 100,
			AucSeller:   2,
		}),
		Key: optional.Some(AuctionIdCategory{
			AucId:    1,
			Category: 3,
		}),
	}
	bytes, b, err := msgSerde.Encode(msg)
	if err != nil {
		t.Fatal(err)
	}
	payloadArr := commtypes.PayloadArr{
		Payloads: [][]byte{bytes},
	}
	payloads, b_payload, err := payloadArrSerde.Encode(payloadArr)
	if err != nil {
		t.Fatal(err)
	}

	ret_payload_arr, err := payloadArrSerde.Decode(payloads)
	if err != nil {
		t.Fatal(err)
	}
	ret_msgSer, err := msgSerSerde.Decode(ret_payload_arr.Payloads[0])
	if err != nil {
		t.Fatal(err)
	}
	ret_msg, err := commtypes.MsgSerToMsgG(&ret_msgSer, msgSerde.GetKeySerdeG(), msgSerde.GetValSerdeG())
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(msg, ret_msg) {
		t.Fatalf("expected: %v, got %v\n", msg, ret_msg)
	}
	*b_payload = payloads
	commtypes.PushBuffer(b_payload)
	*b = bytes
	commtypes.PushBuffer(b)
}
