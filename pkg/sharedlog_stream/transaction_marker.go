//go:generate msgp
//msgp:ignore TxnMarkerJSONSerde TxnMarkerMsgpSerde
package sharedlog_stream

import "encoding/json"

type TxnMark uint8

const (
	COMMIT TxnMark = 0
	ABORT  TxnMark = 1
)

type TxnMarker struct {
	Mark     uint8  `json:"mk" msg:"mk"`
	AppEpoch uint16 `json:"ae" msg:"ae"`
	AppId    uint64 `json:"aid" msg:"aid"`
}

type TxnMarkerJSONSerde struct{}

func (s TxnMarkerJSONSerde) Encode(value interface{}) ([]byte, error) {
	tm := value.(*TxnMarker)
	return json.Marshal(tm)
}

func (s TxnMarkerJSONSerde) Decode(value []byte) (interface{}, error) {
	tm := TxnMarker{}
	if err := json.Unmarshal(value, &tm); err != nil {
		return nil, err
	}
	return tm, nil
}

type TxnMarkerMsgpSerde struct{}

func (s TxnMarkerMsgpSerde) Encode(value interface{}) ([]byte, error) {
	tm := value.(*TxnMarker)
	return tm.MarshalMsg(nil)
}

func (s TxnMarkerMsgpSerde) Decode(value []byte) (interface{}, error) {
	tm := TxnMarker{}
	if _, err := tm.UnmarshalMsg(value); err != nil {
		return nil, err
	}
	return tm, nil
}
