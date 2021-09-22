package spike_detection

import (
	"sharedlog-stream/pkg/stream/processor"
	"testing"
)

func TestMsgEncodeAndDecode(t *testing.T) {
	v := SensorData{
		Val:       2.0,
		Timestamp: 1,
	}
	k := "1"
	k_serde := processor.StringSerde{}
	v_msgSerde := SensorDataMsgpSerde{}
	v_encoded, err := v_msgSerde.Encode(&v)
	if err != nil {
		t.Fatal(err)
	}
	k_encoded, err := k_serde.Encode(k)
	if err != nil {
		t.Fatal(err)
	}

	msgMsgpSerde := processor.MessageSerializedMsgpSerde{}
	msg_encoded, err := msgMsgpSerde.Encode(k_encoded, v_encoded)
	if err != nil {
		t.Fatal(err)
	}

	k_bytes, v_bytes, err := msgMsgpSerde.Decode(msg_encoded)
	if err != nil {
		t.Fatal(err)
	}
	k_decode, err := k_serde.Decode(k_bytes)
	if err != nil {
		t.Fatal(err)
	}
	if k_decode != k {
		t.Errorf("key decoded: %v and original key: %v are different", k_decode, k)
	}

	v_decode, err := v_msgSerde.Decode(v_bytes)
	if err != nil {
		t.Fatal(err)
	}
	if v_decode != v {
		t.Errorf("val decoded: %v and original val: %v are different", v_decode, v)
	}
}

func TestMsgJSONEncodeAndDecode(t *testing.T) {
	v := SensorData{
		Val:       2.0,
		Timestamp: 1,
	}
	k := "1"
	k_serde := processor.StringSerde{}
	v_jsonSerde := SensorDataJSONSerde{}
	v_encoded, err := v_jsonSerde.Encode(&v)
	if err != nil {
		t.Fatal(err)
	}
	k_encoded, err := k_serde.Encode(k)
	if err != nil {
		t.Fatal(err)
	}

	msgJSONSerde := processor.MessageSerializedJSONSerde{}
	msg_encoded, err := msgJSONSerde.Encode(k_encoded, v_encoded)
	if err != nil {
		t.Fatal(err)
	}

	k_bytes, v_bytes, err := msgJSONSerde.Decode(msg_encoded)
	if err != nil {
		t.Fatal(err)
	}
	k_decode, err := k_serde.Decode(k_bytes)
	if err != nil {
		t.Fatal(err)
	}
	if k_decode != k {
		t.Errorf("key decoded: %v and original key: %v are different", k_decode, k)
	}

	v_decode, err := v_jsonSerde.Decode(v_bytes)
	if err != nil {
		t.Fatal(err)
	}
	if v_decode != v {
		t.Errorf("val decoded: %v and original val: %v are different", v_decode, v)
	}
}
