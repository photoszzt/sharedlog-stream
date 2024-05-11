package ntypes

import (
	"reflect"
	"sharedlog-stream/pkg/commtypes"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

func GenTestEncodeDecodeEvent(v *Event, t *testing.T, serdeG commtypes.SerdeG[*Event], serde commtypes.Serde) {
	vl := reflect.ValueOf(v)
	var opts cmp.Options
	if vl.Comparable() {
		opts = append(opts,
			cmpopts.EquateComparable(),
		)
	}
	opts = append(opts, cmpopts.IgnoreUnexported(Event{}))

	bts, buf, err := serdeG.Encode(v)
	if err != nil {
		t.Fatal(err)
	}
	ret, err := serdeG.Decode(bts)
	if err != nil {
		t.Fatal(err)
	}
	if !cmp.Equal(v, ret, opts...) {
		t.Fatal("encode and decode doesn't give same value")
	}
	if serdeG.UsedBufferPool() {
		*buf = bts
		commtypes.PushBuffer(buf)
	}

	bts, buf, err = serde.Encode(v)
	if err != nil {
		t.Fatal(err)
	}
	r, err := serde.Decode(bts)
	if err != nil {
		t.Fatal(err)
	}
	if !cmp.Equal(v, r, opts...) {
		t.Fatal("encode and decode doesn't give same value")
	}
	if serde.UsedBufferPool() {
		*buf = bts
		commtypes.PushBuffer(buf)
	}
}

func TestSerdeEvent(t *testing.T) {
	v := &Event{}
	jsonSerdeG := EventJSONSerdeG{}
	jsonSerde := EventJSONSerde{}
	GenTestEncodeDecodeEvent(v, t, jsonSerdeG, jsonSerde)
	msgSerdeG := EventMsgpSerdeG{}
	msgSerde := EventMsgpSerde{}
	GenTestEncodeDecodeEvent(v, t, msgSerdeG, msgSerde)
}
