package sharedlog_stream

type Encoder interface {
	Encode(interface{}) ([]byte, error)
}

type EncoderFunc func(interface{}) ([]byte, error)

func (f EncoderFunc) Encode(value interface{}) ([]byte, error) {
	return f(value)
}

type Decoder interface {
	Decode([]byte) (interface{}, error)
}

type DecoderFunc func([]byte) (interface{}, error)

func (f DecoderFunc) Decode(value []byte) (interface{}, error) {
	return f(value)
}
