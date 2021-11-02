package sharedlog_stream

import (
	"hash"
	"hash/fnv"
	"sharedlog-stream/pkg/stream/processor"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/stream/processor/store"
)

type SharedLogStreamSink struct {
	pipe         processor.Pipe
	pctx         store.ProcessorContext
	stream       *SharedLogStream
	keyEncoder   commtypes.Encoder
	valueEncoder commtypes.Encoder
	msgEncoder   commtypes.MsgEncoder
	hasher       hash.Hash64
}

type StreamSinkConfig struct {
	KeyEncoder   commtypes.Encoder
	ValueEncoder commtypes.Encoder
	MsgEncoder   commtypes.MsgEncoder
}

func NewSharedLogStreamSink(stream *SharedLogStream, config *StreamSinkConfig) *SharedLogStreamSink {
	return &SharedLogStreamSink{
		stream:       stream,
		keyEncoder:   config.KeyEncoder,
		valueEncoder: config.ValueEncoder,
		msgEncoder:   config.MsgEncoder,
		hasher:       fnv.New64(),
	}
}

var _ = processor.Processor(&SharedLogStreamSink{})

func (sls *SharedLogStreamSink) WithPipe(pipe processor.Pipe) {
	sls.pipe = pipe
}

func (sls *SharedLogStreamSink) WithProcessorContext(pctx store.ProcessorContext) {
	sls.pctx = pctx
}

func (sls *SharedLogStreamSink) Process(msg commtypes.Message) error {
	if msg.Key == nil && msg.Value == nil {
		return nil
	}
	var additionalTag []uint64
	var keyEncoded []byte
	if msg.Key != nil {
		keyEncodedTmp, err := sls.keyEncoder.Encode(msg.Key)
		if err != nil {
			return err
		}
		keyEncoded = keyEncodedTmp
		keyTag := sls.hasher.Sum64()
		additionalTag = []uint64{keyTag}
	}
	valEncoded, err := sls.valueEncoder.Encode(msg.Value)
	if err != nil {
		return err
	}
	// fmt.Fprintf(os.Stderr, "Sink: output key: %v, val: %v\n", string(keyEncoded), string(valEncoded))
	bytes, err := sls.msgEncoder.Encode(keyEncoded, valEncoded)
	if bytes != nil && err == nil {
		_, err = sls.stream.Push(bytes, 0, additionalTag)
		if err != nil {
			return err
		}
	}
	return err
}

func (sls *SharedLogStreamSink) ProcessAndReturn(msg commtypes.Message) ([]commtypes.Message, error) {
	panic("not implemented")
}
