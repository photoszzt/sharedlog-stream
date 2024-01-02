package producer_consumer

import (
	"context"
	"fmt"
	"os"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/data_structure"
	"sharedlog-stream/pkg/sharedlog_stream"
)

type NoDupConsumer struct {
	stream           *sharedlog_stream.ShardedSharedLogStream
	curReadMsgSeqNum map[commtypes.ProducerId]data_structure.Uint64Set
	epochMarkerSerde commtypes.SerdeG[commtypes.EpochMarker]
}

func NewNoDupConsumer(
	stream *sharedlog_stream.ShardedSharedLogStream,
	epochMarkerSerde commtypes.SerdeG[commtypes.EpochMarker],
) *NoDupConsumer {
	return &NoDupConsumer{
		stream:           stream,
		curReadMsgSeqNum: make(map[commtypes.ProducerId]data_structure.Uint64Set),
		epochMarkerSerde: epochMarkerSerde,
	}
}
func (ndc *NoDupConsumer) OutputRemainingStats() {
}

func (ndc *NoDupConsumer) ReadNext(ctx context.Context, parNum uint8) (*commtypes.RawMsg, error) {
	for {
		rawMsg, err := ndc.stream.ReadNext(ctx, parNum)
		if err != nil {
			return nil, err
		}
		if rawMsg.IsControl {
			epochMark, err := ndc.epochMarkerSerde.Decode(rawMsg.Payload)
			if err != nil {
				return nil, err
			}
			// debug.Fprintf(os.Stderr, "%+v\n", epochMark)
			rawMsg.Mark = epochMark.Mark
			rawMsg.ProdIdx = epochMark.ProdIndex
			if epochMark.Mark == commtypes.SCALE_FENCE {
				rawMsg.ScaleEpoch = epochMark.ScaleEpoch
			} else if epochMark.Mark == commtypes.STREAM_END {
				rawMsg.StartTime = epochMark.StartTime
			}
			return rawMsg, err
		} else {
			if shouldIgnoreThisMsg(ndc.curReadMsgSeqNum, rawMsg) {
				fmt.Fprintf(os.Stderr, "got a duplicate entry; continue\n")
				continue
			}
			return rawMsg, err
		}
	}
}
