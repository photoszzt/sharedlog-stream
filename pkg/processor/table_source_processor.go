package processor

import (
	"context"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/store"
	"sharedlog-stream/pkg/treemap"

	"github.com/rs/zerolog/log"
)

type TableSourceProcessor struct {
	store store.KeyValueStore
	name  string
}

var _ = Processor(&TableSourceProcessor{})

func NewTableSourceProcessor() *TableSourceProcessor {
	return &TableSourceProcessor{
		name: "toTable",
	}
}

func NewTableSourceProcessorWithTable(tab store.KeyValueStore) *TableSourceProcessor {
	return &TableSourceProcessor{
		name:  "toTable",
		store: tab,
	}
}

func (p *TableSourceProcessor) Name() string {
	return p.name
}

func (p *TableSourceProcessor) ProcessAndReturn(ctx context.Context, msg commtypes.Message) ([]commtypes.Message, error) {
	if msg.Key == nil {
		log.Warn().Msgf("Skipping record due to null key")
		return nil, nil
	}
	if p.store != nil {
		var oldVal interface{}
		v, ok, err := p.store.Get(ctx, msg.Key)
		if err != nil {
			return nil, err
		}
		if ok {
			oldValTs := commtypes.CastToValTsPtr(v)
			oldVal = oldValTs.Value
			if msg.Timestamp < oldValTs.Timestamp {
				log.Warn().Msgf("Detected out-of-order table update for %s, old Ts=[%v] new Ts=[%v].",
					p.store.Name(), oldValTs.Timestamp, msg.Timestamp)
			}
		} else {
			oldVal = nil
		}
		err = p.store.Put(ctx, msg.Key, commtypes.ValueTimestamp{Value: msg.Value, Timestamp: msg.Timestamp})
		if err != nil {
			return nil, err
		}
		return []commtypes.Message{{Key: msg.Key, Value: commtypes.Change{NewVal: msg.Value, OldVal: oldVal}, Timestamp: msg.Timestamp}}, nil
	} else {
		return []commtypes.Message{{Key: msg.Key, Value: commtypes.Change{NewVal: msg.Value, OldVal: nil}, Timestamp: msg.Timestamp}}, nil
	}
}

func MsgSerdeWithValueTs(serdeFormat commtypes.SerdeFormat, keySerde commtypes.Serde, valSerde commtypes.Serde) (commtypes.MessageSerde, error) {
	valueTsSerde, err := commtypes.GetValueTsSerde(serdeFormat, valSerde)
	if err != nil {
		return nil, err
	}
	return commtypes.GetMsgSerde(serdeFormat, keySerde, valueTsSerde)
}

func ToInMemKVTable(storeName string, compare func(a, b treemap.Key) int) (
	*MeteredProcessor, store.KeyValueStore,
) {
	s := store.NewInMemoryKeyValueStore(storeName, compare)
	toTableProc := NewMeteredProcessor(NewTableSourceProcessorWithTable(s))
	return toTableProc, s
}
