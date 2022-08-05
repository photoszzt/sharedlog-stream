package processor

import (
	"context"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/store"

	"github.com/rs/zerolog/log"
)

type TableSourceProcessor struct {
	store store.CoreKeyValueStore
	name  string
}

var _ = Processor(&TableSourceProcessor{})

func NewTableSourceProcessor() *TableSourceProcessor {
	return &TableSourceProcessor{
		name: "toTable",
	}
}

func NewTableSourceProcessorWithTable(tab store.CoreKeyValueStore) *TableSourceProcessor {
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
			if oldValTs != nil {
				oldVal = oldValTs.Value
				if msg.Timestamp < oldValTs.Timestamp {
					log.Warn().Msgf("Detected out-of-order table update for %s, old Ts=[%v] new Ts=[%v].",
						p.store.Name(), oldValTs.Timestamp, msg.Timestamp)
				}
			}
		} else {
			oldVal = nil
		}
		err = p.store.Put(ctx, msg.Key, commtypes.CreateValueTimestamp(msg.Value, msg.Timestamp))
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

func MsgSerdeWithValueTsG[K any](serdeFormat commtypes.SerdeFormat, keySerde commtypes.SerdeG[K], valSerde commtypes.Serde) (commtypes.MessageSerdeG[K, *commtypes.ValueTimestamp], error) {
	valueTsSerde, err := commtypes.GetValueTsSerdeG(serdeFormat, valSerde)
	if err != nil {
		return nil, err
	}
	return commtypes.GetMsgSerdeG(serdeFormat, keySerde, valueTsSerde)
}

func ToInMemKVTable(storeName string, compare store.KVStoreLessFunc) (
	*MeteredProcessor, store.CoreKeyValueStore,
) {
	s := store.NewInMemoryKeyValueStore(storeName, compare)
	toTableProc := NewMeteredProcessor(NewTableSourceProcessorWithTable(s))
	return toTableProc, s
}

type TableSourceProcessorG[K, V any] struct {
	store store.CoreKeyValueStoreG[K, *commtypes.ValueTimestamp]
	name  string
}

var _ = Processor(&TableSourceProcessorG[int, int]{})

func NewTableSourceProcessorG[K, V any]() *TableSourceProcessorG[K, V] {
	return &TableSourceProcessorG[K, V]{
		name: "toTable",
	}
}

func NewTableSourceProcessorWithTableG[K, V any](tab store.CoreKeyValueStoreG[K, *commtypes.ValueTimestamp]) *TableSourceProcessorG[K, V] {
	return &TableSourceProcessorG[K, V]{
		name:  "toTable",
		store: tab,
	}
}

func (p *TableSourceProcessorG[K, V]) Name() string {
	return p.name
}

func (p *TableSourceProcessorG[K, V]) ProcessAndReturn(ctx context.Context, msg commtypes.Message) ([]commtypes.Message, error) {
	if msg.Key == nil {
		log.Warn().Msgf("Skipping record due to null key")
		return nil, nil
	}
	if p.store != nil {
		var oldVal interface{}
		key := msg.Key.(K)
		v, ok, err := p.store.Get(ctx, key)
		if err != nil {
			return nil, err
		}
		if ok {
			oldValTs := commtypes.CastToValTsPtr(v)
			if oldValTs != nil {
				oldVal = oldValTs.Value.(V)
				if msg.Timestamp < oldValTs.Timestamp {
					log.Warn().Msgf("Detected out-of-order table update for %s, old Ts=[%v] new Ts=[%v].",
						p.store.Name(), oldValTs.Timestamp, msg.Timestamp)
				}
			}
		} else {
			oldVal = nil
		}
		err = p.store.Put(ctx, key, commtypes.CreateValueTimestampOptional(msg.Value, msg.Timestamp))
		if err != nil {
			return nil, err
		}
		return []commtypes.Message{{Key: msg.Key, Value: commtypes.Change{NewVal: msg.Value, OldVal: oldVal}, Timestamp: msg.Timestamp}}, nil
	} else {
		return []commtypes.Message{{Key: msg.Key, Value: commtypes.Change{NewVal: msg.Value, OldVal: nil}, Timestamp: msg.Timestamp}}, nil
	}
}
