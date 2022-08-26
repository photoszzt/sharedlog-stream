package processor

import (
	"context"
	"fmt"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/utils"
)

type TableGroupByMapProcessor struct {
	mapper Mapper
	name   string
	BaseProcessor
}

var _ = Processor(&TableGroupByMapProcessor{})

func NewTableGroupByMapProcessor(name string, mapper Mapper) *TableGroupByMapProcessor {
	p := &TableGroupByMapProcessor{
		mapper: mapper,
		name:   name,
	}
	p.BaseProcessor.ProcessingFunc = p.ProcessAndReturn
	return p
}

func (p *TableGroupByMapProcessor) Name() string {
	return p.name
}

func (p *TableGroupByMapProcessor) ProcessAndReturn(ctx context.Context, msg commtypes.Message) ([]commtypes.Message, error) {
	if msg.Key == nil {
		return nil, fmt.Errorf("msg key for the grouping table should not be nil")
	}
	change, ok := msg.Value.(*commtypes.Change)
	if !ok {
		changeTmp := msg.Value.(commtypes.Change)
		change = &changeTmp
	}
	var newK, newV interface{}
	var err error
	if utils.IsNil(change.NewVal) {
		newK = nil
		newV = nil
	} else {
		newK, newV, err = p.mapper.Map(msg.Key, change.NewVal)
		if err != nil {
			return nil, err
		}
	}
	var oldK, oldV interface{}
	if utils.IsNil(change.OldVal) {
		oldK = nil
		oldV = nil
	} else {
		oldK, oldV, err = p.mapper.Map(msg.Key, change.OldVal)
		if err != nil {
			return nil, err
		}
	}
	var outMsgs []commtypes.Message
	if !utils.IsNil(oldK) && !utils.IsNil(oldV) {
		outMsgs = append(outMsgs, commtypes.Message{
			Key:       oldK,
			Value:     commtypes.Change{NewVal: nil, OldVal: oldV},
			Timestamp: msg.Timestamp,
		})
	}
	if !utils.IsNil(newK) && !utils.IsNil(newV) {
		outMsgs = append(outMsgs, commtypes.Message{
			Key:       newK,
			Value:     commtypes.Change{NewVal: newV, OldVal: nil},
			Timestamp: msg.Timestamp,
		})
	}
	return outMsgs, nil
}

type TableGroupByMapProcessorG[K, V, KR, VR any] struct {
	mapper MapperG[K, V, KR, VR]
	name   string
	BaseProcessor
}

var _ = Processor(&TableGroupByMapProcessorG[int, string, int, string]{})

func NewTableGroupByMapProcessorG[K, V, KR, VR any](name string, mapper MapperG[K, V, KR, VR]) *TableGroupByMapProcessorG[K, V, KR, VR] {
	p := &TableGroupByMapProcessorG[K, V, KR, VR]{
		mapper:        mapper,
		name:          name,
		BaseProcessor: BaseProcessor{},
	}
	p.BaseProcessor.ProcessingFunc = p.ProcessAndReturn
	return p
}

func (p *TableGroupByMapProcessorG[K, V, KR, VR]) Name() string {
	return p.name
}

func (p *TableGroupByMapProcessorG[K, V, KR, VR]) ProcessAndReturn(ctx context.Context, msg commtypes.Message) ([]commtypes.Message, error) {
	if msg.Key == nil {
		return nil, fmt.Errorf("msg key for the grouping table should not be nil")
	}
	change, ok := msg.Value.(*commtypes.Change)
	if !ok {
		changeTmp := msg.Value.(commtypes.Change)
		change = &changeTmp
	}
	var newK, newV interface{}
	var err error
	if utils.IsNil(change.NewVal) {
		newK = nil
		newV = nil
	} else {
		newK, newV, err = p.mapper.Map(msg.Key.(K), change.NewVal.(V))
		if err != nil {
			return nil, err
		}
	}
	var oldK, oldV interface{}
	if utils.IsNil(change.OldVal) {
		oldK = nil
		oldV = nil
	} else {
		oldK, oldV, err = p.mapper.Map(msg.Key.(K), change.OldVal.(V))
		if err != nil {
			return nil, err
		}
	}
	var outMsgs []commtypes.Message
	if !utils.IsNil(oldK) && !utils.IsNil(oldV) {
		outMsgs = append(outMsgs, commtypes.Message{
			Key:       oldK,
			Value:     commtypes.Change{NewVal: nil, OldVal: oldV},
			Timestamp: msg.Timestamp,
		})
	}
	if !utils.IsNil(newK) && !utils.IsNil(newV) {
		outMsgs = append(outMsgs, commtypes.Message{
			Key:       newK,
			Value:     commtypes.Change{NewVal: newV, OldVal: nil},
			Timestamp: msg.Timestamp,
		})
	}
	return outMsgs, nil
}
