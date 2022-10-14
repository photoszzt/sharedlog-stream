package processor

import (
	"context"
	"fmt"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/optional"
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
	BaseProcessorG[K, commtypes.ChangeG[V], KR, commtypes.ChangeG[VR]]
}

var _ = ProcessorG[int, commtypes.ChangeG[string], int, commtypes.ChangeG[string]](&TableGroupByMapProcessorG[int, string, int, string]{})

func NewTableGroupByMapProcessorG[K, V, KR, VR any](name string, mapper MapperG[K, V, KR, VR]) ProcessorG[K, commtypes.ChangeG[V], KR, commtypes.ChangeG[VR]] {
	p := &TableGroupByMapProcessorG[K, V, KR, VR]{
		mapper:         mapper,
		name:           name,
		BaseProcessorG: BaseProcessorG[K, commtypes.ChangeG[V], KR, commtypes.ChangeG[VR]]{},
	}
	p.BaseProcessorG.ProcessingFuncG = p.ProcessAndReturn
	return p
}

func (p *TableGroupByMapProcessorG[K, V, KR, VR]) Name() string {
	return p.name
}

func (p *TableGroupByMapProcessorG[K, V, KR, VR]) ProcessAndReturn(ctx context.Context, msg commtypes.MessageG[K, commtypes.ChangeG[V]]) ([]commtypes.MessageG[KR, commtypes.ChangeG[VR]], error) {
	if msg.Key.IsNone() {
		return nil, fmt.Errorf("msg key for the grouping table should not be nil")
	}
	change := msg.Value.Unwrap()
	newKOp := optional.None[KR]()
	newVOp := optional.None[VR]()
	if change.NewVal.IsSome() {
		newK, newV, err := p.mapper.Map(msg.Key, change.NewVal)
		if err != nil {
			return nil, err
		}
		newKOp = optional.Some(newK)
		newVOp = optional.Some(newV)
	}
	oldValOp := optional.None[VR]()
	oldKeyOp := optional.None[KR]()
	if change.OldVal.IsSome() {
		oldK, oldV, err := p.mapper.Map(msg.Key, change.OldVal)
		if err != nil {
			return nil, err
		}
		oldValOp = optional.Some(oldV)
		oldKeyOp = optional.Some(oldK)
	}
	var outMsgs []commtypes.MessageG[KR, commtypes.ChangeG[VR]]
	if oldKeyOp.IsSome() && oldValOp.IsSome() {
		outMsgs = append(outMsgs, commtypes.MessageG[KR, commtypes.ChangeG[VR]]{
			Key:           oldKeyOp,
			Value:         optional.Some(commtypes.ChangeG[VR]{NewVal: optional.None[VR](), OldVal: oldValOp}),
			TimestampMs:   msg.TimestampMs,
			StartProcTime: msg.StartProcTime,
		})
	}
	if newKOp.IsSome() && newVOp.IsSome() {
		outMsgs = append(outMsgs, commtypes.MessageG[KR, commtypes.ChangeG[VR]]{
			Key:           newKOp,
			Value:         optional.Some(commtypes.ChangeG[VR]{NewVal: newVOp, OldVal: optional.None[VR]()}),
			TimestampMs:   msg.TimestampMs,
			StartProcTime: msg.StartProcTime,
		})
	}
	return outMsgs, nil
}
