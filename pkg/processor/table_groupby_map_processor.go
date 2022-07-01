package processor

import (
	"context"
	"fmt"
	"os"
	"sharedlog-stream/pkg/commtypes"
)

type TableGroupByMapProcessor struct {
	mapper Mapper
	name   string
}

var _ = Processor(&TableGroupByMapProcessor{})

func NewTableGroupByMapProcessor(name string, mapper Mapper) *TableGroupByMapProcessor {
	return &TableGroupByMapProcessor{
		mapper: mapper,
		name:   name,
	}
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
	if change.NewVal == nil {
		newK = nil
		newV = nil
	} else {
		newK, newV, err = p.mapper.Map(msg.Key, change.NewVal)
		if err != nil {
			return nil, err
		}
	}
	var oldK, oldV interface{}
	if change.OldVal == nil {
		oldK = nil
		oldV = nil
	} else {
		oldK, oldV, err = p.mapper.Map(msg.Key, change.OldVal)
		if err != nil {
			return nil, err
		}
	}
	var outMsgs []commtypes.Message
	if oldK != nil && oldV != nil {
		outMsgs = append(outMsgs, commtypes.Message{
			Key:       oldK,
			Value:     commtypes.Change{NewVal: nil, OldVal: oldV},
			Timestamp: msg.Timestamp,
		})
	}
	if newK != nil && newV != nil {
		outMsgs = append(outMsgs, commtypes.Message{
			Key:       newK,
			Value:     commtypes.Change{NewVal: newV, OldVal: nil},
			Timestamp: msg.Timestamp,
		})
	}
	fmt.Fprintf(os.Stderr, "tableGroupByMap output: %v\n", outMsgs)
	return outMsgs, nil
}
