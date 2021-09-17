package spike_detection

import (
	"sharedlog-stream/pkg/stream/processor"

	"github.com/gammazero/deque"
)

type movingAverageAggregate struct {
	movingAverageWindow uint32
	deviceIDStreamMap   map[string]*deque.Deque
	deviceIDSumOfEvents map[string]float64
}

func NewMovingAverageMapper() processor.Mapper {
	return &movingAverageAggregate{
		movingAverageWindow: 1000,
		deviceIDStreamMap:   make(map[string]*deque.Deque),
		deviceIDSumOfEvents: make(map[string]float64),
	}
}

func (p *movingAverageAggregate) Map(msg processor.Message) (processor.Message, error) {
	devId := msg.Key.(string)
	nextVal := msg.Value.(float64)
	avg := p.movingAverage(devId, nextVal)
	return processor.Message{Key: msg.Key,
		Value:     ValAndAvg{Val: nextVal, Avg: avg},
		Timestamp: msg.Timestamp}, nil
}

func (p *movingAverageAggregate) movingAverage(devId string, nextVal float64) float64 {
	sum := 0.0
	vallist, ok := p.deviceIDStreamMap[devId]
	if ok {
		sum = p.deviceIDSumOfEvents[devId]
		if vallist.Len() > int(p.movingAverageWindow)-1 {
			valToRemove := vallist.PopFront().(float64)
			sum -= valToRemove
		}
		if vallist == nil {
			vallist = deque.New(int(p.movingAverageWindow))
		}
		vallist.PushBack(nextVal)
		sum += nextVal
		p.deviceIDSumOfEvents[devId] = sum
		p.deviceIDStreamMap[devId] = vallist
		return sum / float64(vallist.Len())
	}
	vallist = deque.New(int(p.movingAverageWindow))
	vallist.PushBack(nextVal)
	p.deviceIDStreamMap[devId] = vallist
	p.deviceIDSumOfEvents[devId] = nextVal
	return nextVal
}
