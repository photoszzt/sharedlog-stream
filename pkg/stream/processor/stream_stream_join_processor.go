package processor

import (
	"math"
	"time"

	"github.com/rs/zerolog/log"
)

type TimeTracker struct {
	emitIntervalMs uint64
	streamTime     uint64
	minTime        uint64
	nextTimeToEmit uint64
}

func NewTimeTracker() *TimeTracker {
	return &TimeTracker{
		emitIntervalMs: 1000,
		streamTime:     0,
		minTime:        math.MaxUint64,
	}
}

func (t *TimeTracker) SetEmitInterval(emitIntervalMs uint64) {
	t.emitIntervalMs = emitIntervalMs
}

func (t *TimeTracker) AdvanceStreamTime(recordTs uint64) {
	if recordTs > t.streamTime {
		t.streamTime = recordTs
	}
}

func (t *TimeTracker) UpdatedMinTime(recordTs uint64) {
	if recordTs < t.minTime {
		t.minTime = recordTs
	}
}

func (t *TimeTracker) AdvanceNextTimeToEmit() {
	t.nextTimeToEmit += t.emitIntervalMs
}

type StreamStreamJoinProcessor struct {
	pipe              Pipe
	otherWindowStore  WindowStore
	pctx              ProcessorContext
	otherWindowName   string
	joinBeforeMs      uint64
	joinAfterMs       uint64
	joinGraceMs       uint64
	joiner            ValueJoinerWithKey
	outer             bool
	isLeftSide        bool
	sharedTimeTracker TimeTracker
}

var _ = Processor(&StreamStreamJoinProcessor{})

func NewStreamStreamJoinProcessor() {}

func (p *StreamStreamJoinProcessor) WithPipe(pipe Pipe) {
	p.pipe = pipe
}

func (p *StreamStreamJoinProcessor) WithProcessorContext(pctx ProcessorContext) {
	p.pctx = pctx
}

func (p *StreamStreamJoinProcessor) Process(msg Message) error {
	if msg.Key == nil || msg.Value == nil {
		log.Warn().Msgf("skipping record due to null key or value. key=%v, val=%v", msg.Key, msg.Value)
		return nil
	}

	needOuterJoin := p.outer
	inputTs := msg.Timestamp
	var timeFrom uint64
	var timeTo uint64
	timeFromTmp := inputTs - p.joinBeforeMs
	if timeFromTmp < 0 {
		timeFrom = 0
	} else {
		timeFrom = uint64(timeFromTmp)
	}
	timeToTmp := inputTs + p.joinAfterMs
	if timeToTmp > 0 {
		timeTo = uint64(timeToTmp)
	} else {
		timeTo = 0
	}
	p.sharedTimeTracker.AdvanceStreamTime(inputTs)

	// TODO: emit all non-joined records which window has closed
	timeFromSec := timeFrom / 1000
	timeFromNs := (timeFrom - timeFromSec*1000) * 1000000

	timeToSec := timeTo / 1000
	timeToNs := (timeTo - timeToSec*1000) * 1000000
	iter := p.otherWindowStore.Fetch(msg.Key,
		time.Unix(int64(timeFromSec), int64(timeFromNs)),
		time.Unix(int64(timeToSec), int64(timeToNs)))
	for iter.HasNext() {
		var newTs uint64
		needOuterJoin = false
		otherRecord := iter.Next()
		otherRecordTs := otherRecord.Key.(uint64)
		newVal := p.joiner.Apply(msg.Key, msg.Value, otherRecord.Value)
		if inputTs > otherRecordTs {
			newTs = inputTs
		} else {
			newTs = otherRecordTs
		}
		p.pipe.Forward(Message{Key: msg.Key, Value: newVal, Timestamp: newTs})
	}

	if needOuterJoin {
		// TODO
	}
	return nil
}
