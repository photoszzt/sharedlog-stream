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
	pipe             Pipe
	pctx             ProcessorContext
	otherWindowStore WindowStore

	otherWindowName   string
	joinBeforeMs      uint64
	joinAfterMs       uint64
	joinGraceMs       uint64
	joiner            ValueJoinerWithKey
	outer             bool
	isLeftSide        bool
	sharedTimeTracker *TimeTracker
}

var _ = Processor(&StreamStreamJoinProcessor{})

func NewStreamStreamJoinProcessor(otherWindowName string, jw *JoinWindows, joiner ValueJoinerWithKey,
	outer bool, isLeftSide bool, stk *TimeTracker) *StreamStreamJoinProcessor {
	ssjp := &StreamStreamJoinProcessor{
		isLeftSide:        isLeftSide,
		outer:             outer,
		otherWindowName:   otherWindowName,
		joiner:            joiner,
		joinGraceMs:       jw.GracePeriodMs(),
		sharedTimeTracker: stk,
	}
	if isLeftSide {
		ssjp.joinBeforeMs = jw.beforeMs
		ssjp.joinAfterMs = jw.afterMs
	} else {
		ssjp.joinBeforeMs = jw.afterMs
		ssjp.joinAfterMs = jw.beforeMs
	}
	return ssjp
}

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

	// needOuterJoin := p.outer
	inputTs := msg.Timestamp
	var timeFrom uint64
	var timeTo uint64
	timeFromTmp := int64(inputTs) - int64(p.joinBeforeMs)
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
	p.otherWindowStore.Fetch(msg.Key,
		time.Unix(int64(timeFromSec), int64(timeFromNs)),
		time.Unix(int64(timeToSec), int64(timeToNs)), func(otherRecordTs uint64, vt ValueT) {
			var newTs uint64
			// needOuterJoin = false
			newVal := p.joiner.Apply(msg.Key, msg.Value, vt)
			if inputTs > otherRecordTs {
				newTs = inputTs
			} else {
				newTs = otherRecordTs
			}
			p.pipe.Forward(Message{Key: msg.Key, Value: newVal, Timestamp: newTs})
		})
	/*
		if needOuterJoin {
			// TODO
		}
	*/
	return nil
}

func (p *StreamStreamJoinProcessor) ProcessAndReturn(msg Message) ([]Message, error) {
	panic("not implemented")
}
