package processor

import (
	"context"
	"math"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/store"
	"time"

	"github.com/rs/zerolog/log"
)

type TimeTracker struct {
	emitIntervalMs uint64
	streamTime     int64
	minTime        int64
	nextTimeToEmit uint64
}

func NewTimeTracker() *TimeTracker {
	return &TimeTracker{
		emitIntervalMs: 1000,
		streamTime:     0,
		minTime:        math.MaxInt64,
	}
}

func (t *TimeTracker) SetEmitInterval(emitIntervalMs uint64) {
	t.emitIntervalMs = emitIntervalMs
}

func (t *TimeTracker) AdvanceStreamTime(recordTs int64) {
	if recordTs > t.streamTime {
		t.streamTime = recordTs
	}
}

func (t *TimeTracker) UpdatedMinTime(recordTs int64) {
	if recordTs < t.minTime {
		t.minTime = recordTs
	}
}

func (t *TimeTracker) AdvanceNextTimeToEmit() {
	t.nextTimeToEmit += t.emitIntervalMs
}

type StreamStreamJoinProcessor struct {
	otherWindowStore  store.WindowStore
	joiner            ValueJoinerWithKeyTs
	sharedTimeTracker *TimeTracker
	name              string
	joinAfterMs       int64
	joinGraceMs       int64
	joinBeforeMs      int64
	outer             bool
	isLeftSide        bool
}

var _ = Processor(&StreamStreamJoinProcessor{})

func NewStreamStreamJoinProcessor(
	name string,
	otherWindowStore store.WindowStore,
	jw *JoinWindows,
	joiner ValueJoinerWithKeyTs,
	outer bool,
	isLeftSide bool,
	stk *TimeTracker,
) *StreamStreamJoinProcessor {
	ssjp := &StreamStreamJoinProcessor{
		isLeftSide:        isLeftSide,
		outer:             outer,
		otherWindowStore:  otherWindowStore,
		joiner:            joiner,
		joinGraceMs:       jw.GracePeriodMs(),
		sharedTimeTracker: stk,
		name:              name,
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

func (p *StreamStreamJoinProcessor) Name() string {
	return p.name
}

func (p *StreamStreamJoinProcessor) ProcessAndReturn(ctx context.Context, msg commtypes.Message) ([]commtypes.Message, error) {
	if msg.Key == nil || msg.Value == nil {
		log.Warn().Msgf("skipping record due to null key or value. key=%v, val=%v", msg.Key, msg.Value)
		return nil, nil
	}

	// needOuterJoin := p.outer
	inputTs := msg.Timestamp
	// debug.Fprintf(os.Stderr, "input ts: %v\n", inputTs)
	var timeFrom uint64
	var timeTo uint64
	timeFromTmp := int64(inputTs) - int64(p.joinBeforeMs)
	if timeFromTmp < 0 {
		timeFrom = 0
	} else {
		timeFrom = uint64(timeFromTmp)
	}
	timeToTmp := inputTs + int64(p.joinAfterMs)
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
	msgs := make([]commtypes.Message, 0)
	err := p.otherWindowStore.Fetch(ctx, msg.Key,
		time.Unix(int64(timeFromSec), int64(timeFromNs)),
		time.Unix(int64(timeToSec), int64(timeToNs)), func(otherRecordTs int64, kt commtypes.KeyT, vt commtypes.ValueT) error {
			var newTs int64
			// needOuterJoin = false
			newVal := p.joiner.Apply(kt, msg.Value, vt, msg.Timestamp, otherRecordTs)
			if inputTs > otherRecordTs {
				newTs = inputTs
			} else {
				newTs = otherRecordTs
			}
			msgs = append(msgs, commtypes.Message{Key: msg.Key, Value: newVal, Timestamp: newTs})
			return nil
		})
	return msgs, err
	/*
		if needOuterJoin {
			// TODO
		}
	*/
}
