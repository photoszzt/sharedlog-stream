package commtypes

import "sync"

type EventTimeExtractor interface {
	ExtractEventTime() (int64, error)
}

type EventTimeSetter interface {
	UpdateEventTime(ts int64)
}

type InjectTimeGetter interface {
	ExtractInjectTimeMs() int64
}

type InjectTimeSetter interface {
	UpdateInjectTime(ts int64)
}

type StreamTimeTracker interface {
	UpdateStreamTime(m *Message)
	GetStreamTime() int64
}

type streamTimeTracker struct {
	lock      sync.Mutex
	timeStamp int64
}

func NewStreamTimeTracker() StreamTimeTracker {
	return &streamTimeTracker{
		timeStamp: 0,
	}
}

// Update the timestamp when the stream
func (s *streamTimeTracker) UpdateStreamTime(m *Message) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if m.Timestamp > s.timeStamp {
		s.timeStamp = m.Timestamp
	}
}

func (s *streamTimeTracker) GetStreamTime() int64 {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.timeStamp
}
