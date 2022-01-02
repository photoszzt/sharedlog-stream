package commtypes

import "sync"

type StreamTimeExtractor interface {
	ExtractStreamTime() (int64, error)
}

type StreamTimeTracker interface {
	UpdateStreamTime(m *Message)
	GetStreamTime() int64
}

type streamTimeTracker struct {
	lock      sync.RWMutex
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
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.timeStamp
}
