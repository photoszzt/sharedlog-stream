//go:generate msgp
package commtypes

import "sync"

type EventTimeExtractor interface {
	ExtractEventTime() (int64, error)
}

type InjectTimeGetterSetter interface {
	UpdateInjectTime(ts int64) error
	ExtractInjectTimeMs() (int64, error)
}

// For benchmark to see the duration from producer to consumer.
// Due to the limitation of msgp flatten, this type has to be defined
// in each package if a type needs to implement it.
type BaseInjTime struct {
	InjT int64 `msg:"injT" json:"injT"`
}

func (ij *BaseInjTime) UpdateInjectTime(ts int64) error {
	ij.InjT = ts
	return nil
}

func (ij *BaseInjTime) ExtractInjectTimeMs() (int64, error) {
	return ij.InjT, nil
}

type BaseTs struct {
	Timestamp int64 `msg:"ts,omitempty" json:"ts,omitempty"`
}

func (ts *BaseTs) ExtractEventTime() (int64, error) {
	return ts.Timestamp, nil
}

func UpdateValInjectTime(msg *Message, ts int64) error {
	v := msg.Value.(InjectTimeGetterSetter)
	return v.UpdateInjectTime(ts)
}

func ExtractValInjectTime(msg *Message) (int64, error) {
	v := msg.Value.(InjectTimeGetterSetter)
	return v.ExtractInjectTimeMs()
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
