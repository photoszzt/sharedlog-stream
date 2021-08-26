package sharedlog_stream

import (
	"errors"
	"time"

	"cs.utexas.edu/zhitingz/sharedlog-stream/pkg/stream"
)

var (
	errSharedLogStreamSourceTimeout = errors.New("SharedLogStreamSource consume timeout.")
)

type SharedLogStreamSource struct {
	stream  *SharedLogStream
	timeout time.Duration
}

func NewSharedLogStreamSource(stream *SharedLogStream, timeout int) *SharedLogStreamSource {
	return &SharedLogStreamSource{
		stream:  stream,
		timeout: time.Duration(timeout) * time.Second,
	}
}

func (s *SharedLogStreamSource) Consume() (stream.Message, error) {
	startTime := time.Now()
	for {
		if s.timeout != 0 && time.Since(startTime) >= s.timeout {
			break
		}
		val, err := s.stream.Pop()
		if err != nil {
			if IsStreamEmptyError(err) {
				time.Sleep(time.Duration(100) * time.Microsecond)
				// fmt.Println("No stream")
				continue
			} else if IsStreamTimeoutError(err) {
				// fmt.Println("pop timeout")
				continue
			} else {
				return stream.EmptyMessage, err
			}
		}
		return stream.Message{Key: nil, Value: val}, nil
	}
	return stream.EmptyMessage, errSharedLogStreamSourceTimeout
}
