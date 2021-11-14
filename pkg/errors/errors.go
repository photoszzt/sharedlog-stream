package errors

import "errors"

var (
	ErrEmptyPayload  = errors.New("payload cannot be empty")
	ErrStreamEmpty   = errors.New("stream empty")
	ErrStreamTimeout = errors.New("blocking pop timeout")
)

func IsStreamEmptyError(err error) bool {
	return err == ErrStreamEmpty
}

func IsStreamTimeoutError(err error) bool {
	return err == ErrStreamTimeout
}
