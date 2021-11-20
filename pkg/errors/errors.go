package errors

import (
	"golang.org/x/xerrors"
)

var (
	ErrEmptyPayload           = xerrors.New("payload cannot be empty")
	ErrStreamEmpty            = xerrors.New("stream empty")
	ErrStreamTimeout          = xerrors.New("blocking pop timeout")
	ErrInvalidStateTransition = xerrors.New("invalid state transition")
)

func IsStreamEmptyError(err error) bool {
	return err == ErrStreamEmpty
}

func IsStreamTimeoutError(err error) bool {
	return err == ErrStreamTimeout
}
