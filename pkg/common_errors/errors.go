package common_errors

import (
	"golang.org/x/xerrors"
)

var (
	ErrEmptyPayload             = xerrors.New("payload cannot be empty")
	ErrStreamEmpty              = xerrors.New("stream empty")
	ErrStreamTimeout            = xerrors.New("blocking pop timeout")
	ErrInvalidStateTransition   = xerrors.New("invalid state transition")
	ErrStreamSourceTimeout      = xerrors.New("SharedLogStreamSource consume timeout")
	ErrShouldExitForScale       = xerrors.New("should exit")
	ErrUnrecognizedSerdeFormat  = xerrors.New("Unrecognized serde format")
	ErrFailToGetOutputSubstream = xerrors.New("Fail to get output substream")
	ErrReturnDueToTest          = xerrors.New("return due to test")
)

func IsStreamEmptyError(err error) bool {
	return err == ErrStreamEmpty
}

func IsStreamTimeoutError(err error) bool {
	return err == ErrStreamTimeout
}
