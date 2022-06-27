package store

type StateStore interface {
	Name() string
	IsOpen() bool
}
