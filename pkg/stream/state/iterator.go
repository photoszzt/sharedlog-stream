package state

type Iterator interface {
	HasNext() bool
	Next() interface{}
	Close()
	PeekNextKey() interface{}
}
