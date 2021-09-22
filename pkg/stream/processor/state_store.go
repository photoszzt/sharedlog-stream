package processor

type StateStore interface {
	Name() string
}

type MaterializeParam struct {
	KeySerde   Serde
	ValueSerde Serde
	MsgSerde   MsgSerde
	StoreName  string
	Changelog  LogStore
}
