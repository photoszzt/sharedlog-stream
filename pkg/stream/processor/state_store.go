package processor

type StateStore interface {
	Name() string
}

type MaterializeParam struct {
	KeySerde   Serde
	ValueSerde Serde
	StoreName  string
}
