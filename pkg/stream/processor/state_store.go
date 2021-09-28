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

type JoinParam struct {
	KeySerde             Serde
	ValueSerde           Serde
	OtherValueSerde      Serde
	MsgSerde             MsgSerde
	LeftWindowStoreName  string
	RightWindowStoreName string
}
