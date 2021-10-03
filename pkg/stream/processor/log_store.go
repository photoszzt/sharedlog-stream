package processor

type LogStore interface {
	Push(payload []byte, parNum uint8) (uint64, error)
	Pop(parNum uint8) ([]byte /* payload */, error)
	// PopBlocking() ([]byte /* payload */, error)
	TopicName() string
}
