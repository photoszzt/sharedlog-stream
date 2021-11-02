package driverinterfaces

type ConsumerCallback interface {
	MessageReceived(payload []byte, publishTimestamp uint64)
}
