package sharedlog_stream

const (
	LogTagReserveBits = 3

	StreamLogTagLowBits     = 1
	streamPushLogTagLowBits = 2

	PartitionBits = 8

	TransactionLogBegin = iota
	TransactionLogPreCommit
	TransactionLogPreAbort
	TransactionLogCompleteCommit
	TransactionLogCompleteAbort
	TransactionLogFence
	TxnMarkLowBits
)
