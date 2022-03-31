package txn_data

const (
	LogTagReserveBits = 3

	StreamLogTagLowBits     = 1
	StreamPushLogTagLowBits = 2

	PartitionBits = 8

	TransactionLogBegin = iota
	TransactionLogPreCommit
	TransactionLogPreAbort
	TransactionLogCompleteCommit
	TransactionLogCompleteAbort
	TransactionLogFence
	TxnMarkLowBits
)
