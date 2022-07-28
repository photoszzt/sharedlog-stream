//go:generate msgp
//msgp:ignore TopicPartitionJSONSerde TopicPartitionMsgpSerde
package txn_data

type TopicPartition struct {
	Topic  string  `json:"topic" msg:"topic"`
	ParNum []uint8 `json:"parnum" msg:"parnum"`
}
