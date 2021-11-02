package commands

import "sharedlog-stream/benchmark/openmessage/benchmark_framework/utils/distributor"

type ProducerWorkAssignment struct {
	PayloadData        []byte
	PublishRate        float64
	KeyDistributorType distributor.KeyDistributorType
}
