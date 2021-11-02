package benchmarkframework

import (
	"sharedlog-stream/benchmark/openmessage/benchmark_framework/utils/distributor"

	"golang.org/x/xerrors"
)

var (
	ErrNeedSubCon  = xerrors.New("Consumer only tests need subscriptions/consumers")
	ErrNeedBacklog = xerrors.New("Consumer only tests need a backlog specification")
	ErrProdRate    = xerrors.New("Producer rate should be > 0")
)

type Workload struct {
	Name                    string
	Topics                  uint32
	PartitionsPerTopic      uint32
	KeyDistributorType      distributor.KeyDistributorType
	MessageSize             uint32
	PayloadFile             string
	SubscriptionsPerTopic   uint32
	ProducersPerTopic       uint32
	ConsumerPerSubscription uint32
	ProducerRate            uint32
	ConsumerBacklogSizeGB   uint64
	TestDurationMinutes     uint32
	ConsumerOnly            bool
}

func NewWorkload() *Workload {
	return &Workload{
		KeyDistributorType:    distributor.NO_KEY,
		ConsumerBacklogSizeGB: 0,
	}
}

func (w *Workload) Validate() error {
	if w.ConsumerOnly && (w.SubscriptionsPerTopic == 0 || w.ConsumerPerSubscription == 0) {
		return ErrNeedSubCon
	}
	if w.ConsumerOnly && (w.ConsumerBacklogSizeGB <= 0) {
		return ErrNeedBacklog
	}

	if w.ProducerRate <= 0 {
		return ErrProdRate
	}
	return nil
}
