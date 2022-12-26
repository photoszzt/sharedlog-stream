package handlers

import (
	"context"
	"fmt"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/common/benchutil"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/ntypes"
	"time"

	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/optional"
	"sharedlog-stream/pkg/producer_consumer"

	"cs.utexas.edu/zjia/faas/types"
)

func only_bid(key optional.Option[string], value optional.Option[*ntypes.Event]) (bool, error) {
	v := value.Unwrap()
	return v.Etype == ntypes.BID, nil
}

func getSrcSink(ctx context.Context, env types.Environment, sp *common.QueryInput,
) ([]*producer_consumer.MeteredConsumer, []producer_consumer.MeteredProducerIntr, error) {
	input_stream, output_streams, err := benchutil.GetShardedInputOutputStreams(ctx, env, sp)
	if err != nil {
		return nil, nil, err
	}
	debug.Assert(len(output_streams) == 1, "expected only one output stream")
	serdeFormat := commtypes.SerdeFormat(sp.SerdeFormat)
	inConfig := &producer_consumer.StreamConsumerConfig{
		Timeout:     common.SrcConsumeTimeout,
		SerdeFormat: serdeFormat,
	}
	outConfig := &producer_consumer.StreamSinkConfig{
		FlushDuration: time.Duration(sp.FlushMs) * time.Millisecond,
		Format:        serdeFormat,
	}
	warmup := time.Duration(sp.WarmupS) * time.Second
	consumer, err := producer_consumer.NewShardedSharedLogStreamConsumer(input_stream, inConfig, sp.NumSubstreamProducer[0], sp.ParNum)
	if err != nil {
		return nil, nil, err
	}
	src := producer_consumer.NewMeteredConsumer(consumer, warmup)
	sink, err := producer_consumer.NewMeteredProducer(producer_consumer.NewShardedSharedLogStreamProducer(output_streams[0], outConfig), warmup)
	if err != nil {
		return nil, nil, err
	}
	return []*producer_consumer.MeteredConsumer{src}, []producer_consumer.MeteredProducerIntr{sink}, nil
}

func getSrcSinkUint64Key(
	ctx context.Context,
	env types.Environment,
	sp *common.QueryInput,
) ([]*producer_consumer.MeteredConsumer, []producer_consumer.MeteredProducerIntr, error) {
	input_stream, output_streams, err := benchutil.GetShardedInputOutputStreams(ctx, env, sp)
	if err != nil {
		return nil, nil, err
	}
	debug.Assert(len(output_streams) == 1, "expected only one output stream")
	serdeFormat := commtypes.SerdeFormat(sp.SerdeFormat)
	warmup := time.Duration(sp.WarmupS) * time.Second
	inConfig := &producer_consumer.StreamConsumerConfig{
		Timeout:     common.SrcConsumeTimeout,
		SerdeFormat: serdeFormat,
	}
	outConfig := &producer_consumer.StreamSinkConfig{
		FlushDuration: time.Duration(sp.FlushMs) * time.Millisecond,
		Format:        serdeFormat,
	}
	consumer, err := producer_consumer.NewShardedSharedLogStreamConsumer(input_stream, inConfig, sp.NumSubstreamProducer[0], sp.ParNum)
	if err != nil {
		return nil, nil, err
	}
	src := producer_consumer.NewMeteredConsumer(consumer, warmup)
	sink, err := producer_consumer.NewMeteredProducer(producer_consumer.NewShardedSharedLogStreamProducer(output_streams[0], outConfig), warmup)
	if err != nil {
		return nil, nil, err
	}
	return []*producer_consumer.MeteredConsumer{src}, []producer_consumer.MeteredProducerIntr{sink}, nil
}

func GetSerdeFromString(serdeStr string, serdeFormat commtypes.SerdeFormat) (commtypes.Serde, error) {
	switch serdeStr {
	case "StartEndTime":
		return ntypes.GetStartEndTimeSerde(serdeFormat)
	case "AuctionIdCount":
		return ntypes.NewAuctionIdCountSerde(serdeFormat)
	case "AuctionIdCntMax":
		return ntypes.GetAuctionIdCntMaxSerde(serdeFormat)
	case "AuctionIdCategory":
		return ntypes.GetAuctionIdCategorySerde(serdeFormat)
	case "AuctionBid":
		return ntypes.GetAuctionBidSerde(serdeFormat)
	case "AuctionIdSeller":
		return ntypes.GetAuctionIDSellerSerde(serdeFormat)
	case "ChangeUint64":
		return commtypes.GetChangeSerde(serdeFormat, commtypes.Uint64Serde{})
	case "ChangePriceTime":
		ptSerde, err := ntypes.GetPriceTimeSerde(serdeFormat)
		if err != nil {
			return nil, err
		}
		return commtypes.GetChangeSerde(serdeFormat, ptSerde)
	case "BidAndMax":
		return ntypes.GetBidAndMaxSerde(serdeFormat)
	case "Event":
		return ntypes.GetEventSerde(serdeFormat)
	case "Uint64":
		return commtypes.Uint64Serde{}, nil
	case "Float64":
		return commtypes.Float64Serde{}, nil
	case "String":
		return commtypes.StringSerde{}, nil
	case "PersonTime":
		return ntypes.GetPersonTimeSerde(serdeFormat)
	case "PriceTime":
		return ntypes.GetPriceTimeSerde(serdeFormat)
	case "NameCityStateId":
		return ntypes.GetNameCityStateIdSerde(serdeFormat)
	default:
		return nil, fmt.Errorf("Unrecognized serde string %s", serdeStr)
	}
}
