package handlers

/*
type query3Handler struct {
	env      types.Environment
	funcName string
}

func NewQuery3(env types.Environment, funcName string) types.FuncHandler {
	return &query3Handler{
		env:      env,
		funcName: funcName,
	}
}

func (h *query3Handler) Call(ctx context.Context, input []byte) ([]byte, error) {
	parsedInput := &common.QueryInput{}
	err := json.Unmarshal(input, parsedInput)
	if err != nil {
		return nil, err
	}
	output := h.Query3(ctx, parsedInput)
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		panic(err)
	}
	fmt.Printf("query 3 output: %v\n", encodedOutput)
	return utils.CompressData(encodedOutput), nil
}

func (h *query3Handler) Query3(ctx context.Context, input *common.QueryInput) *common.FnOutput {
	inputStream, err := sharedlog_stream.NewSharedLogStream(h.env, input.InputTopicNames[0],
		commtypes.SerdeFormat(input.SerdeFormat))
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}
	outputStream, err := sharedlog_stream.NewSharedLogStream(h.env, input.OutputTopicName, commtypes.SerdeFormat(input.SerdeFormat))
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}
	msgSerde, err := commtypes.GetMsgSerde(input.SerdeFormat)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}
	eventSerde, err := getEventSerde(input.SerdeFormat)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}
	timeout := time.Duration(input.Duration) * time.Second
	if timeout == 0 {
		timeout = common.SrcConsumeTimeout
	}
	inConfig := &sharedlog_stream.StreamSourceConfig{
		Timeout:      timeout,
		KeyDecoder:   commtypes.StringDecoder{},
		ValueDecoder: eventSerde,
		MsgDecoder:   msgSerde,
	}
	var ncsiSerde commtypes.Serde
	if input.SerdeFormat == uint8(commtypes.JSON) {
		ncsiSerde = ntypes.NameCityStateIdJSONSerde{}
	} else {
		ncsiSerde = ntypes.NameCityStateIdMsgpSerde{}
	}
	outConfig := &sharedlog_stream.StreamSinkConfig{
		KeySerde:   commtypes.Uint64Serde{},
		ValueSerde: ncsiSerde,
		MsgSerde:   msgSerde,
	}
	builder := stream.NewStreamBuilder()
	inputs := builder.Source("nexmark-src", sharedlog_stream.NewSharedLogStreamSource(inputStream, inConfig))
	auctionsBySellerId := inputs.Filter("filter-auction",
		processor.PredicateFunc(func(msg *commtypes.Message) (bool, error) {
			event := msg.Value.(ntypes.Event)
			return event.Etype == ntypes.AUCTION, nil
		})).
		Filter("filter-category",
			processor.PredicateFunc(func(msg *commtypes.Message) (bool, error) {
				event := msg.Value.(*ntypes.Event)
				return event.NewAuction.Category == 10, nil
			})).
		Map("update-key",
			processor.MapperFunc(func(msg commtypes.Message) (commtypes.Message, error) {
				event := msg.Value.(*ntypes.Event)
				return commtypes.Message{Key: event.NewAuction.Seller, Value: msg.Value, Timestamp: msg.Timestamp}, nil
			})).
		ToTable("convert-to-table")

	personsById := inputs.Filter("filter-person",
		processor.PredicateFunc(func(msg *commtypes.Message) (bool, error) {
			event := msg.Value.(ntypes.Event)
			return event.Etype == ntypes.PERSON, nil
		})).
		Filter("filter-state", processor.PredicateFunc(func(msg *commtypes.Message) (bool, error) {
			event := msg.Value.(ntypes.Event)
			state := event.NewPerson.State
			return state == "OR" || state == "ID" || state == "CA", nil
		})).
		Map("update-key", processor.MapperFunc(func(msg commtypes.Message) (commtypes.Message, error) {
			event := msg.Value.(*ntypes.Event)
			return commtypes.Message{Key: event.NewPerson.ID, Value: msg.Value, Timestamp: msg.Timestamp}, nil
		})).
		ToTable("convert-to-table")

	auctionsBySellerId.
		Join("join", personsById, processor.ValueJoinerFunc(func(leftVal interface{}, rightVal interface{}) interface{} {
			event := rightVal.(*ntypes.Event)
			return &ntypes.NameCityStateId{
				Name:  event.NewPerson.Name,
				City:  event.NewPerson.City,
				State: event.NewPerson.State,
				ID:    event.NewPerson.ID,
			}
		})).
		ToStream().
		Process("sink", sharedlog_stream.NewSharedLogStreamSink(outputStream, outConfig))
	tp, err_arrs := builder.Build()
	if err_arrs != nil {
		return &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("build stream failed: %v", err_arrs),
		}
	}
	pumps := make(map[processor.Node]processor.Pump)
	var srcPumps []processor.SourcePump
	nodes := processor.FlattenNodeTree(tp.Sources())
	processor.ReverseNodes(nodes)
	for _, node := range nodes {
		pipe := processor.NewPipe(processor.ResolvePumps(pumps, node.Children()))
		node.Processor().WithPipe(pipe)

		pump := processor.NewSyncPump(node, pipe)
		pumps[node] = pump
	}
	for source, node := range tp.Sources() {
		srcPump := processor.NewSourcePump(ctx, node.Name(), source, 0, processor.ResolvePumps(pumps, node.Children()), func(err error) {
			log.Fatal(err.Error())
		})
		srcPumps = append(srcPumps, srcPump)
	}

	duration := time.Duration(input.Duration) * time.Second
	latencies := make([]int, 0, 128)
	startTime := time.Now()

	time.After(duration)
	for _, srcPump := range srcPumps {
		srcPump.Stop()
		srcPump.Close()
	}

	return &common.FnOutput{
		Success:   true,
		Duration:  time.Since(startTime).Seconds(),
		Latencies: map[string][]int{"e2e": latencies},
	}
}
*/
