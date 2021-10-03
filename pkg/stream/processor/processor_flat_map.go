package processor

type FlatMapper interface {
	FlatMap(Message) ([]Message, error)
}

var _ = (FlatMapper)(FlatMapperFunc(nil))

type FlatMapperFunc func(Message) ([]Message, error)

func (fn FlatMapperFunc) FlatMap(msg Message) ([]Message, error) {
	return fn(msg)
}

type FlatMapProcessor struct {
	pipe   Pipe
	mapper FlatMapper
	pctx   ProcessorContext
}

func NewFlatMapProcessor(mapper FlatMapper) *FlatMapProcessor {
	return &FlatMapProcessor{
		mapper: mapper,
	}
}

func (p *FlatMapProcessor) WithProcessorContext(pctx ProcessorContext) {
	p.pctx = pctx
}

func (p *FlatMapProcessor) WithPipe(pipe Pipe) {
	p.pipe = pipe
}

func (p *FlatMapProcessor) Process(msg Message) error {
	msgs, err := p.mapper.FlatMap(msg)
	if err != nil {
		return err
	}
	for _, m := range msgs {
		m.Timestamp = msg.Timestamp
		if err := p.pipe.Forward(m); err != nil {
			return err
		}
	}
	return nil
}

func (p *FlatMapProcessor) ProcessAndReturn(msg Message) ([]Message, error) {
	msgs, err := p.mapper.FlatMap(msg)
	if err != nil {
		return nil, err
	}
	return msgs, nil
}
