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

func NewFlatMapProcessor(mapper FlatMapper) Processor {
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
	for _, msg := range msgs {
		if err := p.pipe.Forward(msg); err != nil {
			return err
		}
	}
	return nil
}
