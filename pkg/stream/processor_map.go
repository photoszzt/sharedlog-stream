package stream

type Mapper interface {
	Map(Message) (Message, error)
}

var _ = (Mapper)(MapperFunc(nil))

func (fn MapperFunc) Map(msg Message) (Message, error) {
	return fn(msg)
}

type MapProcessor struct {
	pipe   Pipe
	mapper Mapper
}

func NewMapProcessor(mapper Mapper) Processor {
	return &MapProcessor{
		mapper: mapper,
	}
}

func (p *MapProcessor) WithPipe(pipe Pipe) {
	p.pipe = pipe
}

func (p *MapProcessor) Process(msg Message) error {
	msg, err := p.mapper.Map(msg)
	if err != nil {
		return err
	}
	return p.pipe.Forward(msg)
}
