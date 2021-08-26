package stream

type Node interface {
	// Name: get node name
	Name() string
	// AddChild adds a child node to the node.
	AddChild(n Node)
	// Children gets the node's children.
	Children() []Node
	// Processor gets the node's processor.
	Processor() Processor
}

var _ = (Node)(&SourceNode{})

type SourceNode struct {
	name     string
	children []Node
}

func NewSourceNode(name string) *SourceNode {
	return &SourceNode{
		name: name,
	}
}

func (s *SourceNode) Name() string {
	return s.name
}

func (s *SourceNode) AddChild(node Node) {
	s.children = append(s.children, node)
}

func (s *SourceNode) Children() []Node {
	return s.children
}

func (s *SourceNode) Processor() Processor {
	return nil
}

var _ = (Node)(&ProcessorNode{})

type ProcessorNode struct {
	name      string
	processor Processor
	children  []Node
}

func NewProcessorNode(name string, p Processor) *ProcessorNode {
	return &ProcessorNode{
		name:      name,
		processor: p,
	}
}

func (p *ProcessorNode) Name() string {
	return p.name
}

func (p *ProcessorNode) AddChild(node Node) {
	p.children = append(p.children, node)
}

func (p *ProcessorNode) Children() []Node {
	return p.children
}

func (p *ProcessorNode) Processor() Processor {
	return p.processor
}
