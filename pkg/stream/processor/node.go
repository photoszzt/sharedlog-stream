package processor

import "github.com/rs/zerolog/log"

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

var _ = Node(&KeyValueStoreNode{})

type KeyValueStoreNode struct {
	name string
}

func NewKeyValueStoreNode(name string) *KeyValueStoreNode {
	return &KeyValueStoreNode{
		name: name,
	}
}

func (s *KeyValueStoreNode) Name() string {
	return s.name
}

func (s *KeyValueStoreNode) AddChild(node Node) {
	log.Error().Msg("KeyValueStoreNode doesn't have child")
}

func (s *KeyValueStoreNode) Children() []Node {
	return nil
}

func (s *KeyValueStoreNode) Processor() Processor {
	return nil
}

var _ = (Node)(&ProcessorNode{})

type ProcessorNode struct {
	name          string
	processor     Processor
	children      []Node
	keyChangingOp bool
	valChangingOp bool
}

func NewProcessorNode(name string, p Processor) *ProcessorNode {
	return &ProcessorNode{
		name:          name,
		processor:     p,
		keyChangingOp: false,
		valChangingOp: false,
	}
}

func (p *ProcessorNode) SetKeyChangingOp(keyChangingOp bool) {
	p.keyChangingOp = keyChangingOp
}

func (p *ProcessorNode) SetValChangingOp(valChangingOp bool) {
	p.valChangingOp = valChangingOp
}

func (p *ProcessorNode) KeyChangingOp() bool {
	return p.keyChangingOp
}

func (p *ProcessorNode) ValChangingOp() bool {
	return p.valChangingOp
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
