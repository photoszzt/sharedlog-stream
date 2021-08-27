package stream

type Topology struct {
	sources    map[Source]Node
	processors []Node
}

func (t Topology) Sources() map[Source]Node {
	return t.sources
}

func (t Topology) Processors() []Node {
	return t.processors
}

type TopologyBuilder struct {
	sources    map[Source]Node
	processors []Node
}

func NewTopologyBuilder() *TopologyBuilder {
	return &TopologyBuilder{
		sources:    map[Source]Node{},
		processors: []Node{},
	}
}

func (tb *TopologyBuilder) AddSource(name string, source Source) Node {
	n := NewSourceNode(name)
	tb.sources[source] = n
	return n
}

func (tb *TopologyBuilder) AddProcessor(name string, processor Processor, parents []Node) Node {
	n := NewProcessorNode(name, processor)
	for _, parent := range parents {
		parent.AddChild(n)
	}
	tb.processors = append(tb.processors, n)
	return n
}

func (tb *TopologyBuilder) Build() (*Topology, []error) {
	var errs []error
	return &Topology{
		sources:    tb.sources,
		processors: tb.processors,
	}, errs
}

func nodesConnected(roots []Node) bool {
	if len(roots) <= 1 {
		return true
	}

	var nodes []Node
	var visit []Node

	connections := 0
	for _, node := range roots {
		visit = append(visit, node)
	}

	for len(visit) > 0 {
		var n Node
		n, visit = visit[0], visit[1:]
		nodes = append(nodes, n)

		for _, c := range n.Children() {
			if contains(c, visit) || contains(c, nodes) {
				connections++
				continue
			}
			visit = append(visit, c)
		}
	}

	return connections == len(roots)-1
}

func FlattenNodeTree(roots map[Source]Node) []Node {
	var nodes []Node
	var visit []Node

	for _, node := range roots {
		visit = append(visit, node)
	}

	for len(visit) > 0 {
		var n Node
		n, visit = visit[0], visit[1:]

		if n.Processor() != nil {
			nodes = append(nodes, n)
		}

		for _, c := range n.Children() {
			if contains(c, visit) || contains(c, nodes) {
				continue
			}

			visit = append(visit, c)
		}
	}

	// In asymmetric trees, our dependencies can be out of order,
	// which will cause errors. In order to ratify this, we check
	// that not dependency appears higher in the list than us.
	for i := 0; i < len(nodes); i++ {
		node := nodes[i]
		for _, child := range node.Children() {
			pos := indexOf(child, nodes)
			if pos < i {
				temp := nodes[pos]
				nodes[pos] = nodes[i]
				nodes[i] = temp
				i = pos
			}
		}
	}

	return nodes
}

func ReverseNodes(nodes []Node) {
	for i := len(nodes)/2 - 1; i >= 0; i-- {
		opp := len(nodes) - 1 - i
		nodes[i], nodes[opp] = nodes[opp], nodes[i]
	}
}

func contains(n Node, nodes []Node) bool {
	for _, node := range nodes {
		if node == n {
			return true
		}
	}
	return false
}

func indexOf(n Node, nodes []Node) int {
	for i, node := range nodes {
		if node == n {
			return i
		}
	}
	return -1
}
