package processor

type ErrorFunc func(error)

func ResolvePumps(pumps map[Node]Pump, nodes []Node) []Pump {
	var pumps_ret []Pump
	for _, node := range nodes {
		pumps_ret = append(pumps_ret, pumps[node])
	}
	return pumps_ret
}
