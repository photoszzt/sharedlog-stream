package operator

type FilterFunc func(interface{}) bool

type Filter struct {
	FilterF FilterFunc
}

func NewFilter(filterFunc FilterFunc) *Filter {
	_filter := &Filter{
		filterFunc,
	}
	return _filter
}
