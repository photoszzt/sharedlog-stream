package store

type KeyValueSegments struct {
	BaseSegments
}

func NewKeyValueSegments(name string, retentionPeriod int64, segmentInterval int64) *KeyValueSegments {
	return &KeyValueSegments{
		BaseSegments: *NewBaseSegments(name, retentionPeriod, segmentInterval),
	}
}

func (kvs *KeyValueSegments) GetOrCreateSegment(segmentId int64) Segment {
	if kvs.segments.Has(Int64(segmentId)) {
		kv := kvs.segments.Get(Int64(segmentId)).(*KeySegment)
		return kv.Value
	} else {
		return nil
	}
}
