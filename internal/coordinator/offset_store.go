package coordinator

func (g *Group) CommitOffset(topic string, partition int32, offset int64) {
	if _, ok := g.CommittedOffsets[topic]; !ok {
		g.CommittedOffsets[topic] = make(map[int32]int64)
	}
	g.CommittedOffsets[topic][partition] = offset
}

func (g *Group) FetchOffset(topic string, partition int32) (int64, bool) {
	pm, ok := g.CommittedOffsets[topic]
	if !ok {
		return 0, false
	}
	offset, ok := pm[partition]
	return offset, ok
}
