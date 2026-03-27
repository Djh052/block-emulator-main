package louvain

type WeightType float32

type Edge struct {
	destId int
	weight WeightType
}

type Edges [][]Edge

type Graph struct {
	incidences Edges
	selfs      []WeightType
}

func (this *Graph) AddUndirectedEdge(sourceId int, destId int, weight WeightType) {
	this.incidences[sourceId] = append(this.incidences[sourceId], Edge{destId, weight})
	//this.incidences[destId] = append(this.incidences[destId], Edge{sourceId, weight})
}

func (this *Graph) AddDirectedEdge(sourceId int, destId int, weight WeightType) {
	this.incidences[sourceId] = append(this.incidences[sourceId], Edge{destId, weight})
}

func (this *Graph) AddSelfEdge(nodeId int, weight WeightType) {
	this.selfs[nodeId] = weight
}

func (this *Graph) GetIncidentEdges(nodeId int) []Edge {
	return this.incidences[nodeId]
}

func (this *Graph) GetSelfWeight(nodeId int) WeightType {
	return this.selfs[nodeId]
}

func (this *Graph) GetNodeSize() int {
	return len(this.selfs)
}

func (this *Graph) GetEdgeSize() int {
	edgeNum := 0
	for _, edges := range this.incidences {
		edgeNum += len(edges)
	}
	return edgeNum / 2
}

func (this *Graph) GetEdgeAndTotalWeight() (WeightType, WeightType) {
	edgeWeight := WeightType(0)
	totalWeight := WeightType(0)
	for _, edges := range this.incidences {
		for _, edge := range edges {
			edgeWeight += edge.weight
		}
	}
	edgeWeight = edgeWeight / 2
	totalWeight = edgeWeight
	for _, self := range this.selfs {
		totalWeight += self / 2
	}
	return edgeWeight, totalWeight
}
