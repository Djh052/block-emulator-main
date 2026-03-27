package louvain

type GraphReader struct {
	nodeLabelToIndex map[string]int //真实地址转整数 ID
	nodeIndexToLabel map[int]string //整数 ID 转真实地址
	NodeToNodeWeight map[int]map[int]WeightType
}

func NewGraphReader() *GraphReader { //这里的1000不够呀，后续会造成影响吗--改成map了
	// return &GraphReader{map[string]int{}, make([]string, 0, 1000), make(map[int]map[int]WeightType)}
	return &GraphReader{map[string]int{}, make(map[int]string), make(map[int]map[int]WeightType)}
}

func (gr *GraphReader) addNode(nodeLabel string) {
	if gr.nodeLabelToIndex == nil {
		gr.nodeLabelToIndex = make(map[string]int)
	}
	if gr.nodeIndexToLabel == nil {
		gr.nodeIndexToLabel = make(map[int]string)
	}
	nodeNum := len(gr.nodeLabelToIndex)
	gr.nodeLabelToIndex[nodeLabel] = nodeNum
	gr.nodeIndexToLabel[nodeNum] = nodeLabel
}

func (gr *GraphReader) GetNodeIndex(nodeLabel string) int {
	return gr.nodeLabelToIndex[nodeLabel]
}

func (gr *GraphReader) GetNodeLabel(nodeIndex int) string {
	return gr.nodeIndexToLabel[nodeIndex]
}

func (gr *GraphReader) GetNodeSize() int {
	return len(gr.nodeLabelToIndex)
}

func (gr *GraphReader) addEdge(u, v string) {
	nodeId0 := gr.GetNodeIndex(u)
	nodeId1 := gr.GetNodeIndex(v)
	if _, exists := gr.NodeToNodeWeight[nodeId0]; !exists {
		gr.NodeToNodeWeight[nodeId0] = make(map[int]WeightType)
	}
	if _, exists := gr.NodeToNodeWeight[nodeId1]; !exists {
		gr.NodeToNodeWeight[nodeId1] = make(map[int]WeightType)
	}
	if _, exists := gr.NodeToNodeWeight[nodeId0][nodeId1]; !exists {
		gr.NodeToNodeWeight[nodeId0][nodeId1] = WeightType(1.0)
	} else {
		gr.NodeToNodeWeight[nodeId0][nodeId1] += WeightType(1.0)
	}
	if _, exists := gr.NodeToNodeWeight[nodeId1][nodeId0]; !exists {
		gr.NodeToNodeWeight[nodeId1][nodeId0] = WeightType(1.0)
	} else {
		gr.NodeToNodeWeight[nodeId1][nodeId0] += WeightType(1.0)
	}
}

func (gr *GraphReader) GenerateGraph() Graph {
	graph := Graph{make(Edges, gr.GetNodeSize()), make([]WeightType, gr.GetNodeSize())}
	for node, neighbors := range gr.NodeToNodeWeight {
		// 遍历当前节点的相邻节点及其权重
		for neighbor, weight := range neighbors {
			graph.AddUndirectedEdge(node, neighbor, weight)
		}
	}
	return graph
}

// 重置图
func (gr *GraphReader) ResetGraph() {
	gr.nodeLabelToIndex = make(map[string]int)
	gr.nodeIndexToLabel = make(map[int]string)
	gr.NodeToNodeWeight = make(map[int]map[int]WeightType)
}
