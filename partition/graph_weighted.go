package partition

import (
	"math"
	"time"
)

// 带时间衰减权重的边
type WeightedEdge struct {
	Target    Vertex
	Weight    float64 // 当前权重（考虑时间衰减）
	Original  float64 // 原始权重
	Timestamp int64   // 交易时间戳
}

// 带权重的图（用于 Leiden + 时间衰减）
type WeightedGraph struct {
	VertexSet map[Vertex]bool
	EdgeSet   map[Vertex][]WeightedEdge
}

// 添加带时间戳的边，计算衰减权重
func (wg *WeightedGraph) AddEdgeWithTime(u, v Vertex, timestamp int64, decayRate float64) {
	if wg.VertexSet == nil {
		wg.VertexSet = make(map[Vertex]bool)
	}
	if _, ok := wg.VertexSet[u]; !ok {
		wg.VertexSet[u] = true
	}
	if _, ok := wg.VertexSet[v]; !ok {
		wg.VertexSet[v] = true
	}

	if wg.EdgeSet == nil {
		wg.EdgeSet = make(map[Vertex][]WeightedEdge)
	}

	// 检查边是否已存在
	edgeExists := false
	for i, edge := range wg.EdgeSet[u] {
		if edge.Target == v {
			// 边已存在，更新权重（累加）
			wg.EdgeSet[u][i].Weight += math.Exp(-decayRate * float64(time.Now().Unix()-timestamp))
			wg.EdgeSet[u][i].Original += 1.0
			wg.EdgeSet[u][i].Timestamp = timestamp
			edgeExists = true
			break
		}
	}

	if !edgeExists {
		// 新边，计算时间衰减权重
		currentTime := time.Now().Unix()
		age := currentTime - timestamp
		weight := math.Exp(-decayRate * float64(age))

		edge := WeightedEdge{
			Target:    v,
			Weight:    weight,
			Original:  1.0,
			Timestamp: timestamp,
		}

		wg.EdgeSet[u] = append(wg.EdgeSet[u], edge)

		// 反向边
		reverseEdge := WeightedEdge{
			Target:    u,
			Weight:    weight,
			Original:  1.0,
			Timestamp: timestamp,
		}
		wg.EdgeSet[v] = append(wg.EdgeSet[v], reverseEdge)
	}
}

// 重新计算所有边的衰减权重（时间推移后调用）
func (wg *WeightedGraph) ReDecayEdges(decayRate float64) {
	currentTime := time.Now().Unix()

	for src, edges := range wg.EdgeSet {
		for i := range edges {
			edge := &edges[i]
			age := currentTime - edge.Timestamp
			edge.Weight = edge.Original * math.Exp(-decayRate*float64(age))
		}
		wg.EdgeSet[src] = edges
	}
}

// 获取节点的总加权度数
func (wg *WeightedGraph) GetWeightedDegree(v Vertex) float64 {
	var totalWeight float64
	for _, edge := range wg.EdgeSet[v] {
		totalWeight += edge.Weight
	}
	return totalWeight
}

// 获取节点到指定分片的连接权重
func (wg *WeightedGraph) GetEdgesToCommunity(v Vertex, partitionMap map[Vertex]int, community int) float64 {
	var totalWeight float64
	for _, edge := range wg.EdgeSet[v] {
		if partitionMap[edge.Target] == community {
			totalWeight += edge.Weight
		}
	}
	return totalWeight
}

// 复制图
func (dst *WeightedGraph) CopyWeightedGraph(src WeightedGraph) {
	dst.VertexSet = make(map[Vertex]bool)
	for v := range src.VertexSet {
		dst.VertexSet[v] = true
	}
	if src.EdgeSet != nil {
		dst.EdgeSet = make(map[Vertex][]WeightedEdge)
		for v, edges := range src.EdgeSet {
			dst.EdgeSet[v] = make([]WeightedEdge, len(edges))
			copy(dst.EdgeSet[v], edges)
		}
	}
}

// 输出图（带权重）
func (wg WeightedGraph) PrintWeightedGraph() {
	for v := range wg.VertexSet {
		print(v.Addr, " edges: ")
		for _, edge := range wg.EdgeSet[v] {
			print("->", edge.Target.Addr, "(w:", edge.Weight, ") ", "\t")
		}
		println()
	}
}
