package partition

import (
	"math"
)

// 带周期衰减权重的边
type WeightedEdge struct {
	Target Vertex
	Weight float64 // 当前权重（考虑周期衰减）
	Epoch  int     // 最后一次更新的周期
}

// 带权重的图（用于 Leiden/Louvain + 周期衰减）
type WeightedGraph struct {
	VertexSet map[Vertex]bool
	EdgeSet   map[Vertex][]WeightedEdge
}

// 添加带周期的边，计算衰减权重（新增权重封顶限制）
func (wg *WeightedGraph) AddEdgeWithEpoch(u, v Vertex, currentEpoch int, decayRate float64) {
	if wg.VertexSet == nil {
		wg.VertexSet = make(map[Vertex]bool)
	}
	wg.VertexSet[u] = true
	wg.VertexSet[v] = true

	if wg.EdgeSet == nil {
		wg.EdgeSet = make(map[Vertex][]WeightedEdge)
	}

	maxWeight := 10.0 // 权重封顶，防止形成无法切分的“超级绑定”

	// 辅助函数：更新单向边
	updateEdge := func(src, dst Vertex) {
		edgeExists := false
		for i, edge := range wg.EdgeSet[src] {
			if edge.Target == dst {
				age := currentEpoch - edge.Epoch
				if age < 0 {
					age = 0
				}
				decayedWeight := edge.Weight * math.Exp(-decayRate*float64(age))
				newWeight := decayedWeight + 1.0

				if newWeight > maxWeight {
					newWeight = maxWeight
				}

				wg.EdgeSet[src][i].Weight = newWeight
				wg.EdgeSet[src][i].Epoch = currentEpoch
				edgeExists = true
				break
			}
		}

		if !edgeExists {
			wg.EdgeSet[src] = append(wg.EdgeSet[src], WeightedEdge{
				Target: dst,
				Weight: 1.0,
				Epoch:  currentEpoch,
			})
		}
	}

	// 无向图，双向添加
	updateEdge(u, v)
	updateEdge(v, u)
}

// 重新计算所有边的衰减权重，并执行【边剪枝】与【节点清理】
func (wg *WeightedGraph) ReDecayEdges(currentEpoch int, decayRate float64) {
	pruneThreshold := 0.2 // 剪枝阈值

	// 1. 衰减并清理过期的边
	for src, edges := range wg.EdgeSet {
		var activeEdges []WeightedEdge
		for _, edge := range edges {
			age := currentEpoch - edge.Epoch
			if age > 0 {
				edge.Weight = edge.Weight * math.Exp(-decayRate*float64(age))
				edge.Epoch = currentEpoch // 更新到最新周期
			}

			// 只有权重大于阈值的边才会被保留
			if edge.Weight >= pruneThreshold {
				activeEdges = append(activeEdges, edge)
			}
		}

		if len(activeEdges) == 0 {
			delete(wg.EdgeSet, src)
		} else {
			wg.EdgeSet[src] = activeEdges
		}
	}

	// 2. 【核心修复】清理掉所有失去连接的“幽灵节点”
	// 这样它们就不会再污染 Louvain 的负载均衡计算（BalancePenalty）
	for v := range wg.VertexSet {
		if len(wg.EdgeSet[v]) == 0 {
			delete(wg.VertexSet, v)
		}
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
			print("->", edge.Target.Addr, "(w:", edge.Weight, ", ep:", edge.Epoch, ") \t")
		}
		println()
	}
}
