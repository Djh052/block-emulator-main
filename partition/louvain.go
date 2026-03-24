package partition

import (
	"bytes"
	"crypto/sha256"
	"encoding/gob"
	"fmt"
	"log"
	"math"
)

// Leiden 算法状态
type LeidenState struct {
	NetGraph          WeightedGraph  // 需运行 Leiden 算法的带权图
	PartitionMap      map[Vertex]int // 记录分片信息的 map
	Weight2Shard      []float64      // Shard 相关联的权重和
	VertexsNumInShard []int          // Shard 内节点的数目
	Gamma             float64        // 负载均衡参数
	Resolution        float64        // Leiden 的分辨率参数 (γ)
	MaxIterations     int            // 最大迭代次数
	CrossShardWeight  float64        // 跨分片边的总权重
	ShardNum          int            // 分片数目
	GraphHash         []byte
	MinWeight2Shard   float64 // 最少的 Shard 相关联的权重
}

func (graph *LeidenState) Hash() []byte {
	hash := sha256.Sum256(graph.Encode())
	return hash[:]
}

func (graph *LeidenState) Encode() []byte {
	var buff bytes.Buffer

	enc := gob.NewEncoder(&buff)
	err := enc.Encode(graph)
	if err != nil {
		log.Panic(err)
	}

	return buff.Bytes()
}

// 初始化 Leiden 状态
func (ls *LeidenState) Init_LeidenState(gamma, resolution float64, maxIter, sn int) {
	ls.NetGraph = WeightedGraph{}
	ls.PartitionMap = make(map[Vertex]int)
	ls.Weight2Shard = make([]float64, sn)
	ls.VertexsNumInShard = make([]int, sn)
	ls.Gamma = gamma
	ls.Resolution = resolution
	ls.MaxIterations = maxIter
	ls.ShardNum = sn
	ls.CrossShardWeight = 0.0
	ls.MinWeight2Shard = 0.0
}

// 添加节点
func (ls *LeidenState) AddVertex(v Vertex) {
	// 关键修复：初始化 VertexSet（避免 nil map 赋值）
	if ls.NetGraph.VertexSet == nil {
		ls.NetGraph.VertexSet = make(map[Vertex]bool)
	}
	ls.NetGraph.VertexSet[v] = true
	if _, ok := ls.PartitionMap[v]; !ok {
		// 初始化时使用地址哈希（取地址最后一个字符的 ASCII 值）
		num := int(v.Addr[len(v.Addr)-1]) % ls.ShardNum
		ls.PartitionMap[v] = num
		ls.VertexsNumInShard[num] += 1
	}
}

// 添加带时间戳的边
func (ls *LeidenState) AddEdgeWithTime(u, v Vertex, timestamp int64, decayRate float64) {
	if _, ok := ls.NetGraph.VertexSet[u]; !ok {
		ls.AddVertex(u)
	}
	if _, ok := ls.NetGraph.VertexSet[v]; !ok {
		ls.AddVertex(v)
	}
	ls.NetGraph.AddEdgeWithTime(u, v, timestamp, decayRate)
}

// 计算当前划分下各 Shard 的权重
func (ls *LeidenState) ComputeWeight2Shard() {
	ls.Weight2Shard = make([]float64, ls.ShardNum)
	ls.MinWeight2Shard = math.MaxFloat64

	interWeight := make([]float64, ls.ShardNum)

	for v, edges := range ls.NetGraph.EdgeSet {
		vShard := ls.PartitionMap[v]
		for _, edge := range edges {
			uShard := ls.PartitionMap[edge.Target]
			if vShard != uShard {
				ls.Weight2Shard[uShard] += edge.Weight
			} else {
				interWeight[uShard] += edge.Weight
			}
		}
	}

	// 计算总权重
	ls.CrossShardWeight = 0.0
	for _, val := range ls.Weight2Shard {
		ls.CrossShardWeight += val
	}
	ls.CrossShardWeight /= 2.0 // 每条跨分片边被计算了两次

	// 加上内部权重，得到各 Shard 的总关联权重
	for idx := 0; idx < ls.ShardNum; idx++ {
		ls.Weight2Shard[idx] += interWeight[idx] / 2.0
		if ls.MinWeight2Shard > ls.Weight2Shard[idx] {
			ls.MinWeight2Shard = ls.Weight2Shard[idx]
		}
	}
}

// 计算将节点 v 放入 community 产生的模块度增益（带负载均衡约束）
func (ls *LeidenState) getModularityGain(v Vertex, community int) float64 {
	vDegree := ls.NetGraph.GetWeightedDegree(v)
	if vDegree == 0 {
		return 0.0
	}

	// 计算节点 v 到 community 的内部边权重总和
	internalWeight := ls.NetGraph.GetEdgesToCommunity(v, ls.PartitionMap, community)

	// 计算目标 community 的总度数（内部边 × 2 + 外部边）
	communityWeight := ls.Weight2Shard[community]
	if communityWeight == 0 {
		communityWeight = vDegree // 避免 / 0
	}

	// 计算图的边权重总和
	totalWeight := 0.0
	for v := range ls.NetGraph.VertexSet {
		totalWeight += ls.NetGraph.GetWeightedDegree(v)
	}
	totalWeight /= 2.0 // 每条边被计算了两次

	// Leiden 的模块度增益公式
	// ΔQ = 2 * (k_i_in / (2m) - k_i * Σ_tot / (2m)^2)
	modularityGain := 2.0 * (internalWeight/totalWeight - vDegree*communityWeight/(totalWeight*totalWeight))

	// 负载均衡约束：惩罚过载的 Shard
	avgNodesPerShard := float64(len(ls.NetGraph.VertexSet)) / float64(ls.ShardNum)
	currentNodesInShard := float64(ls.VertexsNumInShard[community])

	// 如果移动后超过平均值的 (1 + Gamma) 倍，进行惩罚
	if currentNodesInShard > avgNodesPerShard*(1.0+ls.Gamma) {
		loadPenalty := (currentNodesInShard - avgNodesPerShard*(1.0+ls.Gamma)) / avgNodesPerShard
		modularityGain -= loadPenalty
	}

	return modularityGain
}

// 在账户所属分片变动时，增量重新计算权重（优化性能）
func (ls *LeidenState) changeShardRecompute(v Vertex, oldShard int) {
	newShard := ls.PartitionMap[v]

	// 更新节点数量
	ls.VertexsNumInShard[oldShard] -= 1
	ls.VertexsNumInShard[newShard] += 1

	// 重新计算受影响的 Shard 的权重
	// 这里简化处理，实际可以优化为只更新相关边
	ls.ComputeWeight2Shard()
}

// Fast Local Moving 阶段（类似 Louvain）
func (ls *LeidenState) fastLocalMoving() {
	for iter := 0; iter < ls.MaxIterations; iter++ {
		changed := false

		// 随机遍历节点
		nodeList := make([]Vertex, 0, len(ls.NetGraph.VertexSet))
		for v := range ls.NetGraph.VertexSet {
			nodeList = append(nodeList, v)
		}

		for _, v := range nodeList {
			currentShard := ls.PartitionMap[v]

			// 找到所有邻居所在的 community
			neighborCommunities := make(map[int]bool)
			for _, edge := range ls.NetGraph.EdgeSet[v] {
				neighborCommunities[ls.PartitionMap[edge.Target]] = true
			}

			// 寻找最佳 community
			bestShard := currentShard
			bestGain := 0.0

			for community := range neighborCommunities {
				// 确保不会产生空分片
				if ls.VertexsNumInShard[currentShard] <= 1 {
					continue
				}

				gain := ls.getModularityGain(v, community)
				if gain > bestGain {
					bestGain = gain
					bestShard = community
				}
			}

			// 移动节点
			if bestShard != currentShard && bestGain > 0.0001 {
				ls.PartitionMap[v] = bestShard
				ls.changeShardRecompute(v, currentShard)
				changed = true
			}
		}

		if !changed {
			break
		}
	}
}

// Refinement 阶段（保证连通性）
func (ls *LeidenState) refinement() {
	// Leiden 的 refinement 阶段：将每个 partition 进一步细分为连通的 community
	// 这里简化实现，实际需要保证每个 shard 是连通的

	// 对于每个 shard，检查是否连通
	// 如果不连通，将其拆分为多个子 community
	// ... (简化处理，可以后续实现)
}

// Aggregation 阶段
func (ls *LeidenState) aggregation() *LeidenState {
	// 创建新的图，节点是 shard，边是 shard 之间的权重
	newState := new(LeidenState)
	newState.Init_LeidenState(ls.Gamma, ls.Resolution, ls.MaxIterations, ls.ShardNum)

	// 创建 super-nodes
	shardToSuperNode := make(map[int]Vertex)
	for shard := 0; shard < ls.ShardNum; shard++ {
		superNode := Vertex{Addr: fmt.Sprintf("shard_%d", shard)}
		newState.AddVertex(superNode)
		shardToSuperNode[shard] = superNode
	}

	// 计算 shard 之间的边权重
	shardEdgeWeight := make([][]float64, ls.ShardNum)
	for i := range shardEdgeWeight {
		shardEdgeWeight[i] = make([]float64, ls.ShardNum)
	}

	for v, edges := range ls.NetGraph.EdgeSet {
		vShard := ls.PartitionMap[v]
		for _, edge := range edges {
			uShard := ls.PartitionMap[edge.Target]
			shardEdgeWeight[vShard][uShard] += edge.Weight
		}
	}

	// 在 super-nodes 之间创建边
	for i := 0; i < ls.ShardNum; i++ {
		for j := i; j < ls.ShardNum; j++ {
			if shardEdgeWeight[i][j] > 0 {
				newState.NetGraph.AddEdgeWithTime(shardToSuperNode[i], shardToSuperNode[j], 0, 0.0)
				// 这里需要手动设置权重，因为 AddEdgeWithTime 会计算衰减
				// 可以添加一个 SetWeight 方法
			}
		}
	}

	// 初始化 super-nodes 的分区
	for shard := 0; shard < ls.ShardNum; shard++ {
		newState.PartitionMap[shardToSuperNode[shard]] = shard
	}

	return newState
}

// 核心 Leiden 分区算法
func (ls *LeidenState) Leiden_Partition() (map[string]uint64, int) {
	ls.ComputeWeight2Shard()
	fmt.Println("Before running Leiden, cross-shard edge weight:", ls.CrossShardWeight)

	// 阶段 1: Fast Local Moving
	ls.fastLocalMoving()

	// 阶段 2: Refinement（可选，保证连通性）
	ls.refinement()

	// 阶段 3: Aggregation（可选，递归处理）
	// currentLevel := ls.aggregation()
	// 可以递归调用 Leiden_Partition，但这里简化为单层

	ls.ComputeWeight2Shard()
	fmt.Println("After running Leiden, cross-shard edge weight:", ls.CrossShardWeight)

	// 转换为返回格式
	res := make(map[string]uint64)
	for v, shard := range ls.PartitionMap {
		res[v.Addr] = uint64(shard)
	}

	return res, int(ls.CrossShardWeight)
}

// 复制 Leiden 状态
func (dst *LeidenState) CopyLeiden(src LeidenState) {
	dst.NetGraph.CopyWeightedGraph(src.NetGraph)
	dst.PartitionMap = make(map[Vertex]int)
	for v := range src.PartitionMap {
		dst.PartitionMap[v] = src.PartitionMap[v]
	}
	dst.Weight2Shard = make([]float64, src.ShardNum)
	copy(dst.Weight2Shard, src.Weight2Shard)
	dst.VertexsNumInShard = src.VertexsNumInShard
	dst.Gamma = src.Gamma
	dst.Resolution = src.Resolution
	dst.MinWeight2Shard = src.MinWeight2Shard
	dst.MaxIterations = src.MaxIterations
	dst.ShardNum = src.ShardNum
}

// 输出 Leiden 分区
func (ls *LeidenState) PrintLeiden() {
	for shard, num := range ls.VertexsNumInShard {
		fmt.Printf("Shard %d has %d vertices\n", shard, num)
	}
	fmt.Printf("Cross-shard edge weight: %.4f\n", ls.CrossShardWeight)
}
