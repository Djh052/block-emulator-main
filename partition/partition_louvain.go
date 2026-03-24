package partition

import (
	"blockEmulator/utils"
	"bytes"
	"crypto/sha256"
	"encoding/gob"
	"errors"
	"fmt"
	"log"
	"math"
	"os"
	"strconv"
)

// Louvain算法状态（固定分片数、局部移动版）
// 注意：这不是“标准多层Louvain”的完整实现，
// 而是适合你当前分片场景的 fixed-K constrained Louvain。

func init() {
	logger = log.New(os.Stdout, "[Louvain] ", log.Ldate|log.Ltime|log.Lshortfile)
}

type LouvainState struct {
	NetGraph          Graph
	PartitionMap      map[Vertex]int // 节点 -> 分片
	VertexsNumInShard []int          // 每个分片中的节点数
	CommTot           []float64      // 每个分片的总度 Σ_tot
	BalancePenalty    float64        // 负载均衡惩罚系数 beta
	MaxIterations     int            // 最大迭代次数
	CrossShardEdgeNum int            // 当前跨分片边数
	ShardNum          int            // 固定分片数
	TotalEdgeWeight   float64        // 图总边权 m（这里无权图中等于边数）
	GraphHash         []byte
}

func (graph *LouvainState) Hash() []byte {
	hash := sha256.Sum256(graph.Encode())
	return hash[:]
}

func (graph *LouvainState) Encode() []byte {
	var buff bytes.Buffer
	enc := gob.NewEncoder(&buff)
	err := enc.Encode(graph)
	if err != nil {
		log.Panic(err)
	}
	return buff.Bytes()
}

// 初始化参数
func (ls *LouvainState) Init_LouvainState(bp float64, mIter, sn int) {
	ls.BalancePenalty = bp
	ls.MaxIterations = mIter
	ls.ShardNum = sn
	ls.VertexsNumInShard = make([]int, ls.ShardNum)
	ls.PartitionMap = make(map[Vertex]int)
	ls.CommTot = make([]float64, ls.ShardNum)
}

// 加入节点，默认放到一个分片
func (ls *LouvainState) AddVertex(v Vertex) {
	ls.NetGraph.AddVertex(v)
	if val, ok := ls.PartitionMap[v]; !ok {
		ls.PartitionMap[v] = utils.Addr2Shard(v.Addr)
	} else {
		ls.PartitionMap[v] = val
	}
	ls.VertexsNumInShard[ls.PartitionMap[v]] += 1
}

// 加入边
func (ls *LouvainState) AddEdge(u, v Vertex) {
	if _, ok := ls.NetGraph.VertexSet[u]; !ok {
		ls.AddVertex(u)
	}
	if _, ok := ls.NetGraph.VertexSet[v]; !ok {
		ls.AddVertex(v)
	}
	ls.NetGraph.AddEdge(u, v)
}

// 复制Louvain状态
func (dst *LouvainState) CopyLouvain(src LouvainState) {
	dst.NetGraph.CopyGraph(src.NetGraph)

	dst.PartitionMap = make(map[Vertex]int)
	for v := range src.PartitionMap {
		dst.PartitionMap[v] = src.PartitionMap[v]
	}

	dst.VertexsNumInShard = make([]int, src.ShardNum)
	copy(dst.VertexsNumInShard, src.VertexsNumInShard)

	dst.CommTot = make([]float64, src.ShardNum)
	copy(dst.CommTot, src.CommTot)

	dst.BalancePenalty = src.BalancePenalty
	dst.MaxIterations = src.MaxIterations
	dst.CrossShardEdgeNum = src.CrossShardEdgeNum
	dst.ShardNum = src.ShardNum
	dst.TotalEdgeWeight = src.TotalEdgeWeight
}

// 输出状态
func (ls *LouvainState) PrintLouvain() {
	ls.NetGraph.PrintGraph()
	fmt.Println("Cross-shard edge number:", ls.CrossShardEdgeNum)
	fmt.Println("Total edge weight:", ls.TotalEdgeWeight)

	for v, item := range ls.PartitionMap {
		print(v.Addr, " ", item, "\t")
	}
	println()

	fmt.Println("VertexsNumInShard:", ls.VertexsNumInShard)
	fmt.Println("CommTot:", ls.CommTot)
}

// 重新计算社区统计量：
// 1) 每个分片中的节点数
// 2) 每个分片的总度 CommTot
// 3) 图总边数 TotalEdgeWeight
// 4) 跨分片边数 CrossShardEdgeNum
func (ls *LouvainState) ComputeCommunityStats() {
	ls.VertexsNumInShard = make([]int, ls.ShardNum)
	ls.CommTot = make([]float64, ls.ShardNum)
	ls.CrossShardEdgeNum = 0
	ls.TotalEdgeWeight = 0.0

	for v := range ls.NetGraph.VertexSet {
		shard := ls.PartitionMap[v]
		ls.VertexsNumInShard[shard] += 1
		deg := float64(len(ls.NetGraph.EdgeSet[v]))
		ls.CommTot[shard] += deg
		ls.TotalEdgeWeight += deg
	}
	ls.TotalEdgeWeight /= 2.0

	for v, lst := range ls.NetGraph.EdgeSet {
		vShard := ls.PartitionMap[v]
		for _, u := range lst {
			uShard := ls.PartitionMap[u]
			if vShard != uShard {
				ls.CrossShardEdgeNum += 1
			}
		}
	}
	ls.CrossShardEdgeNum /= 2
}

// 初始化划分：按地址尾数
func (ls *LouvainState) Init_Partition() {
	ls.VertexsNumInShard = make([]int, ls.ShardNum)
	ls.PartitionMap = make(map[Vertex]int)

	for v := range ls.NetGraph.VertexSet {
		var va = v.Addr[len(v.Addr)-8:]
		num, err := strconv.ParseInt(va, 16, 64)
		if err != nil {
			log.Panic()
		}
		ls.PartitionMap[v] = int(num) % ls.ShardNum
		ls.VertexsNumInShard[ls.PartitionMap[v]] += 1
	}
	ls.ComputeCommunityStats()
}

// 稳定初始化：保证不会有空分片
func (ls *LouvainState) Stable_Init_Partition() error {
	if ls.ShardNum > len(ls.NetGraph.VertexSet) {
		return errors.New("too many shards, number of shards should be less than nodes")
	}

	ls.VertexsNumInShard = make([]int, ls.ShardNum)
	ls.PartitionMap = make(map[Vertex]int)

	cnt := 0
	for v := range ls.NetGraph.VertexSet {
		ls.PartitionMap[v] = cnt % ls.ShardNum
		ls.VertexsNumInShard[ls.PartitionMap[v]] += 1
		cnt++
	}
	ls.ComputeCommunityStats()
	return nil
}

// 获取节点v连向哪些分片，以及分别有多少条边
// 这里是无权图，所以“边权”就是边数
func (ls *LouvainState) NeighborShardEdges(v Vertex) map[int]int {
	res := make(map[int]int)
	for _, u := range ls.NetGraph.EdgeSet[v] {
		uShard := ls.PartitionMap[u]
		res[uShard] += 1
	}
	return res
}

// 负载均衡惩罚：按“节点数偏离平均值”的程度计算
func (ls *LouvainState) BalancePenaltyAfterInsert(targetShard int) float64 {
	if ls.ShardNum == 0 {
		return 0
	}
	avg := float64(len(ls.NetGraph.VertexSet)) / float64(ls.ShardNum)
	if avg == 0 {
		return 0
	}
	sizeAfter := float64(ls.VertexsNumInShard[targetShard] + 1)
	return math.Abs(sizeAfter-avg) / avg
}

// 计算将节点v移入targetShard的增益
// 这里使用“Louvain局部移动”的简化比较形式：
// gain = k_i_in(C) - (k_i * Σ_tot(C)) / (2m) - beta * penalty
//
// 其中：
// k_i_in(C) = v与目标分片C之间的边数
// k_i       = v的度
// Σ_tot(C)  = 目标分片总度
// m         = 图总边数
//
// 说明：这里省略了统一的归一化常数，因为只用于比较大小。
func (ls *LouvainState) getShard_gain(v Vertex, targetShard int, edgesToTarget int) float64 {
	kv := float64(len(ls.NetGraph.EdgeSet[v]))
	m := ls.TotalEdgeWeight

	if kv == 0 || m == 0 {
		return -1e18
	}

	modularityGain := float64(edgesToTarget) - (kv*ls.CommTot[targetShard])/(2.0*m)
	penalty := ls.BalancePenalty * ls.BalancePenaltyAfterInsert(targetShard)

	return modularityGain - penalty
}

// 在节点v从old分片迁移到new分片后，增量更新跨分片边数
func (ls *LouvainState) changeShardRecompute(v Vertex, old int) {
	newShard := ls.PartitionMap[v]

	for _, u := range ls.NetGraph.EdgeSet[v] {
		neighborShard := ls.PartitionMap[u]

		// 原来 old-new 是跨分片，现在 new-new 不是跨分片
		if neighborShard == newShard {
			ls.CrossShardEdgeNum--
		} else if neighborShard == old {
			// 原来 old-old 不是跨分片，现在 new-old 是跨分片
			ls.CrossShardEdgeNum++
		}
		// 其他分片 old-x -> new-x，仍然都是跨分片，不变
	}
}

// 固定K的局部Louvain划分算法
// 注意：
// 1. 这不是“标准两阶段多层Louvain”
// 2. 它更像“用Louvain增益替换CLPA打分函数”
// 3. 特别适合你当前分片场景做第一版
func (ls *LouvainState) Louvain_Partition() (map[string]uint64, int) {
	ls.ComputeCommunityStats()
	logger.Println("Before running Louvain, cross-shard edge number:", ls.CrossShardEdgeNum)
	res := make(map[string]uint64)
	updateThreshold := make(map[string]int)

	for iter := 0; iter < ls.MaxIterations; iter++ {
		moved := false

		for v := range ls.NetGraph.VertexSet {
			// 反震荡机制：如果某个节点迁移过多次，就不再考虑
			if updateThreshold[v.Addr] >= 50 {
				continue
			}

			kv := len(ls.NetGraph.EdgeSet[v])
			if kv == 0 {
				continue
			}

			oldShard := ls.PartitionMap[v]

			// 防止产生空分片
			if ls.VertexsNumInShard[oldShard] <= 1 {
				continue
			}

			neighborShardEdges := ls.NeighborShardEdges(v)

			// 临时把v从原分片“移出”，这样计算gain时更合理
			ls.VertexsNumInShard[oldShard]--
			ls.CommTot[oldShard] -= float64(kv)

			bestShard := oldShard
			bestGain := 0.0 // 只有增益为正才迁移

			for shard, edgesToShard := range neighborShardEdges {
				if shard == oldShard {
					continue
				}
				gain := ls.getShard_gain(v, shard, edgesToShard)
				if gain > bestGain {
					bestGain = gain
					bestShard = shard
				}
			}

			// 放回某个分片（原地或新分片）
			ls.PartitionMap[v] = bestShard
			ls.VertexsNumInShard[bestShard]++
			ls.CommTot[bestShard] += float64(kv)

			if bestShard != oldShard {
				res[v.Addr] = uint64(bestShard)
				updateThreshold[v.Addr]++
				moved = true

				// 增量更新跨分片边数
				ls.changeShardRecompute(v, oldShard)
			} else {
				ls.PartitionMap[v] = oldShard
			}
		}

		// 如果这一轮没有任何节点迁移，提前结束
		if !moved {
			break
		}
	}

	for sid, n := range ls.VertexsNumInShard {
		logger.Println("has vertexs: ", sid, n)
	}

	ls.ComputeCommunityStats()
	logger.Println("After running Louvain, cross-shard edge number:", ls.CrossShardEdgeNum)
	return res, ls.CrossShardEdgeNum
}

func (ls *LouvainState) EraseEdges() {
	ls.NetGraph.EdgeSet = make(map[Vertex][]Vertex)
}
