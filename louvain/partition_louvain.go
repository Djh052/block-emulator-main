package louvain

import (
	"blockEmulator/utils"
	"fmt"
)

// type Vertex struct {
// 	Addr string // 账户地址
// 	// 其他属性待补充
// }

type PLouvainState struct {
	PLGraphReader *GraphReader      // 读取图的工具
	NetGraph      Graph             // 需运行PLouvain算法的图
	PartitionMap  map[string]uint64 // 记录分片信息的 map，某个节点属于哪个分片
	PLouvain      *Louvain          //louvain

	secStageMoveNode bool //默认值false，In the second stage, move community [false] or node [true] to shard
	isDeterministic  bool //默认值false，Use non-deterministic[false] or deterministic algorithm[true] algorithm
	isTxAllo         bool //默认值false，Don't use[false] or use[true] workflows of TxAllo
	israndom         bool //是否随机

	shardNum          int       //分片数目
	VertexsNumInShard []int     // Shard 内节点的数目
	shardPerformance  []float32 //分片性能
	beta              float32   //权重惩罚
}

func (pl *PLouvainState) Init_PLouvainState(beta float32, shardNum int, shardPerformance []float32,
	secStageMoveNode bool, isDeterministic bool, isTxAllo bool, israndom bool) {
	pl.PLGraphReader = NewGraphReader()
	pl.PartitionMap = make(map[string]uint64)
	pl.secStageMoveNode = secStageMoveNode
	pl.isDeterministic = isDeterministic
	pl.isTxAllo = isTxAllo
	pl.israndom = israndom

	pl.shardNum = shardNum
	pl.VertexsNumInShard = make([]int, shardNum)
	pl.shardPerformance = shardPerformance
	pl.beta = beta
}

// 增加节点
func (pl *PLouvainState) AddVertex(v string) {
	pl.PLGraphReader.addNode(v)
	if val, ok := pl.PartitionMap[v]; !ok {
		pl.PartitionMap[v] = uint64(utils.Addr2Shard(v))
	} else {
		pl.PartitionMap[v] = val
	}
	pl.VertexsNumInShard[pl.PartitionMap[v]] += 1
}

// 增加边
func (pl *PLouvainState) AddEdge(u, v string) {
	if _, ok := pl.PLGraphReader.nodeLabelToIndex[u]; !ok {
		pl.AddVertex(u)
	}
	if _, ok := pl.PLGraphReader.nodeLabelToIndex[v]; !ok {
		pl.AddVertex(v)
	}
	pl.PLGraphReader.addEdge(u, v)
}

// 使用PLouvain算法进行分片，返回分片结果
func (pl *PLouvainState) PLouvain_Partition() map[string]uint64 {
	pl.NetGraph = pl.PLGraphReader.GenerateGraph()

	//步骤一：用模块度Q进行社区划分
	fmt.Println("---------start the first stage : community partition--------------")
	pl.PLouvain = NewLouvain(pl.NetGraph, pl.shardPerformance, pl.israndom, pl.beta, pl.shardNum)
	var isModularity bool = true
	pl.PLouvain.Compute(isModularity, pl.isDeterministic, pl.isTxAllo) //社区划分

	//打印社区划分结果
	fmt.Printf("Number of nodes: %d\n", pl.NetGraph.GetNodeSize())
	fmt.Printf("Number of communities: %d\n", pl.PLouvain.GetCommunitiesNum())
	_, nodeNum := pl.PLouvain.GetBestPertition()
	allnodeNum := 0
	for _, nodeNumber := range nodeNum {
		allnodeNum += nodeNumber
	}
	pl.PLouvain.PrintGraphWeight() //(totalWeight-edgeWeight)/2+edgeWeight=graph.txNum即正确

	nodeToShard := make([]int, pl.NetGraph.GetNodeSize())
	//步骤二：将社区分配到分片
	if pl.secStageMoveNode == false {
		fmt.Println("---------start the second stage with pattern 1(comm to shard)----------")
		pl.PLouvain.CommToShard(pl.PLouvain.Getbeta())
		_, nodeToShard = pl.PLouvain.UpdateNodeShardIndex()
	} else {
		fmt.Println("---------start the second stage with pattern 2(node to shard)----------")
		nodeToShard = pl.PLouvain.NodeToShard(pl.beta)
	}

	pl.PLouvain.PrintShard()
	//自我验证
	//pl.PLouvain.VerifyMyResult(nodeToShard)
	//步骤三：按处理时间判断节点是否移动
	result := true
	maxIterationNum := 70 //最大迭代次数
	if pl.isDeterministic {
		fmt.Printf("[start deterministic third atage : node Merge]\n")
	} else if pl.isTxAllo {
		fmt.Printf("[start use the stage of TXALLo in third atage : node Merge]\n")
	} else {
		fmt.Printf("[start non-deterministic third atage : node Merge]\n")
	}
	for i := 1; result && i <= maxIterationNum; i++ {
		// fmt.Println("---------start the third stage node Merge: ", i, " times---------")
		result, nodeToShard = pl.PLouvain.NodeMerge(nodeToShard, pl.isDeterministic, pl.isTxAllo)
	}
	//输出结果，可删
	pl.PLouvain.PrintShard()
	//自我验证
	pl.PLouvain.VerifyMyResult(nodeToShard)

	//步骤四：将结果返回并重置图
	for i := 0; i < len(nodeToShard); i++ {
		shardId := nodeToShard[i]
		nodeLabel := pl.PLGraphReader.GetNodeLabel(i)
		pl.PartitionMap[nodeLabel] = uint64(shardId)
	}
	pl.PLGraphReader.ResetGraph()

	return pl.PartitionMap
}

// 在有交易数据已经建图的情况下用于验证别人的结果
func (pl *PLouvainState) VerifyOtherResult(PartitionMap map[string]uint64) (isSuccess bool) {
	fmt.Println("----------start verify algorithm (others' result)----------------")
	// verifyGraph := pl.PLGraphReader.GenerateGraph()
	//以len(PartitionMap)建一个数组，名为nodeToShard
	nodeToShard := make([]int, len(PartitionMap))
	//将PartitionMap中的值赋给nodeToShard
	for key := range PartitionMap {
		if index, ok := pl.PLGraphReader.nodeLabelToIndex[key]; !ok {
			fmt.Println("error: the node is not in the PLGraphReader")
		} else {
			nodeToShard[index] = int(PartitionMap[key])
		}
	}

	verifyLouvain, borderNode := VerifyNewLouvain(pl.NetGraph, pl.shardPerformance, pl.beta, pl.shardNum, nodeToShard)
	verifyResult, errorNodeAndShard := verifyLouvain.Verify(nodeToShard, borderNode)
	if verifyResult {
		fmt.Println("[verify success]")
		return true
	} else {
		fmt.Println("verify fail")
		if len(errorNodeAndShard) != 0 {
			fmt.Printf("nodeId %d was allocated to error Shard: %d, correntShard: %d \n", errorNodeAndShard[0], errorNodeAndShard[1], errorNodeAndShard[2])
			fmt.Println("the node(account)'s address is: ", pl.PLGraphReader.GetNodeLabel(errorNodeAndShard[0]))
		}
		return false
	}
}
