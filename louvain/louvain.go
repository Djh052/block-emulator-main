package louvain

import (
	"fmt"
	"math"
)

type Community struct {
	inWeight    WeightType //内部权重2倍加上自环边1倍
	totalWeight WeightType //等于inWeight+连接外部节点的权重
	inTXnum     int        //社区内部交易数量
	exTXnum     int        //社区外部交易数量
	inShard     int        //所属分片
	nodeNum     int        //社区拥有的账户数量
}

type Level struct {
	graph         Graph       //当前层级的图。
	communities   []Community //社区的社区信息
	inCommunities []int       //节点所属社区
}

type Shard struct {
	nodeNumInShard int     //分片内的账户/节点总数。
	performance    float32 //处理能力（默认相等1）
	processTime    float32 //分片的处理性能（TPS 或算力）
	inTXnum        int     //分片内交易数量（无需跨片的交易）
	exTXnum        int     //跨分片交易数量
}
type Louvain struct {
	level    []Level
	current  *Level
	m2       WeightType
	shard    []Shard
	shardNum int
	beta     float32 //跨分片交易相对片内交易的权重系数
}

func NewLouvain(graph Graph, shardPerformance []float32, random bool, beta float32, shardNum int) *Louvain {
	louvain := Louvain{}
	louvain.level = make([]Level, 1)
	louvain.current = &louvain.level[0]
	louvain.current.graph = graph
	louvain.current.communities = make([]Community, louvain.current.graph.GetNodeSize())
	louvain.current.inCommunities = make([]int, louvain.current.graph.GetNodeSize())
	louvain.m2 = WeightType(0.0)
	louvain.shard = make([]Shard, shardNum)
	louvain.shardNum = shardNum
	louvain.beta = beta
	for i := 0; i < louvain.shardNum; i++ {
		louvain.shard[i].performance = shardPerformance[i]
	}
	var round int = 0
	for nodeId := 0; nodeId < louvain.current.graph.GetNodeSize(); nodeId++ {
		louvain.current.inCommunities[nodeId] = nodeId
		neigh := WeightType(0.0)
		for _, edge := range louvain.current.graph.GetIncidentEdges(nodeId) {
			neigh += edge.weight
		}
		self := WeightType(louvain.current.graph.GetSelfWeight(nodeId))
		if !random { //先不分配分片，模块度社区划分后再分配分片
			louvain.current.communities[nodeId] = Community{self, neigh + self, 0, int(neigh), -1, 1}
		} else { //随机均匀分配到各分片
			if len(louvain.shard) == 0 {
				fmt.Println("shard number is 0,err!")
			} else {
				louvain.current.communities[nodeId] = Community{self, neigh + self, 0, int(neigh), round, 1}
				round = (round + 1) % shardNum
			}
		}
		louvain.m2 += (neigh + 2*self)
	}
	return &louvain
}

// func (this *Louvain) BestModularity() WeightType {
// 	return this.Modularity(len(this.level) - 1)
// }

// func (this *Louvain) Modularity(level int) WeightType {
// 	return this.level[level].modularity(this.m2)
// }

// func (this *Level) modularity(m2 WeightType) WeightType {
// 	q := WeightType(0.0)
// 	for _, comm := range this.communities {
// 		q += comm.inWeight/m2 - (comm.totalWeight/m2)*(comm.totalWeight/m2)
// 	}
// 	return q
// }

// 合并社区(一次层级内社区合并，要移动的节点的所属分片标签inCommunity变为移动到的分片的分片编号)
func (this *Louvain) Merge(isDeterministic bool, isTxAllo bool) bool {
	improved := false
	q := make([]int, this.current.graph.GetNodeSize())
	mark := make([]bool, this.current.graph.GetNodeSize())
	for nodeId := 0; nodeId < this.current.graph.GetNodeSize(); nodeId++ {
		q[nodeId] = nodeId
	}
	if isTxAllo {
		fmt.Printf("[In Merge() use workflows of TxAllo in first atage : louvain]\n")
		unProcessed := this.current.graph.GetNodeSize() //确定性
		//for len(q) != 0 { //q为判断队列，对所有节点进行操作，加入到deltaQ最大的社区//非确定性
		for i := 0; unProcessed != 0; i = (i + 1) % len(q) { //确定性
			// nodeId := q[0]//非确定性
			// q = q[1:] // pop_front，弹出一个节点进行操作，非确定性
			nodeId := i //确定性

			mark[nodeId] = true
			neighWeights := map[int]WeightType{}
			self := WeightType(this.current.graph.GetSelfWeight(nodeId)) //社区内部的权重
			totalWeight := self
			//neighWeightsKeys存邻居节点所在社区
			neighWeightsKeys := make([]int, 0, len(neighWeights))
			for _, edge := range this.current.graph.GetIncidentEdges(nodeId) {
				destCommId := this.current.inCommunities[edge.destId]
				if _, exists := neighWeights[destCommId]; !exists {
					neighWeightsKeys = append(neighWeightsKeys, destCommId)
				}
				neighWeights[destCommId] += edge.weight
				totalWeight += edge.weight
			}

			prevCommunity := this.current.inCommunities[nodeId]
			prevNeighWeight := WeightType(neighWeights[prevCommunity])
			this.remove(nodeId, prevCommunity, 2*prevNeighWeight+self, totalWeight)

			maxInc := WeightType(0.0) //max_detaQ
			bestCommunity := prevCommunity
			bestNeighWeight := WeightType(prevNeighWeight) //指与当前节点相连的社区的权重
			for _, community := range neighWeightsKeys {
				weight := neighWeights[community]
				inc := WeightType(weight - this.current.communities[community].totalWeight*totalWeight/this.m2) //detaQ
				if inc > maxInc {
					maxInc = inc
					bestCommunity = community
					bestNeighWeight = weight
				}
			}
			this.insert(nodeId, bestCommunity, 2*bestNeighWeight+self, totalWeight)
			if bestCommunity != prevCommunity {
				improved = true
				this.removeTX(nodeId, prevCommunity, int(prevNeighWeight), int(self), int(totalWeight))
				this.insertTX(nodeId, bestCommunity, int(bestNeighWeight), int(self), int(totalWeight))
				for _, edge := range this.current.graph.GetIncidentEdges(nodeId) { //移动节点的邻居需要重新判断deltaQ，因此加入判断队列q
					if mark[edge.destId] {
						//q = append(q, edge.destId)//非确定性
						unProcessed++ //确定性
						mark[edge.destId] = false
					}
				}
			}
			unProcessed-- //确定性
		}
	} else {
		if isDeterministic {
			fmt.Println("[In Merge(): isDeterministic=true]")
			unProcessed := this.current.graph.GetNodeSize() //确定性
			//for len(q) != 0 { //q为判断队列，对所有节点进行操作，加入到deltaQ最大的社区//非确定性
			roundImprovedInc := WeightType(0.0)
			for i := 0; unProcessed != 0; i = (i + 1) % len(q) { //确定性
				if !mark[i] {
					// nodeId := q[0]//非确定性
					// q = q[1:] // pop_front，弹出一个节点进行操作，非确定性
					nodeId := i //确定性

					mark[nodeId] = true

					neighWeights := map[int]WeightType{}
					self := WeightType(this.current.graph.GetSelfWeight(nodeId)) //社区内部的权重
					totalWeight := self
					//neighWeightsKeys存邻居节点所在社区
					neighWeightsKeys := make([]int, 0, len(neighWeights))
					for _, edge := range this.current.graph.GetIncidentEdges(nodeId) {
						destCommId := this.current.inCommunities[edge.destId]
						if _, exists := neighWeights[destCommId]; !exists {
							neighWeightsKeys = append(neighWeightsKeys, destCommId)
						}
						neighWeights[destCommId] += edge.weight
						totalWeight += edge.weight
					}

					prevCommunity := this.current.inCommunities[nodeId]
					prevNeighWeight := WeightType(neighWeights[prevCommunity])
					this.remove(nodeId, prevCommunity, 2*prevNeighWeight+self, totalWeight)

					maxInc := WeightType(0.0) //max_detaQ
					bestCommunity := prevCommunity
					bestNeighWeight := WeightType(prevNeighWeight) //指与当前节点相连的社区的权重
					toPreCommInc := WeightType(prevNeighWeight - this.current.communities[prevCommunity].totalWeight*totalWeight/this.m2)
					for _, community := range neighWeightsKeys {
						weight := neighWeights[community]
						inc := WeightType(weight - this.current.communities[community].totalWeight*totalWeight/this.m2) //detaQ
						if inc > maxInc {
							maxInc = inc
							bestCommunity = community
							bestNeighWeight = weight
						}
					}
					this.insert(nodeId, bestCommunity, 2*bestNeighWeight+self, totalWeight)
					if bestCommunity != prevCommunity {
						improved = true
						roundImprovedInc += (toPreCommInc - maxInc)
						this.removeTX(nodeId, prevCommunity, int(prevNeighWeight), int(self), int(totalWeight))
						this.insertTX(nodeId, bestCommunity, int(bestNeighWeight), int(self), int(totalWeight))
						for _, edge := range this.current.graph.GetIncidentEdges(nodeId) { //移动节点的邻居需要重新判断deltaQ，因此加入判断队列q
							if mark[edge.destId] {
								//q = append(q, edge.destId)//非确定性
								unProcessed++ //确定性
								mark[edge.destId] = false
							}
						}
					}
					unProcessed-- //确定性
				}
				if i+1 == len(q) {
					if roundImprovedInc > WeightType(0.0) {
						roundImprovedInc = WeightType(0.0)
					} else {
						break
					}
				}

				//fmt.Printf("%d,", unProcessed)
			}
			//fmt.Println()
		} else {
			fmt.Printf("[start undeterministic first atage : louvain]\n")
			//unProcessed := this.current.graph.GetNodeSize() //确定性
			for len(q) != 0 { //q为判断队列，对所有节点进行操作，加入到deltaQ最大的社区//非确定性
				//for i := 0; !mark[i] && unProcessed != 0; i = (i + 1) % len(q) { //确定性
				nodeId := q[0] //非确定性
				q = q[1:]      // pop_front，弹出一个节点进行操作，非确定性
				//nodeId := i //确定性

				mark[nodeId] = true

				neighWeights := map[int]WeightType{}
				self := WeightType(this.current.graph.GetSelfWeight(nodeId)) //社区内部的权重
				totalWeight := self
				//neighWeightsKeys存邻居节点所在社区
				neighWeightsKeys := make([]int, 0, len(neighWeights))
				for _, edge := range this.current.graph.GetIncidentEdges(nodeId) {
					destCommId := this.current.inCommunities[edge.destId]
					if _, exists := neighWeights[destCommId]; !exists {
						neighWeightsKeys = append(neighWeightsKeys, destCommId)
					}
					neighWeights[destCommId] += edge.weight
					totalWeight += edge.weight
				}

				prevCommunity := this.current.inCommunities[nodeId]
				prevNeighWeight := WeightType(neighWeights[prevCommunity])
				this.remove(nodeId, prevCommunity, 2*prevNeighWeight+self, totalWeight)

				maxInc := WeightType(0.0) //max_detaQ
				bestCommunity := prevCommunity
				bestNeighWeight := WeightType(prevNeighWeight) //指与当前节点相连的社区的权重
				for _, community := range neighWeightsKeys {
					weight := neighWeights[community]
					inc := WeightType(weight - this.current.communities[community].totalWeight*totalWeight/this.m2) //detaQ
					if inc > maxInc {
						maxInc = inc
						bestCommunity = community
						bestNeighWeight = weight
					}
				}
				this.insert(nodeId, bestCommunity, 2*bestNeighWeight+self, totalWeight)
				if bestCommunity != prevCommunity {
					improved = true
					this.removeTX(nodeId, prevCommunity, int(prevNeighWeight), int(self), int(totalWeight))
					this.insertTX(nodeId, bestCommunity, int(bestNeighWeight), int(self), int(totalWeight))
					for _, edge := range this.current.graph.GetIncidentEdges(nodeId) { //移动节点的邻居需要重新判断deltaQ，因此加入判断队列q
						if mark[edge.destId] {
							q = append(q, edge.destId) //非确定性
							//unProcessed++ //确定性
							mark[edge.destId] = false
						}
					}
				}
				//unProcessed-- //确定性
			}
		}
	}

	return improved
}
func (this *Louvain) insert(nodeId int, community int, inWeight WeightType, totalWeight WeightType) {
	this.current.inCommunities[nodeId] = community
	this.current.communities[community].inWeight += inWeight
	this.current.communities[community].totalWeight += totalWeight
}

func (this *Louvain) insertTX(nodeId int, community int, bestNeighWeight int, self int, totalWeight int) {
	this.current.communities[community].inTXnum = int(this.current.communities[community].inWeight) / 2
	this.current.communities[community].exTXnum = int(this.current.communities[community].totalWeight - this.current.communities[community].inWeight)
}

func (this *Louvain) remove(nodeId int, community int, inWeight WeightType, totalWeight WeightType) {
	this.current.inCommunities[nodeId] = -1
	this.current.communities[community].inWeight -= inWeight
	this.current.communities[community].totalWeight -= totalWeight
}

func (this *Louvain) removeTX(nodeId int, community int, prevNeighWeight int, self int, totalWeight int) {
	this.current.communities[community].inTXnum = int(this.current.communities[community].inWeight) / 2
	this.current.communities[community].exTXnum = int(this.current.communities[community].totalWeight - this.current.communities[community].inWeight)
}

// 第三步：循环遍历所有节点判断是否移动到隔壁分片可以降低处理时间较大值，
// 如果可以降低则移动到隔壁分片，直到没有节点移动
// 模式一的情况下是否先判断处理时间长的分片的节点会快一些？
func (this *Louvain) NodeMerge(nodeToShard []int, isDeterministic bool, isTxAllo bool) (bool, []int) {
	improved := false
	this.current = &this.level[0]
	newNodeToShard := nodeToShard

	q := make([]int, this.current.graph.GetNodeSize())
	mark := make([]bool, this.current.graph.GetNodeSize())

	for nodeId := 0; nodeId < this.current.graph.GetNodeSize(); nodeId++ {
		q[nodeId] = nodeId
		this.current.inCommunities[nodeId] = nodeId
		this.current.communities[nodeId] = Community{WeightType(this.current.graph.GetSelfWeight(nodeId)), WeightType(this.current.graph.GetSelfWeight(nodeId)), 0, 0, newNodeToShard[nodeId], 1}
	}
	if isTxAllo {
		fmt.Println("[In Merge(): isTxAllo=true]")
		unProcessed := this.current.graph.GetNodeSize() //确定性
		//for len(q) != 0 { //q为判断队列，对所有节点进行操作，加入到deltaQ最大的社区，非确定性
		for i := 0; unProcessed != 0; i = (i + 1) % len(q) { //确定性
			// nodeId := q[0]//非确定性
			// q = q[1:] // pop_front，弹出一个节点进行操作//非确定性
			nodeId := i //确定性
			mark[nodeId] = true
			self := WeightType(this.current.graph.GetSelfWeight(nodeId)) //社区内部的权重
			totalWeight := self
			if self != WeightType(0.0) {
				fmt.Println("err!!!!!!!!!!!!In NodeMerge(): self!=WeightType(0.0)")
			}
			neighShardList := make([]bool, this.shardNum)
			neighShardTxNum := make([]int, this.shardNum)
			myShard := newNodeToShard[nodeId]
			for _, edge := range this.current.graph.GetIncidentEdges(nodeId) {
				totalWeight += edge.weight
				neighShard := newNodeToShard[edge.destId]
				if myShard != neighShard && !neighShardList[neighShard] {
					neighShardList[neighShard] = true
				}
				neighShardTxNum[neighShard] += int(edge.weight)
			}

			//以处理时间为依据判断是否移动节点，
			bestShard := myShard
			myShardProcessTime := this.shard[myShard].processTime
			myShardInTxNum := this.shard[myShard].inTXnum
			myShardExTxNum := this.shard[myShard].exTXnum
			bestShardProcesstime := this.shard[myShard].processTime
			bestShardInTxNum := this.shard[myShard].inTXnum
			bestShardExTxNum := this.shard[myShard].exTXnum
			maskShard := make([]int, len(this.shard))
			var maxDifference float32 = 0.0
			for index := 0; index < len(neighShardList); index++ {
				if neighShardList[index] { //内部有nodeId邻居的分片
					//判断不在一个分片的邻居节点
					neighShard := index
					if myShard != neighShard && maskShard[neighShard] == 0 {
						//计算节点移动到邻居所在社区后，两个分片的处理时间的最大值是否下降，
						nodeToMyShardTXnum := neighShardTxNum[myShard]
						nodeToOtherShardTXnum := int(totalWeight-self) - nodeToMyShardTXnum
						sourceShardProcessTime := this.shard[myShard].processTime
						destShardProcessTime := this.shard[neighShard].processTime
						var biggerProcessTime float32 = 0.0
						if sourceShardProcessTime > destShardProcessTime {
							biggerProcessTime = sourceShardProcessTime
						} else {
							biggerProcessTime = destShardProcessTime
						}
						newSourceExTXnum := this.shard[myShard].exTXnum - nodeToOtherShardTXnum + nodeToMyShardTXnum
						newSourceInTXnum := this.shard[myShard].inTXnum - nodeToMyShardTXnum - int(self)
						newSourceProcessTime := (float32(newSourceInTXnum) + this.beta*float32(newSourceExTXnum)) / this.shard[myShard].performance
						//计算节点移动到邻居所在社区后，目标分片的处理时间
						nodeToDestShardTXnum := neighShardTxNum[neighShard]
						nodeToNoDestShardTXnum := int(totalWeight-self) - nodeToDestShardTXnum
						newDestExTXnum := this.shard[neighShard].exTXnum + nodeToNoDestShardTXnum - nodeToDestShardTXnum
						newDestInTXnum := this.shard[neighShard].inTXnum + nodeToDestShardTXnum + int(self)
						newDestProcessTime := (float32(newDestInTXnum) + this.beta*float32(newDestExTXnum)) / this.shard[neighShard].performance
						var newBiggerProcessTime float32 = 0.0
						if newSourceProcessTime > newDestProcessTime {
							newBiggerProcessTime = newSourceProcessTime
						} else {
							newBiggerProcessTime = newDestProcessTime
						}
						differ := biggerProcessTime - newBiggerProcessTime
						//如果移动节点后，两个分片的处理时间的最大值下降，则移动节点到邻居所在社区
						if differ-maxDifference > 0 { //迁移阈值
							//移动节点到邻居所在社区
							maxDifference = differ
							bestShard = neighShard
							myShardProcessTime = newSourceProcessTime
							myShardInTxNum = newSourceInTXnum
							myShardExTxNum = newSourceExTXnum
							bestShardInTxNum = newDestInTXnum
							bestShardExTxNum = newDestExTXnum
							bestShardProcesstime = newDestProcessTime
						}
						maskShard[neighShard] = 1
					}
				}
			}
			if bestShard != myShard { //节点移动后更新分片的处理时间、节点所属分片
				this.shard[bestShard].inTXnum = bestShardInTxNum
				this.shard[bestShard].exTXnum = bestShardExTxNum
				this.shard[bestShard].processTime = bestShardProcesstime
				this.shard[bestShard].nodeNumInShard += 1
				this.shard[myShard].inTXnum = myShardInTxNum
				this.shard[myShard].exTXnum = myShardExTxNum
				this.shard[myShard].processTime = myShardProcessTime
				this.shard[myShard].nodeNumInShard -= 1
				newNodeToShard[nodeId] = bestShard
				improved = true
				for _, edge := range this.current.graph.GetIncidentEdges(nodeId) { //移动节点的邻居需要重新判断deltaQ，因此加入判断队列q
					if mark[edge.destId] {
						//q = append(q, edge.destId)//非确定性
						unProcessed++ //确定性
						mark[edge.destId] = false
					}
				}
			}
			unProcessed-- //确定性
		}
	} else {
		if isDeterministic {
			fmt.Println("[In Merge(): isDeterministic=true]")
			unProcessed := this.current.graph.GetNodeSize() //确定性
			//for len(q) != 0 { //q为判断队列，对所有节点进行操作，加入到deltaQ最大的社区，非确定性
			for i := 0; unProcessed != 0; i = (i + 1) % len(q) { //确定性
				if !mark[i] {
					// nodeId := q[0]//非确定性
					// q = q[1:] // pop_front，弹出一个节点进行操作//非确定性
					nodeId := i //确定性
					mark[nodeId] = true
					self := WeightType(this.current.graph.GetSelfWeight(nodeId)) //社区内部的权重
					totalWeight := self
					if self != WeightType(0.0) {
						fmt.Println("err!!!!!!!!!!!!In NodeMerge(): self!=WeightType(0.0)")
					}
					neighShardList := make([]bool, this.shardNum)
					neighShardTxNum := make([]int, this.shardNum)
					myShard := newNodeToShard[nodeId]
					for _, edge := range this.current.graph.GetIncidentEdges(nodeId) {
						totalWeight += edge.weight
						neighShard := newNodeToShard[edge.destId]
						if myShard != neighShard && !neighShardList[neighShard] {
							neighShardList[neighShard] = true
						}
						neighShardTxNum[neighShard] += int(edge.weight)
					}

					//以处理时间为依据判断是否移动节点，
					bestShard := myShard
					myShardProcessTime := this.shard[myShard].processTime
					myShardInTxNum := this.shard[myShard].inTXnum
					myShardExTxNum := this.shard[myShard].exTXnum
					bestShardProcesstime := this.shard[myShard].processTime
					bestShardInTxNum := this.shard[myShard].inTXnum
					bestShardExTxNum := this.shard[myShard].exTXnum
					maskShard := make([]int, len(this.shard))
					var maxDifference float32 = 0.0
					for index := 0; index < len(neighShardList); index++ {
						if neighShardList[index] { //内部有nodeId邻居的分片
							//判断不在一个分片的邻居节点
							neighShard := index
							if myShard != neighShard && maskShard[neighShard] == 0 {
								//计算节点移动到邻居所在社区后，两个分片的处理时间的最大值是否下降，
								nodeToMyShardTXnum := neighShardTxNum[myShard]
								nodeToOtherShardTXnum := int(totalWeight-self) - nodeToMyShardTXnum
								sourceShardProcessTime := this.shard[myShard].processTime
								destShardProcessTime := this.shard[neighShard].processTime
								var biggerProcessTime float32 = 0.0
								if sourceShardProcessTime > destShardProcessTime {
									biggerProcessTime = sourceShardProcessTime
								} else {
									biggerProcessTime = destShardProcessTime
								}
								newSourceExTXnum := this.shard[myShard].exTXnum - nodeToOtherShardTXnum + nodeToMyShardTXnum
								newSourceInTXnum := this.shard[myShard].inTXnum - nodeToMyShardTXnum - int(self)
								newSourceProcessTime := (float32(newSourceInTXnum) + this.beta*float32(newSourceExTXnum)) / this.shard[myShard].performance
								//计算节点移动到邻居所在社区后，目标分片的处理时间
								nodeToDestShardTXnum := neighShardTxNum[neighShard]
								nodeToNoDestShardTXnum := int(totalWeight-self) - nodeToDestShardTXnum
								newDestExTXnum := this.shard[neighShard].exTXnum + nodeToNoDestShardTXnum - nodeToDestShardTXnum
								newDestInTXnum := this.shard[neighShard].inTXnum + nodeToDestShardTXnum + int(self)
								newDestProcessTime := (float32(newDestInTXnum) + this.beta*float32(newDestExTXnum)) / this.shard[neighShard].performance
								var newBiggerProcessTime float32 = 0.0
								if newSourceProcessTime > newDestProcessTime {
									newBiggerProcessTime = newSourceProcessTime
								} else {
									newBiggerProcessTime = newDestProcessTime
								}
								differ := biggerProcessTime - newBiggerProcessTime
								//如果移动节点后，两个分片的处理时间的最大值下降，则移动节点到邻居所在社区
								if differ-maxDifference > 0 { //迁移阈值
									//移动节点到邻居所在社区
									maxDifference = differ
									bestShard = neighShard
									myShardProcessTime = newSourceProcessTime
									myShardInTxNum = newSourceInTXnum
									myShardExTxNum = newSourceExTXnum
									bestShardInTxNum = newDestInTXnum
									bestShardExTxNum = newDestExTXnum
									bestShardProcesstime = newDestProcessTime
								}
								maskShard[neighShard] = 1
							}
						}
					}
					if bestShard != myShard { //节点移动后更新分片的处理时间、节点所属分片
						this.shard[bestShard].inTXnum = bestShardInTxNum
						this.shard[bestShard].exTXnum = bestShardExTxNum
						this.shard[bestShard].processTime = bestShardProcesstime
						this.shard[bestShard].nodeNumInShard += 1
						this.shard[myShard].inTXnum = myShardInTxNum
						this.shard[myShard].exTXnum = myShardExTxNum
						this.shard[myShard].processTime = myShardProcessTime
						this.shard[myShard].nodeNumInShard -= 1
						newNodeToShard[nodeId] = bestShard
						improved = true
						for _, edge := range this.current.graph.GetIncidentEdges(nodeId) { //移动节点的邻居需要重新判断deltaQ，因此加入判断队列q
							if mark[edge.destId] {
								//q = append(q, edge.destId)//非确定性
								unProcessed++ //确定性
								mark[edge.destId] = false
							}
						}
					}
					unProcessed-- //确定性
				}

			}
		} else {
			//unProcessed := this.current.graph.GetNodeSize() //确定性
			for len(q) != 0 { //q为判断队列，对所有节点进行操作，加入到deltaQ最大的社区，非确定性
				// for i := 0; !mark[i] && unProcessed != 0; i = (i + 1) % len(q) { //确定性
				nodeId := q[0] //非确定性
				q = q[1:]      // pop_front，弹出一个节点进行操作//非确定性
				// nodeId := i //确定性
				mark[nodeId] = true
				self := WeightType(this.current.graph.GetSelfWeight(nodeId)) //社区内部的权重
				totalWeight := self
				if self != WeightType(0.0) {
					fmt.Println("err!!!!!!!!!!!!In NodeMerge(): self!=WeightType(0.0)")
				}
				neighShardList := make([]bool, this.shardNum)
				neighShardTxNum := make([]int, this.shardNum)
				myShard := newNodeToShard[nodeId]
				for _, edge := range this.current.graph.GetIncidentEdges(nodeId) {
					totalWeight += edge.weight
					neighShard := newNodeToShard[edge.destId]
					if myShard != neighShard && !neighShardList[neighShard] {
						neighShardList[neighShard] = true
					}
					neighShardTxNum[neighShard] += int(edge.weight)
				}

				//以处理时间为依据判断是否移动节点，
				bestShard := myShard
				myShardProcessTime := this.shard[myShard].processTime
				myShardInTxNum := this.shard[myShard].inTXnum
				myShardExTxNum := this.shard[myShard].exTXnum
				bestShardProcesstime := this.shard[myShard].processTime
				bestShardInTxNum := this.shard[myShard].inTXnum
				bestShardExTxNum := this.shard[myShard].exTXnum
				maskShard := make([]int, len(this.shard))
				var maxDifference float32 = 0.0
				for index := 0; index < len(neighShardList); index++ {
					if neighShardList[index] { //内部有nodeId邻居的分片
						//判断不在一个分片的邻居节点
						neighShard := index
						if myShard != neighShard && maskShard[neighShard] == 0 {
							//计算节点移动到邻居所在社区后，两个分片的处理时间的最大值是否下降，
							nodeToMyShardTXnum := neighShardTxNum[myShard]
							nodeToOtherShardTXnum := int(totalWeight-self) - nodeToMyShardTXnum
							sourceShardProcessTime := this.shard[myShard].processTime
							destShardProcessTime := this.shard[neighShard].processTime
							var biggerProcessTime float32 = 0.0
							if sourceShardProcessTime > destShardProcessTime {
								biggerProcessTime = sourceShardProcessTime
							} else {
								biggerProcessTime = destShardProcessTime
							}
							newSourceExTXnum := this.shard[myShard].exTXnum - nodeToOtherShardTXnum + nodeToMyShardTXnum
							newSourceInTXnum := this.shard[myShard].inTXnum - nodeToMyShardTXnum - int(self)
							newSourceProcessTime := (float32(newSourceInTXnum) + this.beta*float32(newSourceExTXnum)) / this.shard[myShard].performance
							//计算节点移动到邻居所在社区后，目标分片的处理时间
							nodeToDestShardTXnum := neighShardTxNum[neighShard]
							nodeToNoDestShardTXnum := int(totalWeight-self) - nodeToDestShardTXnum
							newDestExTXnum := this.shard[neighShard].exTXnum + nodeToNoDestShardTXnum - nodeToDestShardTXnum
							newDestInTXnum := this.shard[neighShard].inTXnum + nodeToDestShardTXnum + int(self)
							newDestProcessTime := (float32(newDestInTXnum) + this.beta*float32(newDestExTXnum)) / this.shard[neighShard].performance
							var newBiggerProcessTime float32 = 0.0
							if newSourceProcessTime > newDestProcessTime {
								newBiggerProcessTime = newSourceProcessTime
							} else {
								newBiggerProcessTime = newDestProcessTime
							}
							differ := biggerProcessTime - newBiggerProcessTime
							//如果移动节点后，两个分片的处理时间的最大值下降，则移动节点到邻居所在社区
							if differ-maxDifference > 0 { //迁移阈值
								//移动节点到邻居所在社区
								maxDifference = differ
								bestShard = neighShard
								myShardProcessTime = newSourceProcessTime
								myShardInTxNum = newSourceInTXnum
								myShardExTxNum = newSourceExTXnum
								bestShardInTxNum = newDestInTXnum
								bestShardExTxNum = newDestExTXnum
								bestShardProcesstime = newDestProcessTime
							}
							maskShard[neighShard] = 1
						}
					}
				}
				if bestShard != myShard { //节点移动后更新分片的处理时间、节点所属分片
					this.shard[bestShard].inTXnum = bestShardInTxNum
					this.shard[bestShard].exTXnum = bestShardExTxNum
					this.shard[bestShard].processTime = bestShardProcesstime
					this.shard[bestShard].nodeNumInShard += 1
					this.shard[myShard].inTXnum = myShardInTxNum
					this.shard[myShard].exTXnum = myShardExTxNum
					this.shard[myShard].processTime = myShardProcessTime
					this.shard[myShard].nodeNumInShard -= 1
					newNodeToShard[nodeId] = bestShard
					improved = true
					for _, edge := range this.current.graph.GetIncidentEdges(nodeId) { //移动节点的邻居需要重新判断deltaQ，因此加入判断队列q
						if mark[edge.destId] {
							q = append(q, edge.destId) //非确定性
							//unProcessed++ //确定性
							mark[edge.destId] = false
						}
					}
				}
				//unProcessed-- //确定性
			}
		}
	}
	this.current = &this.level[len(this.level)-1]
	return improved, newNodeToShard
}

func (this *Louvain) rebuild() { //重构社区编号
	renumbers := map[int]int{}
	reserveCommIndex := make([]int, 0, len(this.current.communities))
	num := 0 //新社区的数量
	//循环前移动节点的所属分片标签inCommunity为移动到的社区的社区编号，这个循环的目的是为了给新的社区重新编号
	for nodeId, inCommunity := range this.current.inCommunities {
		if commId, exists := renumbers[inCommunity]; !exists {
			renumbers[inCommunity] = num
			this.current.inCommunities[nodeId] = num
			reserveCommIndex = append(reserveCommIndex, inCommunity)
			num++
		} else {
			this.current.inCommunities[nodeId] = commId
		}
	}
	//fmt.Println("rebuild:num:", num)
	//移动旧编号社区的社区信息到新编号社区
	newCommunities := make([]Community, num)
	for index, comm := range reserveCommIndex {
		newCommunities[index] = this.current.communities[comm]
	}
	// allInTXnum, allExTXnum, allTXnum := 0, 0, 0
	// for _, comm := range newCommunities {
	// 	allInTXnum += comm.inTXnum
	// 	allExTXnum += comm.exTXnum
	// }
	// allExTXnum = allExTXnum / 2
	// allTXnum = allInTXnum + allExTXnum
	// fmt.Printf("[xx.0]---allInTXnum: %d, allExTXnum: %d, allTXnum: %d \n", allInTXnum, allExTXnum, allTXnum)

	//二维切片，表示新社区所含的节点ID
	communityNodes := make([][]int, len(newCommunities))
	for nodeId := 0; nodeId < this.current.graph.GetNodeSize(); nodeId++ {
		communityNodes[this.current.inCommunities[nodeId]] = append(communityNodes[this.current.inCommunities[nodeId]], nodeId)
	}

	//一个新社区作为一个点，构造新层级的图
	newGraph := Graph{make(Edges, len(newCommunities)), make([]WeightType, len(newCommunities))}
	//新图中添加边
	fmt.Println("newGraph.GetNodeSize():", newGraph.GetNodeSize())
	for commId := 0; commId < newGraph.GetNodeSize(); commId++ {
		newEdges := map[int]WeightType{}
		selfWeight := WeightType(0.0)
		for _, nodeId := range communityNodes[commId] {
			edges := this.current.graph.GetIncidentEdges(nodeId)
			for _, edge := range edges {
				newEdges[this.current.inCommunities[edge.destId]] += edge.weight
			}
			selfWeight += this.current.graph.GetSelfWeight(nodeId)
		}
		newGraph.AddSelfEdge(commId, newEdges[commId]+selfWeight) //不能添加/2
		for nodeId, weight := range newEdges {
			if nodeId != commId {
				newGraph.AddDirectedEdge(commId, nodeId, weight)
			}
		}
	}
	newInCommunities := make([]int, newGraph.GetNodeSize())
	for nodeId := 0; nodeId < len(newInCommunities); nodeId++ {
		newInCommunities[nodeId] = nodeId
	}
	this.level = append(this.level, Level{newGraph, newCommunities, newInCommunities})
	this.current = &this.level[len(this.level)-1]
}

// func (this *Louvain) GetLevel(n int) *Level {
// 	return &this.level[n]
// }

func (this *Louvain) Compute(isModularity bool, isDeterministic bool, isTxAllo bool) {
	if isDeterministic {
		fmt.Printf("[start deterministic first atage : louvain]\n")
	}
	if isTxAllo {
		fmt.Printf("[start use workflows of TxAllo in first atage : louvain]\n")
	}
	if !isDeterministic && !isTxAllo {
		fmt.Printf("[start undeterministic first atage : louvain]\n")
	}
	improved := this.Merge(isDeterministic, isTxAllo) //在模块度划分时不会用上
	// allInTXnum, allExTXnum, allTXnum := this.CalCommTX() //hereeeee
	// fmt.Printf("1.0---allInTXnum: %d, allExTXnum: %d, allTXnum: %d \n", allInTXnum, allExTXnum, allTXnum)
	//i := 1
	for improved { //不断的合并重构直到没有节点移动，没有增益
		this.rebuild()
		// allInTXnum, allExTXnum, allTXnum = this.CalCommTX()
		// fmt.Printf("%d.1---allInTXnum: %d, allExTXnum: %d, allTXnum: %d \n", i, allInTXnum, allExTXnum, allTXnum)
		// i++
		improved = this.Merge(isDeterministic, isTxAllo)
		// allInTXnum, allExTXnum, allTXnum = this.CalCommTX()
		// fmt.Printf("%d.0---allInTXnum: %d, allExTXnum: %d, allTXnum: %d \n", i, allInTXnum, allExTXnum, allTXnum)
	}
	//allInTXnum, allExTXnum, allTXnum = this.CalCommTX()
	//fmt.Printf("final---allInTXnum: %d, allExTXnum: %d, allTXnum: %d \n", allInTXnum, allExTXnum, allTXnum)
}

func (this *Louvain) GetPertition(level int) ([]int, []int) {
	nodeToCommunity := make([]int, this.level[0].graph.GetNodeSize())
	nodeNum := make([]int, this.GetCommunitiesNum())
	for nodeId, _ := range nodeToCommunity {
		commId := nodeId
		for l := 0; l != level; l++ {
			commId = this.level[l].inCommunities[commId]
		}
		nodeToCommunity[nodeId] = commId
		this.current.communities[commId].nodeNum += 1
		nodeNum[commId] = nodeNum[commId] + 1
	}
	return nodeToCommunity, nodeNum
}

func (this *Louvain) GetBestPertition() ([]int, []int) {
	return this.GetPertition(len(this.level))
}

// 返回最后的社区数量
func (this *Louvain) GetCommunitiesNum() int {
	return len(this.current.inCommunities)
}

// 返回最后的社区ID和拥有的节点数量,用getbestpartition
func (this *Louvain) GetCommunitiesNodeNum() ([]int, []int) {
	_, commNodeNum := this.GetBestPertition()
	communities_num := len(commNodeNum)
	//对数组communities_num中各标签对应的值进行排序，得到的标签顺序存在descRank中
	descRank := make([]int, communities_num)
	descNodeNum := make([]int, communities_num)
	sourceIndex := make([]int, communities_num)
	for i := 0; i < communities_num; i++ {
		sourceIndex[i] = i
	}
	for i := 0; i < communities_num; i++ {
		max := commNodeNum[i]
		maxIndex := sourceIndex[i]
		Index := i
		for j := i + 1; j < communities_num; j++ {
			if commNodeNum[j] > max {
				max = commNodeNum[j]
				maxIndex = sourceIndex[j]
				Index = j
			}
		}
		descRank[i] = maxIndex
		descNodeNum[i] = max
		commNodeNum[Index] = commNodeNum[i]
		sourceIndex[Index] = sourceIndex[i]
	}
	return descNodeNum, descRank
}

// 寻找处理时间最短的分片
func (this *Louvain) FindMinProcessTimeShard() int {
	var minProcessTimeShard int = -1
	var minProcessTime float32 = math.MaxFloat32
	for index, shard := range this.shard {
		if shard.processTime < minProcessTime {
			minProcessTime = shard.processTime
			minProcessTimeShard = index
		}
	}
	return minProcessTimeShard
}

// 计算某个社区与某个分片的交易数量
func (this *Louvain) CalCommShardTXnum(community int, shard int, levelIndex *Level) int {
	var commToShardTXnum int = 0
	for _, edge := range levelIndex.graph.GetIncidentEdges(community) {
		if levelIndex.communities[edge.destId].inShard == shard {
			commToShardTXnum += int(edge.weight)
		}
	}
	return commToShardTXnum
}

// 分配社区到分片。将节点数最多的社区分配到前几个分片，如果社区数多于分片数，后续社区加到处理时间最短的分片
func (this *Louvain) CommToShard(beta float32) {
	//返回最后一层（current）的社区按拥有的节点数降序排列，descRank是社区原有的标签，descNodeNum是社区拥有的节点数
	descNodeNum, descRank := this.GetCommunitiesNodeNum()
	if len(descRank) < len(this.shard) {
		fmt.Println("社区划分后的社区数小于分片数！会导致最后几个分片分配到的账户为空")
		//将社区分到前几个分片
		fmt.Println("len(descRank):", len(descRank))
		for i := 0; i < len(descRank); i++ {
			this.current.communities[descRank[i]].inShard = i
			fmt.Println("descRank[i]:", descRank[i], " i:", i)
			//更新分片的节点数、内部交易、外部交易、处理时间
			this.shard[i].nodeNumInShard = descNodeNum[i]
			this.shard[i].inTXnum = this.current.communities[descRank[i]].inTXnum
			this.shard[i].exTXnum = this.current.communities[descRank[i]].exTXnum
			this.shard[i].processTime = (float32(this.shard[i].inTXnum) + beta*float32(this.shard[i].exTXnum)) / this.shard[i].performance
		}
		for i := len(descRank); i < len(this.shard); i++ {
			this.shard[i].processTime = 0.0
			this.shard[i].nodeNumInShard = 0
			this.shard[i].inTXnum = 0
			this.shard[i].exTXnum = 0
		}

	} else { //分片的节点数和之后打印的不一样，以及为什么第三步merge没有节点移动
		for i := 0; i < len(this.shard); i++ {
			this.current.communities[descRank[i]].inShard = i
			this.shard[i].nodeNumInShard = descNodeNum[i]
			//fmt.Println("commID:", descRank[i], "shardID i:", i, "commNodeNum:", descNodeNum[i])
			this.shard[i].inTXnum = this.current.communities[descRank[i]].inTXnum
			this.shard[i].exTXnum = this.current.communities[descRank[i]].exTXnum
			this.shard[i].processTime = (float32(this.shard[i].inTXnum) + beta*float32(this.shard[i].exTXnum)) / this.shard[i].performance
		}
		minProcessTimeShard := this.FindMinProcessTimeShard()
		for i := len(this.shard); i < len(descRank); i++ {
			commID := descRank[i]
			//加入到处理时间最短的分片
			this.current.communities[commID].inShard = minProcessTimeShard
			//计算该社区与要加入的分片的交易数量
			commToShardTXnum := this.CalCommShardTXnum(commID, minProcessTimeShard, this.current)
			//更新分片的交易数量
			this.shard[minProcessTimeShard].inTXnum += commToShardTXnum + this.current.communities[commID].inTXnum
			this.shard[minProcessTimeShard].exTXnum = this.shard[minProcessTimeShard].exTXnum + this.current.communities[commID].exTXnum - 2*commToShardTXnum
			//更新分片的节点数量、处理时间
			this.shard[minProcessTimeShard].nodeNumInShard += descNodeNum[i]
			this.shard[minProcessTimeShard].processTime = (float32(this.shard[minProcessTimeShard].inTXnum) +
				beta*float32(this.shard[minProcessTimeShard].exTXnum)) / this.shard[minProcessTimeShard].performance
			//更新最短处理时间的分片
			minProcessTimeShard = this.FindMinProcessTimeShard()

		}
	}
}

// 获取louvain的beta
func (this *Louvain) Getbeta() float32 {
	return this.beta
}

// 打印分片的处理时间和节点数量
func (this *Louvain) PrintShard() {
	allInTXnum := 0
	allExTXnum := 0
	for i := 0; i < this.shardNum; i++ {
		fmt.Println("shard:", i, "nodeNumInShard:", this.shard[i].nodeNumInShard,
			"inTxNum:", this.shard[i].inTXnum, "exTxNum:", this.shard[i].exTXnum, "processTime:", this.shard[i].processTime)
		allInTXnum += this.shard[i].inTXnum
		allExTXnum += this.shard[i].exTXnum
	}
	fmt.Println("allInTXnum:", allInTXnum, "allExTXnum:", allExTXnum/2, "allTXnum:", allInTXnum+allExTXnum/2)
}

// 根据节点-分片映射表得到各分片包含的节点列表
func (this *Louvain) GetShardNodeList(nodeToShard []int) [][]int {
	shardNodeList := make([][]int, len(this.shard))
	for i := 0; i < len(nodeToShard); i++ {
		shardId := nodeToShard[i]
		shardNodeList[shardId] = append(shardNodeList[shardId], i)
	}
	return shardNodeList
}

// 获取louvain的层数
func (this *Louvain) GetLevelNum() int {
	return len(this.level)
}

// 将节点在levelIndex(0开始计数）层所在社区所属的分片标签传递到第一层
func (this *Louvain) UpdateNodeShardIndex() ([]int, []int) {
	levelIndex := len(this.level) - 1
	nodeToCommunity := make([]int, this.level[0].graph.GetNodeSize())
	nodeToShard := make([]int, this.level[0].graph.GetNodeSize())
	for nodeId, _ := range nodeToCommunity {
		commId := this.level[0].inCommunities[nodeId]
		firstLevelCommId := commId
		for l := 1; l < len(this.level); l++ {
			commId = this.level[l].inCommunities[commId]
		}
		nodeToCommunity[nodeId] = commId
		this.level[0].communities[firstLevelCommId].inShard = this.level[levelIndex].communities[commId].inShard
		nodeToShard[nodeId] = this.level[levelIndex].communities[commId].inShard
	}
	for nodeId, shard := range nodeToShard {
		firtcomm := this.level[0].inCommunities[nodeId]
		if shard != this.level[0].communities[firtcomm].inShard {
			fmt.Println("error!!!!!!!!shard !=this.level[0].communities[firtcomm].inShard--------")
		}
	}
	return nodeToCommunity, nodeToShard
}

// 第二步选择将非前k的社区的节点一个个加入到前k个社区，
// 有邻居在前k个社区的，选择的分片范围是邻居所在的分片，没有邻居在前k个社区的话，选择范围是所有分片；
// 加入到选择范围内处理时间最短的分片
func (this *Louvain) NodeToShard(beta float32) []int {
	//返回最后一层（current）的社区按拥有的节点数降序排列，descRank是社区原有的标签，descNodeNum是社区拥有的节点数
	descNodeNum, descRank := this.GetCommunitiesNodeNum()
	if len(descRank) < len(this.shard) {
		fmt.Println("社区划分后的社区数小于分片数！会导致最后几个分片分配到的账户为空")
		//将社区分到前几个分片
		fmt.Println("len(descRank):", len(descRank))
		for i := 0; i < len(descRank); i++ {
			this.current.communities[descRank[i]].inShard = i
			fmt.Println("descRank[i]:", descRank[i], " i:", i)
			//更新分片的节点数、内部交易、外部交易、处理时间
			this.shard[i].nodeNumInShard = descNodeNum[i]
			this.shard[i].inTXnum = this.current.communities[descRank[i]].inTXnum
			this.shard[i].exTXnum = this.current.communities[descRank[i]].exTXnum
			this.shard[i].processTime = (float32(this.shard[i].inTXnum) + beta*float32(this.shard[i].exTXnum)) / this.shard[i].performance
		}
		for i := len(descRank); i < len(this.shard); i++ {
			this.shard[i].processTime = 0.0
			this.shard[i].nodeNumInShard = 0
			this.shard[i].inTXnum = 0
			this.shard[i].exTXnum = 0
		}
	} else { //分片的节点数和之后打印的不一样，以及为什么第三步merge没有节点移动
		for i := 0; i < len(this.shard); i++ {
			this.current.communities[descRank[i]].inShard = i
			this.shard[i].nodeNumInShard = descNodeNum[i]
			//fmt.Println("commID:", descRank[i], "shardID i:", i, "commNodeNum:", descNodeNum[i])
			this.shard[i].inTXnum = this.current.communities[descRank[i]].inTXnum
			this.shard[i].exTXnum = this.current.communities[descRank[i]].exTXnum
			this.shard[i].processTime = (float32(this.shard[i].inTXnum) + beta*float32(this.shard[i].exTXnum)) / this.shard[i].performance
		}
		_, nodeToShard := this.UpdateNodeShardIndex()
		num := 0
		for i := 0; i < len(nodeToShard); i++ {
			if nodeToShard[i] == -1 {
				num += 1
			}
		}
		if num != 0 {
			fmt.Println("num of -1(node not allocated to shard) after move the first k comm :", num)
		}
		for nodeId := 0; nodeId < this.level[0].graph.GetNodeSize(); nodeId++ {
			if nodeToShard[nodeId] < 0 || nodeToShard[nodeId] >= len(this.shard) {
				if nodeToShard[nodeId] != -1 {
					fmt.Println("nodeToShard[nodeId] is wrong!")
				}
				//获取nodeId在level[0]的邻居的社区编号，如果存在一些邻居在前k个社区，找出这些邻居中所在分片的处理时间最短的那个邻居x，则将nodeId加入到x所在的分片
				//如果不存在邻居在前k个社区，将nodeId加入到处理时间最短的分片
				minProTimeNeighShardId := -1
				var minNeighShardProTime float32 = math.MaxFloat32
				totalWeight := WeightType(0.0)
				shardWeight := make([]WeightType, len(this.shard))
				for _, edge := range this.level[0].graph.GetIncidentEdges(nodeId) {
					//shardIndex := this.current.communities[nodeToCommunity[edge.destId]].inShard
					shardIndex := nodeToShard[edge.destId]
					if shardIndex != -1 {
						if this.shard[shardIndex].processTime < minNeighShardProTime {
							minNeighShardProTime = this.shard[shardIndex].processTime
							// neighCommId = edge.destId
							minProTimeNeighShardId = shardIndex
						}
						shardWeight[shardIndex] += edge.weight
					}
					totalWeight += edge.weight
				}
				minProcessTimeShard := minProTimeNeighShardId
				nodeToShardTXnum := 0
				if minProcessTimeShard == -1 {
					minProcessTimeShard = this.FindMinProcessTimeShard()
				} else {
					//计算该社区与要加入的分片的交易数量
					nodeToShardTXnum = int(shardWeight[minProcessTimeShard])
				}
				//节点移动到minProcessTimeShard，更新minProcessTimeShard的节点数、内部交易、外部交易、处理时间
				//加入到处理时间最短的分片
				nodeToShard[nodeId] = minProcessTimeShard
				//更新分片的交易数量
				this.shard[minProcessTimeShard].inTXnum += nodeToShardTXnum
				this.shard[minProcessTimeShard].exTXnum = this.shard[minProcessTimeShard].exTXnum - 2*nodeToShardTXnum + int(totalWeight)
				//更新分片的节点数量、处理时间
				this.shard[minProcessTimeShard].nodeNumInShard += 1
				this.shard[minProcessTimeShard].processTime = (float32(this.shard[minProcessTimeShard].inTXnum) +
					beta*float32(this.shard[minProcessTimeShard].exTXnum)) / this.shard[minProcessTimeShard].performance
				firstLevelCommId := this.level[0].inCommunities[nodeId]
				this.level[0].communities[firstLevelCommId].inShard = minProcessTimeShard
			}
		}
		number := 0
		for i := 0; i < len(nodeToShard); i++ {
			if nodeToShard[i] == -1 {
				number += 1
			}
		}
		if number != 0 {
			fmt.Println("err! num of -1 after allocate Remaining nodes to shard:", number)
		} else {
			fmt.Println("all nodes are allocated to shard! ")
		}
		return nodeToShard
	}
	return nil
}

// 内部verify
func (this *Louvain) VerifyMyResult(nodeToShard []int) {
	fmt.Println("[verifyMyResult: start]")
	shardInfo := make([]Shard, this.shardNum)
	for nodeId := 0; nodeId < this.level[0].graph.GetNodeSize(); nodeId++ {
		neigh := WeightType(0.0)
		myShard := nodeToShard[nodeId]
		for _, edge := range this.level[0].graph.GetIncidentEdges(nodeId) {
			neigh += edge.weight
			neighShard := nodeToShard[edge.destId]
			if myShard != neighShard {
				shardInfo[myShard].exTXnum += int(edge.weight)
			} else {
				shardInfo[myShard].inTXnum += int(edge.weight)
			}
		}
		shardInfo[nodeToShard[nodeId]].nodeNumInShard += 1
	}
	for index, _ := range shardInfo {
		shardInfo[index].inTXnum = shardInfo[index].inTXnum / 2
	}
	newprocessTime := make([]float32, len(shardInfo))
	for i := 0; i < this.shardNum; i++ {
		newprocessTime[i] = (float32(shardInfo[i].inTXnum) + this.beta*float32(shardInfo[i].exTXnum)) / this.shard[i].performance
	}
	allInTXnum := 0
	allExTXnum := 0
	var maxProcessTime float32 = 0.0
	for i := 0; i < this.shardNum; i++ {
		fmt.Println("shard:", i, "nodeNumInShard:", shardInfo[i].nodeNumInShard,
			"inTxNum:", shardInfo[i].inTXnum, "exTxNum:", shardInfo[i].exTXnum, "processTime:", newprocessTime[i])
		allInTXnum += shardInfo[i].inTXnum
		allExTXnum += shardInfo[i].exTXnum
		if newprocessTime[i] > maxProcessTime {
			maxProcessTime = newprocessTime[i]
		}
	}
	fmt.Printf("******maxProcessTime:%v  *******", maxProcessTime)
	fmt.Println("allInTXnum:", allInTXnum, "allExTXnum:", allExTXnum/2, "allTXnum:", allInTXnum+allExTXnum/2)
	fmt.Println("[verifyMyResult: End]")
}

// 验证账户分配时用的构建louvain函数
func VerifyNewLouvain(graph Graph, shardPerformance []float32, beta float32, shardNum int, nodeToShard []int) (*Louvain, []int) {
	louvain := Louvain{}
	louvain.level = make([]Level, 1)
	louvain.current = &louvain.level[0]
	louvain.current.graph = graph
	louvain.current.communities = make([]Community, louvain.current.graph.GetNodeSize())
	louvain.current.inCommunities = make([]int, louvain.current.graph.GetNodeSize())
	louvain.m2 = WeightType(0.0)
	louvain.shard = make([]Shard, shardNum)
	louvain.shardNum = shardNum
	louvain.beta = beta
	for i := 0; i < louvain.shardNum; i++ {
		louvain.shard[i].performance = shardPerformance[i]
	}
	borderNode := make([]int, 0)
	for nodeId := 0; nodeId < louvain.current.graph.GetNodeSize(); nodeId++ {
		louvain.current.inCommunities[nodeId] = nodeId
		neigh := WeightType(0.0)
		ifAppended := false
		myShard := nodeToShard[nodeId]
		for _, edge := range louvain.current.graph.GetIncidentEdges(nodeId) {
			neigh += edge.weight
			neighShard := nodeToShard[edge.destId]
			if myShard != neighShard {
				if !ifAppended {
					borderNode = append(borderNode, nodeId)
					ifAppended = true
				}
				louvain.shard[myShard].exTXnum += int(edge.weight)
			} else {
				louvain.shard[myShard].inTXnum += int(edge.weight)
			}
		}
		self := WeightType(louvain.current.graph.GetSelfWeight(nodeId))
		louvain.current.communities[nodeId] = Community{self, neigh + self, 0, int(neigh), nodeToShard[nodeId], 1}
		louvain.shard[nodeToShard[nodeId]].nodeNumInShard += 1
		louvain.m2 += (neigh + 2*self)
	}
	for index := range louvain.shard {
		louvain.shard[index].inTXnum = louvain.shard[index].inTXnum / 2
	}
	//更新分片处理时间
	for i := 0; i < louvain.shardNum; i++ {
		louvain.shard[i].processTime = (float32(louvain.shard[i].inTXnum) + beta*float32(louvain.shard[i].exTXnum)) / louvain.shard[i].performance
	}
	allInTXnum := 0
	allExTXnum := 0
	var maxProcessTime float32 = 0.0
	for i := 0; i < louvain.shardNum; i++ {
		fmt.Println("shard:", i, "nodeNumInShard:", louvain.shard[i].nodeNumInShard,
			"inTxNum:", louvain.shard[i].inTXnum, "exTxNum:", louvain.shard[i].exTXnum, "processTime:", louvain.shard[i].processTime)
		allInTXnum += louvain.shard[i].inTXnum
		allExTXnum += louvain.shard[i].exTXnum
		if louvain.shard[i].processTime > maxProcessTime {
			maxProcessTime = louvain.shard[i].processTime
		}
	}
	fmt.Printf("******maxProcessTime:%v  *******", maxProcessTime)
	fmt.Println("allInTXnum:", allInTXnum, "allExTXnum:", allExTXnum/2, "allTXnum:", allInTXnum+allExTXnum/2)
	return &louvain, borderNode
}

// 验证算法结果是否正确,返回值bool表示是否通过，
// 如果不通过，返回的数组包含两个元素，[0]标签表示可以移动的节点ID，[1]标签表示移动到的分片ID
// 验证步骤
// 1、找出边界节点
// 2、找出边界节点的邻居所在的分片，
// 3、节点移动到邻居所在的分片后两分片的处理时间较大值有所降低，说明有节点可以移动，验证失败
func (this *Louvain) Verify(nodeToShard []int, borderNode []int) (bool, []int) {
	verifyResult := false
	newNodeToShard := nodeToShard
	q := borderNode

	for len(q) != 0 { //q为判断队列
		nodeId := q[0]
		q = q[1:]
		self := WeightType(this.current.graph.GetSelfWeight(nodeId))
		totalWeight := self
		neighShardList := make([]bool, this.shardNum)
		neighShardTxNum := make([]int, this.shardNum)
		myShard := newNodeToShard[nodeId]
		bestShard := myShard
		for _, edge := range this.current.graph.GetIncidentEdges(nodeId) {
			totalWeight += edge.weight
			neighShard := newNodeToShard[edge.destId]
			if myShard != neighShard && !neighShardList[neighShard] {
				neighShardList[neighShard] = true
			}
			neighShardTxNum[neighShard] += int(edge.weight)
		}

		maskShard := make([]int, len(this.shard))
		for index := 0; index < len(neighShardList); index++ {
			if neighShardList[index] { //内部有nodeId邻居的分片
				neighShard := index
				if myShard != neighShard && maskShard[neighShard] == 0 {
					//计算节点移动到邻居所在分片后，两个分片的处理时间的最大值是否下降，
					nodeToMyShardTXnum := neighShardTxNum[myShard]
					nodeToOtherShardTXnum := int(totalWeight-self) - nodeToMyShardTXnum
					sourceShardProcessTime := this.shard[myShard].processTime
					destShardProcessTime := this.shard[neighShard].processTime
					var biggerProcessTime float32 = 0.0
					if sourceShardProcessTime > destShardProcessTime {
						biggerProcessTime = sourceShardProcessTime
					} else {
						biggerProcessTime = destShardProcessTime
					}
					newSourceExTXnum := this.shard[myShard].exTXnum - nodeToOtherShardTXnum + nodeToMyShardTXnum
					newSourceInTXnum := this.shard[myShard].inTXnum - nodeToMyShardTXnum
					newSourceProcessTime := (float32(newSourceInTXnum) + this.beta*float32(newSourceExTXnum)) / this.shard[myShard].performance
					//计算节点移动到邻居所在分片后，目标分片的处理时间
					nodeToDestShardTXnum := neighShardTxNum[neighShard]
					nodeToNoDestShardTXnum := int(totalWeight-self) - nodeToDestShardTXnum
					newDestExTXnum := this.shard[neighShard].exTXnum + nodeToNoDestShardTXnum - nodeToDestShardTXnum
					newDestInTXnum := this.shard[neighShard].inTXnum + nodeToDestShardTXnum
					newDestProcessTime := (float32(newDestInTXnum) + this.beta*float32(newDestExTXnum)) / this.shard[neighShard].performance
					var newBiggerProcessTime float32 = 0.0
					if newSourceProcessTime > newDestProcessTime {
						newBiggerProcessTime = newSourceProcessTime
					} else {
						newBiggerProcessTime = newDestProcessTime
					}
					//如果移动节点后，两个分片的处理时间的最大值下降，则说明有节点可以移动，验证失败
					if biggerProcessTime-newBiggerProcessTime > 0.0001 { //验证容忍阈值
						fmt.Printf("[!!]nodeId %d in shard %d can be moded to shard %d\n", nodeId, myShard, neighShard)
						fmt.Printf("[!!]sourceShardProcessTime: %f destShardProcessTime: %f\n", sourceShardProcessTime, destShardProcessTime)
						fmt.Printf("[!!]newSourceProcessTime: %f newDestProcessTime: %f\n", newSourceProcessTime, newDestProcessTime)
						errorNodeAndShard := []int{nodeId, myShard, neighShard}
						return verifyResult, errorNodeAndShard
					}
					maskShard[neighShard] = 1
				}
			}
		}
		if bestShard != myShard {
			fmt.Println("err! the fountion should return value before!")
			return verifyResult, nil
		}
	}
	verifyResult = true
	return verifyResult, nil
}

func (this *Louvain) CalCommTX() (int, int, int) {
	allInTXnum := 0
	allExTXnum := 0
	num := 0
	//fmt.Println("len(this.current.communities):", len(this.current.communities))
	for _, comm := range this.current.communities {
		if comm.inTXnum != 0 {
			num++
		}
		allInTXnum += comm.inTXnum
		allExTXnum += comm.exTXnum
	}
	//fmt.Println("num of comm with inTXnum!=0:", num)
	return allInTXnum, allExTXnum / 2, allInTXnum + allExTXnum/2
}

func (this *Louvain) PrintGraphWeight() {
	edgeWeight, totalWeight := this.current.graph.GetEdgeAndTotalWeight()
	fmt.Printf("edgeWeight %f,totalWeight %f\n", edgeWeight, totalWeight)
}

// 划分账户需要的参数
func (this *Louvain) PLouvain_Partition() {

}
