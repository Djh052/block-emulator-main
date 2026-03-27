package committee

import (
	"blockEmulator/core"
	"blockEmulator/louvain"
	"blockEmulator/message"
	"blockEmulator/networks"
	"blockEmulator/params"
	"blockEmulator/supervisor/signal"
	"blockEmulator/supervisor/supervisor_log"
	"blockEmulator/utils"
	"encoding/csv"
	"encoding/json"
	"io"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// PLouvain committee operations
type PLouvainCommitteeModule struct {
	csvPath      string
	dataTotalNum int
	nowDataNum   int
	batchDataNum int

	// additional variants
	plouvainLock            sync.Mutex
	plouvainState           *louvain.PLouvainState
	modifiedMap             map[string]int //账户地址->分片id
	plouvainLastRunningTime time.Time
	plouvainFreq            int
	plouvainFreaEpoch       int
	shardPerformance        []float32

	// logger module
	sl *supervisor_log.SupervisorLog

	// control components
	Ss          *signal.StopSignal           // to control the stop message sending
	IpNodeTable map[uint64]map[uint64]string //分片Id->节点Id->节点IP

	nodeValueHistory     *NodeValueHistory
	nodeAllocLastRunTime time.Time
	nodeAllocFreq        int
	shardLoadHistory     map[uint64][]float64

	epochId int32
}

type NodeValueHistory struct {
	// epoch内节点贡献值变化、贡献交易变化
	nodeSafeVaule        map[uint64]map[uint64]float32 //分片id，节点id，节点贡献值
	temSafeVaule         map[uint64]map[uint64]float32 //epoch内分片id，节点id，节点贡献值
	nodePerformanceVaule map[uint64]map[uint64]float32 //分片id，节点id，节点性能值
	temPerformanceVaule  map[uint64]map[uint64]float32 //epoch内分片id，节点id，节点性能值
}

func (nah *NodeValueHistory) Init_NodeValueHistory(Ip_nodeTable map[uint64]map[uint64]string) {
	//初始化节点安全、性能贡献值
	nah.nodeSafeVaule = make(map[uint64]map[uint64]float32)
	nah.nodePerformanceVaule = make(map[uint64]map[uint64]float32)
	nah.temSafeVaule = make(map[uint64]map[uint64]float32)
	nah.temPerformanceVaule = make(map[uint64]map[uint64]float32)
	for i := uint64(0); i < uint64(len(Ip_nodeTable)-1); i++ {
		nah.nodeSafeVaule[i] = make(map[uint64]float32)
		nah.nodePerformanceVaule[i] = make(map[uint64]float32)
		nah.temSafeVaule[i] = make(map[uint64]float32)
		nah.temPerformanceVaule[i] = make(map[uint64]float32)
		for j := uint64(0); j < uint64(len(Ip_nodeTable[i])); j++ {
			nah.nodeSafeVaule[i][j] = 0.5
			nah.nodePerformanceVaule[i][j] = 0.0
		}
	}
}

// 此处需要传shardPerformance参数
func NewPLouvainCommitteeModule(Ip_nodeTable map[uint64]map[uint64]string, Ss *signal.StopSignal, sl *supervisor_log.SupervisorLog,
	csvFilePath string, dataNum, batchNum int, shardPerformance []float32) *PLouvainCommitteeModule {
	plState := new(louvain.PLouvainState)
	//参数分别为beta, shardNum, shardPerformance, secStageMoveNode, isDeterministic, isTxAllo, israndom，全false是模式一非确定性
	//plState.Init_PLouvainState(params.Beta, params.ShardNum, shardPerformance, false, false, false, false)
	//参数分别为beta, shardNum, shardPerformance, secStageMoveNode, isDeterministic, isTxAllo, israndom，全false是模式一非确定性
	plState.Init_PLouvainState(2, params.ShardNum, shardPerformance, false, true, false, false)

	newnodeValueHistory := new(NodeValueHistory)
	newnodeValueHistory.Init_NodeValueHistory(Ip_nodeTable)
	//输出nodeSafeVaule的length
	//sl.Slog.Printf("Supervisor: newnodeValueHistory.nodeSafeVaule length is %d\n", len(newnodeValueHistory.nodeSafeVaule))
	shardLoad := make(map[uint64][]float64)
	return &PLouvainCommitteeModule{
		csvPath:                 csvFilePath,
		dataTotalNum:            dataNum,
		batchDataNum:            batchNum,
		nowDataNum:              0,
		plouvainState:           plState,
		modifiedMap:             make(map[string]int),
		plouvainFreq:            50,
		plouvainFreaEpoch:       4,
		shardPerformance:        shardPerformance,
		plouvainLastRunningTime: time.Time{},
		IpNodeTable:             Ip_nodeTable,
		Ss:                      Ss,
		sl:                      sl,
		nodeValueHistory:        newnodeValueHistory,
		nodeAllocLastRunTime:    time.Time{},
		//nodeAllocFreq:           params.NodeAllocFreq,
		shardLoadHistory: shardLoad,
		epochId:          0,
	}
}

func (plcm *PLouvainCommitteeModule) HandleOtherMessage([]byte) {}

func (plcm *PLouvainCommitteeModule) fetchModifiedMap(key string) uint64 {
	if val, ok := plcm.modifiedMap[key]; !ok {
		return uint64(utils.Addr2Shard(key))
	} else {
		return uint64(val)
	}
}

func (plcm *PLouvainCommitteeModule) txSending(txlist []*core.Transaction) map[uint64]uint64 {
	// the txs will be sent
	sendToShard := make(map[uint64][]*core.Transaction)
	sendTxNum := make(map[uint64]uint64)

	for idx := 0; idx <= len(txlist); idx++ {
		if idx > 0 && (idx%params.InjectSpeed == 0 || idx == len(txlist)) {
			// send to shard
			for sid := uint64(0); sid < uint64(params.ShardNum); sid++ {
				it := message.InjectTxs{
					Txs:       sendToShard[sid],
					ToShardID: sid,
				}
				itByte, err := json.Marshal(it)
				if err != nil {
					log.Panic(err)
				}
				send_msg := message.MergeMessage(message.CInject, itByte)
				//plcm.sl.Slog.Printf("txSending():ready sending %d txs to [%d][0]\n", len(it.Txs), sid)
				go networks.TcpDial(send_msg, plcm.IpNodeTable[sid][0])
				//plcm.sl.Slog.Printf("txSending(): Supervisor: sended %d txs to [%d][0]\n", len(it.Txs), sid)
				if _, ok := sendTxNum[sid]; !ok {
					sendTxNum[sid] = 0
				}
				sendTxNum[sid] += uint64(len(sendToShard[sid]))
			}

			sendToShard = make(map[uint64][]*core.Transaction)
			time.Sleep(time.Second)
		}
		if idx == len(txlist) {
			break
		}
		tx := txlist[idx]
		sendersid := plcm.fetchModifiedMap(tx.Sender)
		sendToShard[sendersid] = append(sendToShard[sendersid], tx)
	}
	return sendTxNum
}

func (plcm *PLouvainCommitteeModule) MsgSendingControl() {
	txfile, err := os.Open(plcm.csvPath)
	if err != nil {
		log.Panic(err)
	}
	defer txfile.Close()
	reader := csv.NewReader(txfile)
	txlist := make([]*core.Transaction, 0) // save the txs in this epoch (round)

	louvainCnt := 0
	//epochAfterAccAlloc := 0
	//shardTxNumHistory := make(map[int]map[uint64]uint64)
	//ifPlouvained := make(map[int]bool)
	//节点物理Ip-作恶IP的映射
	//maliciousIP := make(map[string]string)

	plcm.sl.Slog.Printf("Supervisor: len(plcm.IpNodeTable) is %d\n", len(plcm.IpNodeTable))
	plcm.sl.Slog.Printf("Supervisor: len(plcm.shardLoadHistory.nodeSafeVaule) is %d\n", len(plcm.nodeValueHistory.nodeSafeVaule))

	plcm.sl.Slog.Printf("Supervisor: epoch %d begins, start sending Tx.\n", plcm.epochId)
	plcm.sl.Slog.Printf("Supervisor: plcm.batchDataNum is %d\n", int(plcm.batchDataNum))
	needAccountAlloc := false
	//stopepoch := 0
	for {
		if plcm.Ss.EpochEnough() {
			plcm.sl.Slog.Printf("Supervisor: Epoch is enough, stop MsgSendingControl.\n")
			return
		}
		data, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Panic(err)
		}
		if tx, ok := data2tx(data, uint64(plcm.nowDataNum)); ok {
			txlist = append(txlist, tx)
			plcm.nowDataNum++
		} else {
			continue
		}

		// if len(txlist)%1000 == 0 {
		// 	plcm.sl.Slog.Printf("Supervisor: epoch %d, txlist length is %d. \n", plcm.epochId, len(txlist))
		// }
		// batch sending condition
		if len(txlist) == int(plcm.batchDataNum) || plcm.nowDataNum == plcm.dataTotalNum {
			//日志打印表示攒够一批交易
			//plcm.sl.Slog.Printf("Supervisor: epoch %d start sending [%d]th batch Tx.\n", plcm.epochId, batchId)
			//plcm.sl.Slog.Printf("--Supervisor: txlist length is %d. \n", len(txlist))
			// set the algorithm timer begins
			if plcm.plouvainLastRunningTime.IsZero() {
				plcm.plouvainLastRunningTime = time.Now()
			}
			if plcm.nodeAllocLastRunTime.IsZero() {
				plcm.nodeAllocLastRunTime = time.Now()
			}
			//日志中打印txlist长度
			plcm.sl.Slog.Printf("Supervisor: txlist length is %d. \n", len(txlist))
			plcm.txSending(txlist)

			// reset the variants about tx sending 这里会清空
			txlist = make([]*core.Transaction, 0)
			plcm.Ss.StopGap_Reset()
		}

		if !plcm.nodeAllocLastRunTime.IsZero() && time.Since(plcm.nodeAllocLastRunTime) >= time.Duration(plcm.nodeAllocFreq)*time.Second {
			// ifchange := plcm.Ss.StopEpoch_update(stopepoch + 1)
			// if ifchange {
			// 	plcm.sl.Slog.Printf("Supervisor: StopEpoch updated to %d", plcm.epochId+1)
			// }
			if plcm.Ss.EpochEnough() {
				plcm.sl.Slog.Printf("Supervisor: Epoch is enough, stop MsgSendingControl.\n")
				return
			}
		}

		//再判断账户划分,每plouvainFreqEpoch个epoch或者达到plouvainFreq时间会进行一次账户分配，且在epoch的第nodeAllocFreq-10秒（80-10s）进行
		//if needAccountAlloc || !plcm.plouvainLastRunningTime.IsZero() && (time.Since(plcm.plouvainLastRunningTime) >= time.Duration(plcm.plouvainFreq)*time.Second || (epochAfterAccAlloc+1)%plcm.plouvainFreaEpoch == 0) {
		if needAccountAlloc || !plcm.plouvainLastRunningTime.IsZero() && time.Since(plcm.plouvainLastRunningTime) >= time.Duration(plcm.plouvainFreq)*time.Second {
			if needAccountAlloc || time.Since(plcm.nodeAllocLastRunTime) >= time.Duration(plcm.nodeAllocFreq-10)*time.Second {
				// if !params.IfNodeAlloc {
				// 	ifchange := plcm.Ss.StopEpoch_update(plcm.epochId + 1)
				// 	if ifchange {
				// 		plcm.sl.Slog.Printf("Supervisor: StopEpoch updated to %d", plcm.epochId+1)
				// 	}
				if plcm.Ss.EpochEnough() {
					plcm.sl.Slog.Printf("Supervisor: Epoch is enough, stop MsgSendingControl.\n")
					return
				}
				//}
				plcm.plouvainLock.Lock()
				plcm.sl.Slog.Printf("Supervisor: epoch %d begins to allocate accounts.\n", plcm.epochId)
				louvainCnt++
				mmap := plcm.plouvainState.PLouvain_Partition()

				//plcm.sl.Slog.Printf("Supervisor: epoch %d allocated accounts mmap:%v \n", plcm.epochId, mmap)
				plcm.plouvainMapSend(mmap)
				for key, val := range mmap {
					plcm.modifiedMap[key] = int(val)
				}
				plcm.plouvainReset()
				plcm.plouvainLock.Unlock()
				for atomic.LoadInt32(&plcm.epochId) != int32(louvainCnt) {
					time.Sleep(time.Second)
				}
				plcm.plouvainLastRunningTime = time.Now()
				//plcm.nodeAllocLastRunTime = time.Now() //账户分配后更新节点分配时间

				//ifPlouvained[plcm.epochId] = true
				plcm.sl.Slog.Printf("Supervisor: epoch %d allocated accounts successfully.\n", plcm.epochId)

				needAccountAlloc = false
			}
		}

		if plcm.nowDataNum == plcm.dataTotalNum {
			break
		}
	}

	// all transactions are sent. keep sending partition message...
	for !plcm.Ss.GapEnough() && !plcm.Ss.EpochEnough() { // wait all txs to be handled
		time.Sleep(time.Second)

		if time.Since(plcm.nodeAllocLastRunTime) >= time.Duration(plcm.nodeAllocFreq)*time.Second {
			if plcm.Ss.EpochEnough() {
				plcm.sl.Slog.Printf("Supervisor: Epoch is enough, stop MsgSendingControl.\n")
				return
			}
		}
		if needAccountAlloc || time.Since(plcm.plouvainLastRunningTime) >= time.Duration(plcm.plouvainFreq)*time.Second {
			if needAccountAlloc || time.Since(plcm.nodeAllocLastRunTime) >= time.Duration(plcm.nodeAllocFreq-10)*time.Second {
				if plcm.Ss.EpochEnough() {
					plcm.sl.Slog.Printf("Supervisor: Epoch is enough, stop MsgSendingControl.\n")
					return
				}
				//}
				plcm.plouvainLock.Lock()
				plcm.sl.Slog.Printf("Supervisor: epoch %d begins to allocate accounts.\n", plcm.epochId)
				louvainCnt++
				mmap := plcm.plouvainState.PLouvain_Partition()
				//plcm.sl.Slog.Printf("Supervisor: epoch %d allocated accounts mmap:%v \n", plcm.epochId, mmap)
				plcm.plouvainMapSend(mmap)
				for key, val := range mmap {
					plcm.modifiedMap[key] = int(val)
				}
				plcm.plouvainReset()
				plcm.plouvainLock.Unlock()
				for atomic.LoadInt32(&plcm.epochId) != int32(louvainCnt) {
					time.Sleep(time.Second)
				}
				plcm.plouvainLastRunningTime = time.Now()
				//plcm.nodeAllocLastRunTime = time.Now() //账户分配后更新节点分配时间

				//ifPlouvained[plcm.epochId] = true
				plcm.sl.Slog.Printf("Supervisor: epoch %d allocated accounts successfully.\n", plcm.epochId)

				needAccountAlloc = false
			}
		}
	}
}

func (plcm *PLouvainCommitteeModule) plouvainMapSend(m map[string]uint64) {
	// send partition modified Map message
	pm := message.PartitionModifiedMap{
		PartitionModified: m,
	}
	pmByte, err := json.Marshal(pm)
	if err != nil {
		log.Panic()
	}
	send_msg := message.MergeMessage(message.CPartitionMsg, pmByte)
	// send to worker shards
	for i := uint64(0); i < uint64(params.ShardNum); i++ {
		networks.TcpDial(send_msg, plcm.IpNodeTable[i][0])
	}
	plcm.sl.Slog.Println("Supervisor: all partition map message has been sent. ")
}

func (plcm *PLouvainCommitteeModule) plouvainReset() {
	plcm.plouvainState = new(louvain.PLouvainState)
	//plcm.plouvainState.Init_PLouvainState(params.Beta, params.ShardNum, plcm.shardPerformance, false, false, false, false)
	plcm.plouvainState.Init_PLouvainState(params.Beta, params.ShardNum, plcm.shardPerformance, false, true, false, false)
	for key, val := range plcm.modifiedMap {
		plcm.plouvainState.PartitionMap[key] = uint64(val)
	}
}

func (plcm *PLouvainCommitteeModule) HandleBlockInfo(b *message.BlockInfoMsg) {
	plcm.sl.Slog.Printf("Supervisor: received  blockInfo from shard %d in epoch %d.\n", b.SenderShardID, b.Epoch)
	if b.BlockBodyLength == 0 {
		plcm.sl.Slog.Printf("Supervisor: received BlockBodyLength=0")
		return
	}
	//if b.Epoch != plcm.epochId {
	//	plcm.sl.Slog.Printf("Supervisor: received BlockInfo epoch is not equal to epochID in Supervisor! \n")
	//	return
	//}
	if atomic.CompareAndSwapInt32(&plcm.epochId, int32(b.Epoch-1), int32(b.Epoch)) {
		plcm.sl.Slog.Println("this curEpoch is updated", b.Epoch)
	}
	//plcm.sl.Slog.Printf("Supervisor: starts to handle BlockInfo with %d excuted TXs from shard %d in epoch %d.\n", len(b.ExcutedTxs), b.SenderShardID, b.Epoch)
	//根据已执行交易和relay交易加权得到分片的负载
	plcm.plouvainLock.Lock()
	loadChange := float64(len(b.InnerShardTxs)) + float64(params.Beta)*float64(len(b.Broker1Txs))
	if _, ok := plcm.shardLoadHistory[b.SenderShardID]; !ok {
		plcm.shardLoadHistory[b.SenderShardID] = make([]float64, 0)
	}
	if len(plcm.shardLoadHistory[b.SenderShardID]) == b.Epoch {
		plcm.shardLoadHistory[b.SenderShardID] = append(plcm.shardLoadHistory[b.SenderShardID], loadChange)
	} else if len(plcm.shardLoadHistory[b.SenderShardID]) == b.Epoch+1 {
		plcm.shardLoadHistory[b.SenderShardID][b.Epoch] += loadChange
	} else {
		plcm.sl.Slog.Printf("Supervisor: shard %d load history length is wrong.\n", b.SenderShardID)
	}
	for _, tx := range b.InnerShardTxs {
		plcm.plouvainState.AddEdge(tx.Sender, tx.Recipient)
	}
	for _, tx := range b.Relay2Txs {
		plcm.plouvainState.AddEdge(tx.Sender, tx.Recipient)
	}
	plcm.sl.Slog.Printf("Supervisor: %d excuted TXs from shard %d in epoch %d  are added to graph.\n", len(b.InnerShardTxs), b.SenderShardID, b.Epoch)
	plcm.plouvainLock.Unlock()
}
