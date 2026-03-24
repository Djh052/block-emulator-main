package committee

import (
	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/networks"
	"blockEmulator/params"
	"blockEmulator/partition"
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

// Louvain committee operations
type LouvainCommitteeModule struct {
	csvPath      string
	dataTotalNum int
	nowDataNum   int
	batchDataNum int

	// additional variants
	curEpoch               int32
	louvainLock            sync.Mutex
	louvainGraph           *partition.LouvainState // 全局交易图
	modifiedMap            map[string]uint64       // 账户地址 -> 分片ID
	louvainLastRunningTime time.Time
	louvainFreq            int

	// logger module
	sl *supervisor_log.SupervisorLog

	// control components
	Ss          *signal.StopSignal
	IpNodeTable map[uint64]map[uint64]string
}

func NewLouvainCommitteeModule(
	Ip_nodeTable map[uint64]map[uint64]string,
	Ss *signal.StopSignal,
	sl *supervisor_log.SupervisorLog,
	csvFilePath string,
	dataNum, batchNum, louvainFrequency int,
) *LouvainCommitteeModule {
	lg := new(partition.LouvainState)
	// 这里的 0.05 可以按你的实验再调
	lg.Init_LouvainState(0.05, 100, params.ShardNum)

	return &LouvainCommitteeModule{
		csvPath:                csvFilePath,
		dataTotalNum:           dataNum,
		batchDataNum:           batchNum,
		nowDataNum:             0,
		louvainGraph:           lg,
		modifiedMap:            make(map[string]uint64),
		louvainFreq:            louvainFrequency,
		louvainLastRunningTime: time.Time{},
		IpNodeTable:            Ip_nodeTable,
		Ss:                     Ss,
		sl:                     sl,
		curEpoch:               0,
	}
}

func (lcm *LouvainCommitteeModule) HandleOtherMessage([]byte) {}

func (lcm *LouvainCommitteeModule) fetchModifiedMap(key string) uint64 {
	if val, ok := lcm.modifiedMap[key]; !ok {
		return uint64(utils.Addr2Shard(key))
	} else {
		return val
	}
}

func (lcm *LouvainCommitteeModule) txSending(txlist []*core.Transaction) {
	sendToShard := make(map[uint64][]*core.Transaction)

	for idx := 0; idx <= len(txlist); idx++ {
		if idx > 0 && (idx%params.InjectSpeed == 0 || idx == len(txlist)) {
			for sid := uint64(0); sid < uint64(params.ShardNum); sid++ {
				it := message.InjectTxs{
					Txs:       sendToShard[sid],
					ToShardID: sid,
				}
				itByte, err := json.Marshal(it)
				if err != nil {
					log.Panic(err)
				}
				sendMsg := message.MergeMessage(message.CInject, itByte)
				//默认第一个节点为leader
				go networks.TcpDial(sendMsg, lcm.IpNodeTable[sid][0])
			}
			sendToShard = make(map[uint64][]*core.Transaction)
			//限速
			time.Sleep(time.Second)
		}

		if idx == len(txlist) {
			break
		}

		tx := txlist[idx]
		// 查询发送者id属于哪个分片
		senderSid := lcm.fetchModifiedMap(tx.Sender)
		sendToShard[senderSid] = append(sendToShard[senderSid], tx)
	}
}

func (lcm *LouvainCommitteeModule) MsgSendingControl() {
	txfile, err := os.Open(lcm.csvPath)
	if err != nil {
		log.Panic(err)
	}
	defer txfile.Close()

	reader := csv.NewReader(txfile)
	txlist := make([]*core.Transaction, 0)
	louvainCnt := 0

	for {
		data, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Panic(err)
		}

		if tx, ok := data2tx(data, uint64(lcm.nowDataNum)); ok {
			txlist = append(txlist, tx)
			lcm.nowDataNum++
		} else {
			continue
		}

		// batch sending
		if len(txlist) == int(lcm.batchDataNum) || lcm.nowDataNum == lcm.dataTotalNum {
			if lcm.louvainLastRunningTime.IsZero() {
				lcm.louvainLastRunningTime = time.Now()
			}
			lcm.txSending(txlist)

			txlist = make([]*core.Transaction, 0)
			lcm.Ss.StopGap_Reset()
		}

		// periodic Louvain partition
		if params.ShardNum > 1 &&
			!lcm.louvainLastRunningTime.IsZero() &&
			time.Since(lcm.louvainLastRunningTime) >= time.Duration(lcm.louvainFreq)*time.Second {

			lcm.louvainLock.Lock()
			louvainCnt++

			mmap, _ := lcm.louvainGraph.Louvain_Partition()
			lcm.louvainMapSend(mmap)

			for key, val := range mmap {
				lcm.modifiedMap[key] = val
			}

			lcm.louvainReset()
			lcm.louvainLock.Unlock()

			for atomic.LoadInt32(&lcm.curEpoch) != int32(louvainCnt) {
				time.Sleep(time.Second)
			}

			lcm.louvainLastRunningTime = time.Now()
			lcm.sl.Slog.Println("Next Louvain epoch begins.")
		}

		if lcm.nowDataNum == lcm.dataTotalNum {
			break
		}
	}

	// all transactions are sent, keep sending partition messages if needed
	for !lcm.Ss.GapEnough() {
		time.Sleep(time.Second)

		if params.ShardNum > 1 &&
			!lcm.louvainLastRunningTime.IsZero() &&
			time.Since(lcm.louvainLastRunningTime) >= time.Duration(lcm.louvainFreq)*time.Second {

			lcm.louvainLock.Lock()
			louvainCnt++

			mmap, _ := lcm.louvainGraph.Louvain_Partition()
			lcm.louvainMapSend(mmap)

			for key, val := range mmap {
				lcm.modifiedMap[key] = val
			}

			lcm.louvainReset()
			lcm.louvainLock.Unlock()

			for atomic.LoadInt32(&lcm.curEpoch) != int32(louvainCnt) {
				time.Sleep(time.Second)
			}

			lcm.sl.Slog.Println("Next Louvain epoch begins.")
			lcm.louvainLastRunningTime = time.Now()
		}
	}
}

func (lcm *LouvainCommitteeModule) louvainMapSend(m map[string]uint64) {
	pm := message.PartitionModifiedMap{
		PartitionModified: m,
	}
	pmByte, err := json.Marshal(pm)
	if err != nil {
		log.Panic(err)
	}

	sendMsg := message.MergeMessage(message.CPartitionMsg, pmByte)
	for i := uint64(0); i < uint64(params.ShardNum); i++ {
		go networks.TcpDial(sendMsg, lcm.IpNodeTable[i][0])
	}
	lcm.sl.Slog.Println("Supervisor: all Louvain partition map messages have been sent.")
}

func (lcm *LouvainCommitteeModule) louvainReset() {
	lcm.louvainGraph = new(partition.LouvainState)
	lcm.louvainGraph.Init_LouvainState(0.05, 100, params.ShardNum)

	// 把上一轮已经确定的账户分片关系保留为下一轮初始状态
	for key, val := range lcm.modifiedMap {
		lcm.louvainGraph.PartitionMap[partition.Vertex{Addr: key}] = int(val)
	}
}

func (lcm *LouvainCommitteeModule) HandleBlockInfo(b *message.BlockInfoMsg) {
	lcm.sl.Slog.Printf("Supervisor: received from shard %d in epoch %d.\n", b.SenderShardID, b.Epoch)

	if atomic.CompareAndSwapInt32(&lcm.curEpoch, int32(b.Epoch-1), int32(b.Epoch)) {
		lcm.sl.Slog.Println("this curEpoch is updated", b.Epoch)
	}

	if b.BlockBodyLength == 0 {
		return
	}

	lcm.louvainLock.Lock()
	defer lcm.louvainLock.Unlock()

	// 收集所有分片交易，更新全局交易图
	for _, tx := range b.InnerShardTxs {
		lcm.louvainGraph.AddEdge(
			partition.Vertex{Addr: tx.Sender},
			partition.Vertex{Addr: tx.Recipient},
		)
	}
	for _, r2tx := range b.Relay2Txs {
		lcm.louvainGraph.AddEdge(
			partition.Vertex{Addr: r2tx.Sender},
			partition.Vertex{Addr: r2tx.Recipient},
		)
	}
}
