package signal

import (
	"blockEmulator/params"
	"sync"
)

// to judge when the listener to send the stop message to the leaders
type StopSignal struct {
	stoplock sync.Mutex // check the stopGap will not be modified by other processes

	stopGap       int // record how many empty txLists from leaders in a row
	stopThreshold int // the threshold

	stopEpoch          int //进行了多少epoch
	stopEpochThreshold int //运行多少epoch后停止
}

func NewStopSignal(stop_Threshold int) *StopSignal {
	return &StopSignal{
		stopGap:            0,
		stopThreshold:      stop_Threshold,
		stopEpoch:          0,
		stopEpochThreshold: params.StopEpochThreshold,
	}
}

// when receiving a message with an empty txList, then call this function to increase stopGap
func (ss *StopSignal) StopGap_Inc() {
	ss.stoplock.Lock()
	defer ss.stoplock.Unlock()
	ss.stopGap++
}

// when receiving a message with txs excuted, then call this function to reset stopGap
func (ss *StopSignal) StopGap_Reset() {
	ss.stoplock.Lock()
	defer ss.stoplock.Unlock()
	ss.stopGap = 0
}

// Check the stopGap is enough or not
// if StopGap is not less than stopThreshold, then the stop message should be sent to leaders.
func (ss *StopSignal) GapEnough() bool {
	ss.stoplock.Lock()
	defer ss.stoplock.Unlock()
	return ss.stopGap >= ss.stopThreshold
}

func (ss *StopSignal) EpochEnough() bool {
	ss.stoplock.Lock()
	defer ss.stoplock.Unlock()
	return ss.stopEpoch >= ss.stopEpochThreshold
}
