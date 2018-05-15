package raft

import (
	"log"
	"os"
	"runtime"
	"fmt"
	"sync"
	"time"
	"math/rand"
)

// Debugging
const Debug = 1
var initFlag bool
const logFile  = "/home/ubuntu/Desktop/MIT6.824-Distributed-System/src/raft/log.log"
const (
	LogLevelInfo = iota
	LogLevelWarning
	LogLevelError
)
func DPrintfInner(format string, a ...interface{}) (n int, err error) {
	if !initFlag {
		initFlag = true

		if len(logFile) > 0 {
			fd, err := os.Create(logFile)
			if err != nil {
				panic(err)
			}
			log.SetOutput(fd)

		}
	}
	log.Printf(format, a...)

	return
}

func DPrintf(logLevel int, rf *Raft, format string, a ...interface{}) (n int, err error) {
	if Debug <= 0 {
		return
	}

	if rf == nil {
		DPrintfInner(fmt.Sprintf("Testing log " + format, a...))
		return
	}
	_, file, line, _ := runtime.Caller(1)
	file = file[58:]
	switch logLevel {
	case LogLevelInfo:
		DPrintfInner(fmt.Sprintf("%v:%v: INFO: ServerId|State|Term: %v|%v|%v ", file, line, rf.me, StateName[rf.state], rf.currentTerm) + format, a...)
	case LogLevelWarning:
		DPrintfInner(fmt.Sprintf("%v:%v: WARN: ServerId|State|Term: %v|%v|%v ", file, line, rf.me, StateName[rf.state], rf.currentTerm) + format, a...)
	case LogLevelError:
		DPrintfInner(fmt.Sprintf("%v:%v: ERROR: ServerId|State|Term: %v|%v|%v ", file, line, rf.me, StateName[rf.state], rf.currentTerm) + format, a...)
	}
	return
}


type TimerMgr struct {
	mtx             sync.Mutex
	channel         chan int             	// triggered timer.

	callback     	func(int)
	intervalGen  	func() time.Duration
	timerId         int 					// event identifier
	stop			bool
}
func NewTimerMgr() *TimerMgr {
	return &TimerMgr{
		channel:		make(chan int, 10),
	}
}
func (mgr *TimerMgr) SetEvent(callback func(int), intervalGen func() time.Duration) {
	mgr.mtx.Lock()
	defer mgr.mtx.Unlock()

	mgr.timerId++
	mgr.callback = callback
	mgr.intervalGen = intervalGen

	go func(timerId int) {
		time.AfterFunc(mgr.intervalGen(), func(){ mgr.channel <- timerId })
	}(mgr.timerId)
}

func (mgr *TimerMgr) ResetCurrentEvent() {
	mgr.mtx.Lock()
	defer mgr.mtx.Unlock()
	mgr.timerId++

	go func(timerId int) {
		time.AfterFunc(mgr.intervalGen(), func(){
			mgr.channel <- timerId
		})
	}(mgr.timerId)
}

func (mgr *TimerMgr) Schedule() {
	for {
		if mgr.stop {
			return
		}
		currentTimerId := 0
		for {
			currentTimerId = <- mgr.channel
			if currentTimerId == mgr.timerId {
				//fmt.Println(currentTimerId, mgr.timerId)
				break
			}
		}

		if mgr.stop {
			return
		}
		// unlock in callback goroutine
		go mgr.callback(currentTimerId)


		go func(timerId int) {
			mgr.mtx.Lock()
			time.AfterFunc(mgr.intervalGen(), func(){
				mgr.mtx.Lock()
				if mgr.timerId == timerId {
					mgr.timerId++
					mgr.channel <- timerId + 1
				}
				mgr.mtx.Unlock()
			})
			mgr.mtx.Unlock()
		}(currentTimerId)
	}
}


func RunUntil(fc interface{}, stopCondition func() bool, args ...interface{}) {
	for {
		if stopCondition() {
			break
		}

		var ret bool
		if len(args) > 1 {
			ret = fc.(func(...interface{})bool)(args...)
		} else if len(args) == 1 {
			ret = fc.(func(interface{})bool)(args[0])
		} else {
			ret = fc.(func()bool)()
		}

		if ret {
			break
		}

	}
}


var seedMu sync.Mutex
var seedFlag = 0
func InitRandSeed() {
	if seedFlag == 0 {
		seedMu.Lock()
		if seedFlag == 0 {
			rand.Seed(time.Now().UnixNano())
			seedFlag = 1
		}
		seedMu.Unlock()
	}
}