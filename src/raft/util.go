package raft

import (
	"log"
	"sync"
	"time"
	"os"
	"runtime"
	"fmt"
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
	_, file, line, _ := runtime.Caller(1)
	file = file[58:]
	switch logLevel {
	case LogLevelInfo:
		DPrintfInner(fmt.Sprintf("%v:%v: INFO: ServerId|Term: %v|%v ", file, line, rf.me, rf.currentTerm) + format, a...)
	case LogLevelWarning:
		DPrintfInner(fmt.Sprintf("%v:%v: WARN: ServerId|Term: %v|%v ", file, line, rf.me, rf.currentTerm) + format, a...)
	case LogLevelError:
		DPrintfInner(fmt.Sprintf("%v:%v: ERROR: ServerId|Term: %v|%v ", file, line, rf.me, rf.currentTerm) + format, a...)
	}
	return
}


type TimerMgr struct {
	mtx             sync.Mutex
	channel         chan int             // triggered timer.
	timerID         int
	callbackMap     map[int]func()
	intervalMap     map[int]func() time.Duration
	delCount        map[int]int
}

func NewTimerMgr() *TimerMgr {
	return &TimerMgr{
		callbackMap:	map[int]func() {},
		intervalMap:	map[int]func() time.Duration{},
		delCount:		map[int]int{},
		channel:		make(chan int),
	}
}

func (mgr *TimerMgr) AddTimer(callback func(), interval func() time.Duration) int {
	mgr.mtx.Lock()
	defer mgr.mtx.Unlock()
	mgr.timerID++
	timerId := mgr.timerID
	mgr.callbackMap[timerId] = callback
	mgr.intervalMap[timerId] = interval

	time.AfterFunc(interval(), func(){ mgr.channel <- timerId })

	return timerId
}

func (mgr *TimerMgr) ResetTimer(timerId int) {
	mgr.mtx.Lock()
	defer mgr.mtx.Unlock()
	mgr.delCount[timerId]++

	time.AfterFunc(mgr.intervalMap[timerId](), func(){ mgr.channel <- timerId })
}

func (mgr *TimerMgr) DelTimer(timerId int) {
	mgr.mtx.Lock()
	defer mgr.mtx.Unlock()
	mgr.delCount[timerId]++
	delete(mgr.callbackMap, timerId)
	delete(mgr.intervalMap, timerId)
}

// run this method in a particular goroutine.
func (mgr *TimerMgr) Schedule() {
	for {
		timerId := <- mgr.channel

		mgr.mtx.Lock()
		if mgr.delCount[timerId] > 0 {
			mgr.delCount[timerId]--
			if _, ok := mgr.callbackMap[timerId]; !ok {
				delete(mgr.delCount, timerId)
			}
			mgr.mtx.Unlock()
			continue
		}

		if _, ok := mgr.callbackMap[timerId]; ok {
			go mgr.callbackMap[timerId]()

			interval := mgr.intervalMap[timerId]
			time.AfterFunc(interval(), func(){ mgr.channel <- timerId })
		}

		mgr.mtx.Unlock()
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