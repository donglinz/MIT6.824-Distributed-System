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

const (
	LogLevelDebug = iota
	LogLevelInfo
	LogLevelWarning
	LogLevelError
)

// Debugging
const LogLevelFilter = LogLevelDebug
var initFlag bool
const logFile  = "/home/ubuntu/Desktop/MIT6.824-Distributed-System/src/raft/log.log"

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
	if logLevel < LogLevelFilter {
		return
	}

	if rf == nil {
		DPrintfInner(fmt.Sprintf("Golang Testing log " + format, a...))
		return
	}
	_, file, line, _ := runtime.Caller(1)
	file = file[58:]
	switch logLevel {
	case LogLevelDebug:
		DPrintfInner(fmt.Sprintf("%v:%v: DEBUG: ServerId|State|Term|Sign: %v|%v|%v|%v ", file, line, rf.me, StateName[rf.state], rf.currentTerm, rf.timerMgr.GetTimerId()) + format, a...)
	case LogLevelInfo:
		DPrintfInner(fmt.Sprintf("%v:%v: INFO: ServerId|State|Term|Sign: %v|%v|%v|%v ", file, line, rf.me, StateName[rf.state], rf.currentTerm, rf.timerMgr.GetTimerId()) + format, a...)
	case LogLevelWarning:
		DPrintfInner(fmt.Sprintf("%v:%v: WARN: ServerId|State|Term|Sign: %v|%v|%v|%v ", file, line, rf.me, StateName[rf.state], rf.currentTerm, rf.timerMgr.GetTimerId()) + format, a...)
	case LogLevelError:
		DPrintfInner(fmt.Sprintf("%v:%v: ERROR: ServerId|State|Term|Sign: %v|%v|%v|%v ", file, line, rf.me, StateName[rf.state], rf.currentTerm, rf.timerMgr.GetTimerId()) + format, a...)
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
		mgr.mtx.Lock()
		defer mgr.mtx.Unlock()
		time.AfterFunc(mgr.intervalGen(), func(){ mgr.channel <- timerId })
	}(mgr.timerId)
}

func (mgr *TimerMgr) ResetCurrentEvent() {
	mgr.mtx.Lock()
	defer mgr.mtx.Unlock()
	mgr.timerId++

	go func(timerId int) {
		mgr.mtx.Lock()
		defer mgr.mtx.Unlock()
		time.AfterFunc(mgr.intervalGen(), func(){
			mgr.channel <- timerId
		})
	}(mgr.timerId)
}

func (mgr *TimerMgr) GetTimerId() int {
	mgr.mtx.Lock()
	defer mgr.mtx.Unlock()
	return mgr.timerId
}

func (mgr *TimerMgr) Schedule() {
	for {
		currentTimerId := 0
		timeout := false
		for {
			select {
				case currentTimerId = <- mgr.channel:

				case <- time.After(time.Second * 2):

					timeout = true
			}
			if timeout || currentTimerId == mgr.GetTimerId() {
				break
			}
		}

		if mgr.stop {
			for {
				timeout = false
				select {
					case currentTimerId = <- mgr.channel:

					case <- time.After(time.Second * 2):
						timeout = true
				}
				if timeout {
					break
				}
			}
			return
		}

		if timeout {
			continue
		}

		mgr.mtx.Lock()
		if currentTimerId == mgr.timerId {
			go func(callback func(int), para int) {
				callback(para)
			}(mgr.callback, currentTimerId)
		}
		mgr.mtx.Unlock()

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

func LockGroup(args ...sync.Locker) {
	for _, lock := range args {
		lock.Lock()
	}
}

func UnlockGroup(args ...sync.Locker) {
	for _, lock := range args {
		lock.Unlock()
	}
}

func Min(a int, b int) int{
	if a < b {
		return a
	}
	return b
}

func Max(a int, b int) int {
	if a > b {
		return a
	}
	return b
}

// a wait group implementation won't panic if delta < 0, goroutines block on
// wait will resume once delta <= 0
type WaitGroupPlus struct{
	mu 		sync.Mutex
	muCond	*sync.Cond
	delta	int
	counter int
}
func NewWaitGroupPlus() *WaitGroupPlus {
	wg := &WaitGroupPlus{}
	wg.muCond = sync.NewCond(&wg.mu)
	return wg
}
func (wg *WaitGroupPlus) Add(delta int) {
	wg.mu.Lock()
	defer wg.mu.Unlock()
	wg.delta += delta
	//if wg.delta <= 0 {
	//	wg.muCond.Broadcast()
	//}
	if delta < 0 {
		wg.counter -= delta
	}
}

func (wg *WaitGroupPlus) Wait() {
	wg.mu.Lock()
	defer wg.mu.Unlock()
	for ; ; {
		wg.muCond.Wait()
		if wg.delta <= 0 {
			break
		}
	}
}

// if will block after invoke wait, return true, else false.
func (wg *WaitGroupPlus) TryWait() bool {
	wg.mu.Lock()
	defer wg.mu.Unlock()
	return wg.delta > 0
}

func (wg *WaitGroupPlus) Counter() int {
	wg.mu.Lock()
	defer wg.mu.Unlock()
	return wg.counter
}

func (wg *WaitGroupPlus) Done() {
	wg.Add(-1)
	wg.muCond.Broadcast()
}

func (wg *WaitGroupPlus) ForceCancelWait() {
	wg.mu.Lock()
	wg.delta = 0
	wg.mu.Unlock()
	wg.muCond.Broadcast()
}