package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import (
	"labrpc"
	"time"
	"math/rand"
	"sync/atomic"
)

// import "bytes"
// import "labgob"

const (
	ServerStateNone = iota
	ServerStateLeader
	ServerStateCandidate
	ServerStateFollower
)
var StateName = []string{"None", "Leader", "Candidate", "Follower"}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu          sync.Mutex          // Lock to protect shared access to this peer's state
	stateMu     sync.Mutex
	peers       []*labrpc.ClientEnd // RPC end points of all peers
	persister   *Persister          // Object to hold this peer's persisted state
	me          int                 // this peer's index into peers[]
	serverCount int
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state for all servers.
	currentTerm int
	voteFor     int
	log         []interface{}
	logTerm     []int

	// Volatile state on all servers.
	commitIndex int
	lastApplied int

	// Volatile state on leaders
	nextIndex   []int
	matchIndex  []int

	// Server state.
	state       int


	// Read only field
	electionTimePeriod   time.Duration
	electionTimeWave  time.Duration
	heartBeatPeriod  time.Duration

	// Timer
	timerMgr     *TimerMgr
	timerId      int

	// Elapse signature, to identify current time interval and detect server
	// time elapse. Every operation may cause potential expire of ongoing goroutine
	// should increase elapse signature to notify corresponding goroutine termination.
	// elapseSignature   int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isLeader bool
	// Your code here (2A).
	if rf.state == ServerStateLeader {
		isLeader = true
	} else {
		isLeader = false
	}

	term = rf.currentTerm

	return term, isLeader
}


//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term            int
	CandidateIndex  int
	LastLogTerm     int
	LastLogIndex    int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term            int
	VoteGranted     bool
}


type AppendEntriesArgs struct {
	Term            int
	LeaderId        int
	PrevLogIndex    int
	PrevLogTerm     int
	Entries         []interface{}
	LeaderCommit    int
}

type AppendEntriesReply struct {
	Term            int
	Success         bool
}
func (rf *Raft) LeaderRequestVoteHandler() {}

func (rf *Raft) CandidateRequestVoteHandler() {}

func (rf *Raft) FollowerRequestVoteHandler() {}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if (rf.voteFor != -1 && rf.voteFor != args.CandidateIndex) || args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
	} else {
		if args.Term > rf.currentTerm ||
			(args.Term == rf.currentTerm && args.LastLogIndex >= rf.lastApplied) { // at least up-to-date
			reply.VoteGranted = true
			rf.currentTerm = args.Term
			reply.Term = rf.currentTerm

			rf.ChangeState(rf.state, ServerStateFollower)
			rf.voteFor = args.CandidateIndex


		} else {
			reply.VoteGranted = false
			reply.Term = rf.currentTerm
		}
	}


	DPrintf(LogLevelInfo, rf, "Get RequestVote rpc from %v, Term %v, Reply %v", args.CandidateIndex, args.Term, *reply)

}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// TODO
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if len(args.Entries) == 0 { // heart beat message.
		switch rf.state {
		case ServerStateLeader:
			if args.Term == rf.currentTerm {
				DPrintf(LogLevelWarning, rf, "2 leader with identical term")
			}
			rf.ChangeState(rf.state, ServerStateFollower)
			rf.voteFor = args.LeaderId
			rf.currentTerm = args.Term

			reply.Term = args.Term
			reply.Success = true
		case ServerStateCandidate:
			rf.ChangeState(rf.state, ServerStateFollower)
			rf.voteFor = args.LeaderId
			rf.currentTerm = args.Term

			reply.Term = args.Term
			reply.Success = true
		case ServerStateFollower:
			if args.Term > rf.currentTerm ||
				(args.Term == rf.currentTerm && rf.voteFor == args.LeaderId){
				rf.ResetCurrentTimer()
				rf.voteFor = args.LeaderId
				rf.currentTerm = args.Term

				reply.Term = args.Term
				reply.Success = true
			} else {
				reply.Term = rf.currentTerm
				reply.Success = false
			}
		}
	} else {

	}

	DPrintf(LogLevelInfo, rf, "Get AppendEntries RPC from %v, Term %v, Reply %v\n", args.LeaderId, args.Term, *reply)
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	DPrintf(LogLevelInfo, rf, "Send request vote to server %v\n", server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}



func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntriesRecursive(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// TODO
	for {
		ret := rf.sendAppendEntries(server, args, reply)
		if ret {

		} else {

		}

	}
}

func(rf * Raft) sendAppendEntriesCallback(appendEntriesReplyList []AppendEntriesReply) {
	// TODO
}
//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	if rf.state != ServerStateLeader {
		isLeader = false
		return index, term, isLeader
	}



	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

// Generate Duration range in range
// [rf.electionTimeOut - rf.electionTimeWave, rf.electionTimeOut + rf.electionTimeWave]
func (rf *Raft) GetCandidateLoopPeriod() time.Duration {
	return (rf.electionTimePeriod - rf.electionTimeWave) +
		time.Duration(int64(time.Nanosecond) * rand.Int63n(rf.electionTimeWave.Nanoseconds() * 2))
}

func (rf *Raft) GetLeaderLoopPeriod() time.Duration {
	return rf.heartBeatPeriod
}

func (rf *Raft) GetFollowerLoopPeriod() time.Duration {
	return rf.GetCandidateLoopPeriod()
}

func (rf *Raft) StartElection() {
	// TODO expire termination.
	// TODO Lock service.

	if rf.state != ServerStateCandidate {
		DPrintf(LogLevelWarning, rf,"Raft state is not leader.")
		return
	}

	rf.mu.Lock()
	rf.currentTerm++
	rf.voteFor = rf.me
	rf.mu.Unlock()

	DPrintf(LogLevelInfo, rf, "Election start, current term %v\n", rf.currentTerm)

	var wg sync.WaitGroup
	requestVoteArgs := RequestVoteArgs{
		Term:				rf.currentTerm,
		CandidateIndex:		rf.me,
		LastLogIndex:		rf.lastApplied,
		LastLogTerm:		rf.logTerm[rf.lastApplied],
	}

	requestVoteReplyList := make([]RequestVoteReply, rf.serverCount)
	for idx := range requestVoteReplyList {
		requestVoteReplyList[idx].Term = -1
	}
	var voteNum int32 = 1
	forceStop := false


	for idx := range rf.peers {
		if idx == rf.me {
			continue
		}

		wg.Add(1)
		go func(serverIdx int) {
			RunUntil(func(args ...interface{})bool {
				return rf.sendRequestVote(args[0].(int), args[1].(*RequestVoteArgs), args[2].(*RequestVoteReply))
			}, func() bool {
				return forceStop || int(voteNum * 2) > rf.serverCount
			}, serverIdx, &requestVoteArgs, &requestVoteReplyList[serverIdx])

			if requestVoteReplyList[serverIdx].VoteGranted {
				DPrintf(LogLevelInfo, rf, "Get vote from server %v, Term %v\n", serverIdx, requestVoteReplyList[serverIdx].Term)
				atomic.AddInt32(&voteNum, 1)
			} else {
				DPrintf(LogLevelInfo, rf, "Server %v decline vote\n", serverIdx)
			}

			if requestVoteReplyList[serverIdx].Term != -1 && requestVoteReplyList[serverIdx].Term > rf.currentTerm {
				forceStop = true
				DPrintf(LogLevelInfo, rf, "Found server %v have a bigger term\n", serverIdx)

				rf.currentTerm = requestVoteReplyList[serverIdx].Term
				rf.ChangeState(rf.state, ServerStateFollower)
			}

			wg.Done()
		}(idx)
	}

	// TODO if this server cannot win election in time and get blocked
	// here should have a termination implementation exit from current method processing.
	wg.Wait()


	DPrintf(LogLevelInfo, rf, "Election end, get %v vote(s) out of %v\n", voteNum, rf.serverCount)

	if forceStop {
		return
	}

	// Win election
	if int(voteNum * 2) > rf.serverCount {
		rf.ChangeState(rf.state, ServerStateLeader)
		rf.SendHeartBeat()
	}
}

func (rf *Raft) SendHeartBeat() {
	if rf.state != ServerStateLeader {
		DPrintf(LogLevelWarning, rf, "Server state is not leader")
		return
	}

	appendEntriesReplyList := make([]AppendEntriesReply, rf.serverCount)


	appendEntriesArgs := AppendEntriesArgs{
		Term:				rf.currentTerm,
		LeaderId:			rf.me,
		PrevLogIndex:       rf.lastApplied,
		PrevLogTerm:        rf.logTerm[rf.lastApplied],
		LeaderCommit:       rf.commitIndex,
	}

	for idx := range rf.peers {
		if idx == rf.me {
			continue
		}

		go func(serverIdx int) {
			rf.sendAppendEntries(serverIdx, &appendEntriesArgs, &appendEntriesReplyList[serverIdx])
		}(idx)
	}

	// TODO check ret
	// rf.sendAppendEntriesCallback(appendEntriesReplyList)

}
func (rf *Raft) LeaderLoop() {

	if rf.state != ServerStateLeader {
		DPrintf(LogLevelWarning, rf, "Current state expect Leader, found %v\n", rf.state)
		return
	}
	DPrintf(LogLevelInfo, rf,"Leader loop start, current term %v.", rf.currentTerm)

	rf.SendHeartBeat()
}

func (rf *Raft) CandidateLoop() {


	if rf.state != ServerStateCandidate {
		DPrintf(LogLevelWarning, rf, "Current state expect Candidate, found %v\n", rf.state)
		return
	}
	DPrintf(LogLevelInfo, rf, "Candidate loop start.")

	rf.StartElection()
}

func (rf *Raft) FollowerLoop() {


	if rf.state != ServerStateFollower {
		DPrintf(LogLevelWarning, rf, "Current state expect Follower, found %v\n", rf.state)
		return
	}
	DPrintf(LogLevelInfo, rf, "Follower loop start.")

	rf.ChangeState(rf.state, ServerStateCandidate)
}

func (rf *Raft) ChangeState(oldState int, newState int) {
	rf.stateMu.Lock()
	defer rf.stateMu.Unlock()

	DPrintf(LogLevelInfo, rf,"State transfer from %v to %v\n", StateName[oldState], StateName[newState])
	if rf.state != oldState {
		DPrintf(LogLevelWarning, rf,"Server state not match, expect %v get %v\n", oldState, rf.state)
	}

	// TODO init for each state, such as rf.voteFor = -1 while convert to Follower.
	rf.state = newState
	// clean up
	switch oldState {
	case ServerStateLeader:
		rf.timerMgr.DelTimer(rf.timerId)
	case ServerStateCandidate:
		rf.timerMgr.DelTimer(rf.timerId)
	case ServerStateFollower:
		rf.timerMgr.DelTimer(rf.timerId)
	case ServerStateNone:
		// pass

	}

	// set on
	switch newState {
	case ServerStateLeader:
		rf.timerId = rf.timerMgr.AddTimer(rf.LeaderLoop, rf.GetLeaderLoopPeriod)
	case ServerStateCandidate:
		rf.timerId = rf.timerMgr.AddTimer(rf.CandidateLoop, rf.GetCandidateLoopPeriod)
	case ServerStateFollower:
		rf.timerId = rf.timerMgr.AddTimer(rf.FollowerLoop, rf.GetFollowerLoopPeriod)
		rf.voteFor = -1
	case ServerStateNone:
		// pass
	}
}

func (rf *Raft) ResetCurrentTimer() {
	rf.timerMgr.ResetTimer(rf.timerId)
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.serverCount = len(peers)
	rf.currentTerm = 0
	rf.voteFor = -1

	for idx := 0 ; idx < rf.serverCount ; idx++ {
		rf.nextIndex = append(rf.nextIndex, 0)
		rf.matchIndex = append(rf.matchIndex, 0)
	}

	rf.electionTimePeriod = time.Millisecond * 400
	rf.electionTimeWave = time.Millisecond * 100
	rf.heartBeatPeriod = time.Millisecond * 150

	rf.log = append(rf.log, 0)
	rf.logTerm = append(rf.logTerm, 0)



	rf.timerMgr = NewTimerMgr()
	go rf.timerMgr.Schedule()

	// Start from follower.
	rf.state = ServerStateNone
	rf.ChangeState(ServerStateNone, ServerStateFollower)


	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	return rf
}
