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

import (
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

const (
	ElectionTimeoutMin = 150
	ElectionTimeoutMax = 300
)

const (
	ServerStateFollower = iota // 0
	ServerStateCandidate
	ServerStateLeader // 2
)

const UpdateHeartbeatInterval = 150

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	commandIndex int // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied  int // index of highest log entry applied to state (initialized to 0, increases monotonically)

	currentTerm       int           // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor          int           // candidateId that received vote in current term. null is -1
	log               []interface{} // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)
	serverState       int           // current server state 0: follower 1: candidate 2: leader
	lastActiveTime    time.Time     // Last time server recieved msg from leader or higher term server
	leaderStateRecord LeaderState
}

type LeaderState struct {
	nextIndex  []int // for each server, index of the next log entry to send to that server. initialized to leader last log index + 1
	matchIndex []int // for each server, index of highest log entry known to be replicated on server. initialized to 0, increases monotonically
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isleader = false
	term = rf.currentTerm
	if rf.serverState == ServerStateLeader {
		isleader = true
	}
	return term, isleader
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
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate’s term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // Requested server currentTerm
	VoteGranted bool // whether get vote
}

//
// RequestVote RPC handler.
//
// if agrs.Term < currentTerm. Reject this request.
//
// if agrs.Term > currentTerm. Grant vote and update state.
//
// if args.Term == rf.currentTerm. Follow the paper, checker votedFor is null(-1) or has voted.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("Server %v get RequestVote in term %v in state %v. argsTerm is %v. CandidateId is %v", rf.me, rf.currentTerm, rf.serverState, args.Term, args.CandidateId)
	reply.Term = rf.currentTerm // reply currentTerm
	if args.Term > rf.currentTerm {
		// Recieve bigger term
		// Grant vote
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		// Update state.
		rf.serverState = ServerStateFollower // transfer to Follower
		rf.currentTerm = args.Term           // Update currentTerm
		rf.lastActiveTime = time.Now()       // Update election timeout
		reply.Term = rf.currentTerm
		DPrintf("Server %v get RequestVote. Get High Term %v. Grant vote %v", rf.me, args.Term, args.CandidateId)
	} else if args.Term == rf.currentTerm {
		// Determine whether it has voted
		if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
			// Deny vote
			DPrintf("Server %v get RequestVote. Refuse it for rf.votedFor is %v", rf.me, rf.votedFor)
			reply.VoteGranted = false
		} else {
			// accept it
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			rf.serverState = ServerStateFollower // transfer to Follower
			rf.currentTerm = args.Term           // Update currentTerm
			rf.lastActiveTime = time.Now()       // Update election timeout
			reply.Term = rf.currentTerm
			DPrintf("Server %v get RequestVote. Grant vote %v", rf.me, args.CandidateId)
		}
	} else {
		// agrs.Term < currentTerm. Reject this request.
		DPrintf("Server %v get RequestVote. Refuse it for rf.currentTerm is %v", rf.me, rf.currentTerm)
		reply.VoteGranted = false
	}
}

type RequestAppendEntriesArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate’s term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestAppendEntriesReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// RequestAppendEntries RPC handler. Only for 2A at this time
//
// if agrs.Term < currentTerm. Reject this request.
//
// if agrs.Term > currentTerm. Follow this leader.
//
// if args.Term == rf.currentTerm. Check whether serverState. if not Leader, follow this leader.
//
func (rf *Raft) RequestAppendEntries(args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("Server %v get RequestAppendEntries in term %v in state %v. args is %v", rf.me, rf.currentTerm, rf.serverState, args)
	reply.Term = rf.currentTerm // reply currentTerm
	if args.Term > rf.currentTerm {
		// Recieve bigger term
		DPrintf("Server %v get RequestAppendEntries in term %v in state %v. args.Term > rf.currentTerm. To be follower", rf.me, rf.currentTerm, rf.serverState)
		rf.serverState = ServerStateFollower // transfer to Follower
		rf.currentTerm = args.Term           // Update currentTerm
		rf.lastActiveTime = time.Now()       // Update election timeout
	} else if args.Term == rf.currentTerm {
		if rf.serverState == ServerStateLeader {
			// I am Leader. Reject it. There may be a brain-splited
			DPrintf("Server %v get RequestAppendEntries in term %v in state %v. Refuse it for state in Leader", rf.me, rf.currentTerm, rf.serverState)
		} else {
			// follow this leader.
			rf.serverState = ServerStateFollower // transfer to Follower
			rf.lastActiveTime = time.Now()       // Update election timeout
			DPrintf("Server %v get RequestAppendEntries in term %v in state %v. Update lastActiveTime", rf.me, rf.currentTerm, rf.serverState)
		}
	} else {
		// agrs.Term < currentTerm. Reject this request.
		DPrintf("Server %v get RequestAppendEntries in term %v in state %v. Refuse it for rf.currentTerm > args.Term %v", rf.me, rf.currentTerm, rf.serverState, args.Term)
	}

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
func (rf *Raft) sendRequestVote(serverId int, args *RequestVoteArgs, reply *RequestVoteReply, cond *sync.Cond, countVote *int, finish *int) bool {
	rf.mu.Lock()
	// make sure state still is candidate
	if rf.serverState != ServerStateCandidate {
		DPrintf("Candidate ID is %v in state %v. Election Term is %v. Stop sending RequestVote() to Server %v for state %v", rf.me, rf.serverState, rf.currentTerm, serverId, rf.serverState)
		cond.Broadcast() // to awake routine is stucked by cond.wait()
		rf.mu.Unlock()
		return false
	}

	DPrintf("Candidate ID is %v in state %v. Election Term is %v. Send RequestVote() to Server %v", rf.me, rf.serverState, rf.currentTerm, serverId)
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	args.LastLogIndex = rf.leaderStateRecord.matchIndex[rf.me]
	args.LastLogTerm = rf.currentTerm - 1
	rf.mu.Unlock()
	ok := rf.peers[serverId].Call("Raft.RequestVote", args, reply) // PRC RequestVote. Ask for vote
	rf.mu.Lock()
	if ok {
		// Success.
		DPrintf("Candidate ID is %v in state %v. Election Term is %v. Send RequestVote() to Server %v Succeed!", rf.me, rf.serverState, rf.currentTerm, serverId)
		// confirm whether remote server has higher term.
		if rf.currentTerm < reply.Term {
			// get higher term.
			rf.serverState = ServerStateFollower // transfer to Follower
			rf.currentTerm = reply.Term          // Update currentTerm
			rf.lastActiveTime = time.Now()       // Update election timeout
			DPrintf("Candidate ID is %v in state %v. Election Term is %v. Get higher term. To be follower", rf.me, rf.serverState, rf.currentTerm)
		} else if reply.VoteGranted {
			// get a vote
			*countVote = *countVote + 1
		}
	} else {
		// Fail.
		DPrintf("Candidate ID is %v in state %v. Election Term is %v. Send RequestVote() to Server %v Fail!", rf.me, rf.serverState, rf.currentTerm, serverId)
	}
	*finish = *finish + 1 // finish one RPC and ignore RPC state
	cond.Broadcast()      // to awake routine is stucked by cond.wait()
	rf.mu.Unlock()
	return ok
}

func (rf *Raft) sendRequestAppendEntries(serverId int, args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) bool {
	rf.mu.Lock()

	// make sure state still is candidate
	if rf.serverState != ServerStateLeader {
		DPrintf("Leader ID is %v in state %v. Leader Term is %v. Send sendRequestAppendEntries() to Server %v. Old leader stoped!", rf.me, rf.serverState, rf.currentTerm, serverId)
		rf.mu.Unlock()
		return false
	}

	DPrintf("Leader ID is %v in state %v. Leader Term is %v. Send sendRequestAppendEntries() to Server %v", rf.me, rf.serverState, rf.currentTerm, serverId)
	args.Term = rf.currentTerm
	rf.mu.Unlock()
	ok := rf.peers[serverId].Call("Raft.RequestAppendEntries", args, reply) // PRC RequestAppendEntries.
	rf.mu.Lock()
	if ok {
		// Success.
		DPrintf("Leader ID is %v in state %v. Leader Term is %v. Send sendRequestAppendEntries() to Server %v. Succeed!", rf.me, rf.serverState, rf.currentTerm, serverId)
		// confirm whether remote server has higher term.
		if rf.currentTerm < reply.Term {
			// get higher term.
			rf.serverState = ServerStateFollower // transfer to Follower
			rf.currentTerm = reply.Term          // Update currentTerm
			rf.lastActiveTime = time.Now()       // Update election timeout
			DPrintf("Leader ID is %v. Get Higher term %v from server %v", rf.me, reply.Term, serverId)
		}
	} else {
		// Fail.
		DPrintf("Leader ID is %v in state %v. Leader Term is %v. Send sendRequestAppendEntries() to Server %v. RPC Fail!", rf.me, rf.serverState, rf.currentTerm, serverId)
	}
	rf.mu.Unlock()
	return ok
}

//
// 1
// 2
//
func (rf *Raft) StartElection() {
	cond := sync.NewCond(&rf.mu) // create Cond with Locker rf.mu
	// vote for myself
	rf.mu.Lock()
	countVote := 1      // Number of votes obtained
	finish := 1         // Number of completed RPC
	rf.votedFor = rf.me // vote for rf.me
	DPrintf("Candidate ID is %v in state %v. Election Term is %v. Start!", rf.me, rf.serverState, rf.currentTerm)
	rf.mu.Unlock()

	// for each server start routine sendRequestVote()
	for i := 0; i < len(rf.peers); i++ {
		if rf.me != i {
			DPrintf("Candidate ID is %v in state %v. Election Term is %v. i is %v. go rf.sendRequestVote", rf.me, rf.serverState, rf.currentTerm, i)
			args := RequestVoteArgs{}
			reply := RequestVoteReply{}
			go rf.sendRequestVote(i, &args, &reply, cond, &countVote, &finish)
		}
	}

	rf.mu.Lock()
	for countVote <= len(rf.peers)/2 && finish != len(rf.peers) && rf.serverState == ServerStateCandidate {
		DPrintf("Candidate ID is %v in state %v. Election Term is %v. countVote is %v. finish is %v.", rf.me, rf.serverState, rf.currentTerm, countVote, finish)
		cond.Wait() // unlock rf.mu. block this thread until get cond signal
	}
	// confirm state
	if rf.serverState == ServerStateCandidate {
		// still in candidate
		if countVote > len(rf.peers)/2 {
			// win the election. become leader and send heartbeat to other server at once
			DPrintf("Candidate ID is %v in state %v. Election Term is %v. countVote is %v. finish is %v. Become Leader!", rf.me, rf.serverState, rf.currentTerm, countVote, finish)
			rf.serverState = ServerStateLeader
			go rf.HeartBeatTest()
		} else {
			DPrintf("Candidate ID is %v in state %v. Election Term is %v. countVote is %v. finish is %v. Lose in election!", rf.me, rf.serverState, rf.currentTerm, countVote, finish)
		}
	}
	rf.mu.Unlock()

}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
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

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	var recordTime time.Time // record this thread awake time
	for !rf.killed() {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		rf.mu.Lock()
		DPrintf("Server %v ticker() Awake! Term: %v State: %v\t rf.lastActiveTime: %v, recordTime: %v", rf.me, rf.currentTerm, rf.serverState, rf.lastActiveTime, recordTime)
		if !rf.lastActiveTime.IsZero() && rf.serverState != ServerStateLeader && !recordTime.Before(rf.lastActiveTime) {
			// didn't update lastActiveTime in electionTimeout. to start new election
			rf.serverState = ServerStateCandidate // transfer to be candidate
			rf.currentTerm++                      // increase term
			DPrintf("Sever %v. rf.serverState: %v\t To Candidate in term %v", rf.me, rf.serverState, rf.currentTerm)
			go rf.StartElection() // StartElection
		}

		rand.Seed(time.Now().Unix() + int64(rf.me*20000))
		randNum := (ElectionTimeoutMax - ElectionTimeoutMin + rand.Intn(ElectionTimeoutMin) + 1)
		electionTimeout := time.Millisecond * time.Duration(randNum) // rerandom election timeout
		rf.lastActiveTime = time.Now()                               // update lastActiveTime
		recordTime = rf.lastActiveTime
		DPrintf("Server %v ticker() Sleep! Term: %v State: %v\t rf.lastActiveTime: %v, recordTime: %v", rf.me, rf.currentTerm, rf.serverState, rf.lastActiveTime, recordTime)
		rf.mu.Unlock()
		time.Sleep(electionTimeout)
	}
}

//
// A timer regularly checks whether it is a leader.
// If it is leader start rountine  HeartBeatTest() to send
// headbeat to other servers.
//
func (rf *Raft) HeartBeatTestTimer() {
	for !rf.killed() {
		rf.mu.Lock()
		DPrintf("Server %v State %v Term %v HeartBeatTestTimer() Awake", rf.me, rf.serverState, rf.currentTerm)
		if rf.serverState == ServerStateLeader {
			// leader start HeartBeatTest()
			DPrintf("Leader ID is %v in term %v. HeartBeatTest() Start", rf.me, rf.currentTerm)
			go rf.HeartBeatTest()
		}
		rf.mu.Unlock()
		time.Sleep(UpdateHeartbeatInterval * time.Millisecond)
	}
}

//
// sned AppendEntries RPCs that carry no log entries
// to prevent election timeouts
//
func (rf *Raft) HeartBeatTest() {
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			args := RequestAppendEntriesArgs{}
			reply := RequestAppendEntriesReply{}
			go rf.sendRequestAppendEntries(i, &args, &reply)
		}
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.lastApplied = 0
	rf.commandIndex = 0
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]interface{}, 0)
	rf.lastActiveTime = time.Time{}
	rf.serverState = ServerStateFollower
	rf.leaderStateRecord.matchIndex = make([]int, len(rf.peers))
	rf.leaderStateRecord.nextIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.leaderStateRecord.nextIndex); i++ {
		rf.leaderStateRecord.nextIndex[i] = 1
	}
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	DPrintf("Raft  %v Make() initialization finished!\n", rf.me)
	// start ticker goroutine to start elections
	go rf.ticker()             // routine check whether elcetion time elapse.
	go rf.HeartBeatTestTimer() // routine send heartbeat regularly if sever is leader
	return rf
}
