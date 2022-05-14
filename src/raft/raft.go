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
	"bytes"
	"errors"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
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

const UpdateHeartbeatInterval = 60
const UpdateSnapShotInterval = 400
const MaxLogLen = 50

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

	// Volatile state on all servers
	commandIndex int // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied  int // index of highest log entry applied to state (initialized to 0, increases monotonically)

	// Persistent state on all servers
	currentTerm int        // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor    int        // candidateId that received vote in current term. null is -1
	log         []LogEntry // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)

	// Volatile state on leaders.
	leaderStateRecord LeaderState

	//  define by MH
	serverState    int       // current server state 0: follower 1: candidate 2: leader
	lastActiveTime time.Time // Last time server recieved msg from leader's heatrneat and server granted vote
	lastLogIndex   int

	applyCh chan ApplyMsg

	// For Debug
	heartbeatTimes int
}

//
// Volatile state on leaders.
// Reinitialized after election
//
type LeaderState struct {
	nextIndex  []int // for each server, index of the next log entry to send to that server. initialized to leader last log index + 1
	matchIndex []int // for each server, index of highest log entry known to be replicated on server. initialized to 0, increases monotonically
}

type LogEntry struct {
	Term         int         // term number when the entry was received by the leader
	Command      interface{} //
	CommandIndex int         //  index identifying its position in the log
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
	LastLogTerm  int // term of candidate’s last log entry
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

type RequestAppendEntriesArgs struct {
	// Your data here (2A, 2B).
	Term         int        // leader’s term
	LeaderID     int        // so follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // prevLogTerm
	Entries      []LogEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        // leader’s commitIndex
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestAppendEntriesReply struct {
	// Your data here (2A).
	Term    int  //  for leader to update itself
	Success bool // true if follower contained entry matching  prevLogIndex and prevLogTerm
	// optimize for 2C
	ConflictingTerm       int // the term of the conflicting entry
	ConflictingFisrtIndex int // the first index it stores for that term
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastLogIndex)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
	// DPrintf("Server %v. State: %v. Term: %v. persist(). rf.currentTerm: %v. rf.votedFor: %v. rf.loglen: %v ", rf.me, rf.serverState, rf.currentTerm, rf.currentTerm, rf.votedFor, len(rf.log))
	DPrintf("Server %v. State: %v. Term: %v. persist(). rf.currentTerm: %v. rf.votedFor: %v. CommitIndex: %v. rf.log: %v.", rf.me, rf.serverState, rf.currentTerm, rf.currentTerm, rf.votedFor, rf.commandIndex, rf.log)
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var voteFor int
	var log []LogEntry
	var lastLogIndex int
	if d.Decode(&currentTerm) != nil || d.Decode(&voteFor) != nil || d.Decode(&log) != nil || d.Decode(&lastLogIndex) != nil {
		rf.currentTerm = 0
		rf.votedFor = -1
		rf.log = make([]LogEntry, 1)
		rf.lastLogIndex = 0
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = voteFor
		rf.log = log
		rf.lastLogIndex = lastLogIndex
	}
	// DPrintf("Server %v. State: %v. Term: %v. readPersist(). rf.currentTerm: %v. rf.votedFor: %v. rf.loglen: %v ", rf.me, rf.serverState, rf.currentTerm, rf.currentTerm, rf.votedFor, len(rf.log))
	DPrintf("Server %v. State: %v. Term: %v. readPersist(). rf.currentTerm: %v. rf.votedFor: %v. rf.log: %v ", rf.me, rf.serverState, rf.currentTerm, rf.currentTerm, rf.votedFor, rf.log)
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

//
// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
// The index argument indicates the highest log entry that's reflected in the snapshot.
//
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("Server %v. State: %v. Term: %v. Get RequestVote from Server %v. ", rf.me, rf.serverState, rf.currentTerm, args.CandidateId)
	reply.Term = rf.currentTerm // reply currentTerm
	reply.VoteGranted = false

	// agrs.Term < currentTerm. Reject this request.
	if args.Term < rf.currentTerm {
		DPrintf("Server %v. State: %v. Term: %v. Get RequestVote from Server %v. Refuse it for args.Term %v < rf.currentTerm is %v", rf.me, rf.serverState, rf.currentTerm, args.CandidateId, args.Term, rf.currentTerm)
		return
	}

	if args.Term == rf.currentTerm {
		// check whether it has voted
		if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
			// Deny vote
			DPrintf("Server %v. State: %v. Term: %v. Get RequestVote from Server %v. Refuse it for rf.votedFor is %v", rf.me, rf.serverState, rf.currentTerm, args.CandidateId, rf.votedFor)
			return
		}
	}

	// rf.BeFollower(args.Term)
	// can't use func BeFollower(), because it can't update lastActiveTime at this time.
	// if server update lastActiveTime at this moment, it couldn't start StartElection() when it deals with lost of RPC RequestVote
	rf.serverState = ServerStateFollower // transfer to Follower
	rf.currentTerm = args.Term           // Update currentTerm
	rf.persist()

	// check up-to-date
	// If the logs have last entries with different terms, then the log with the later term is more up-to-date.
	entry, _ := rf.GetLogEntry(rf.lastLogIndex)
	if args.LastLogTerm < entry.Term {
		// Deny vote
		DPrintf("Server %v. State: %v. Term: %v. Get RequestVote from Server %v. Refuse it for up-to-date. args.LastLogTerm: %v rf.log[rf.lastLogIndex].Term: %v lastIndex: %v", rf.me, rf.serverState, rf.currentTerm, args.CandidateId, args.LastLogTerm, entry.Term, rf.lastLogIndex)
		return
	}
	// If the logs end with the same term, then whichever log is longer is more up-to-date.
	if args.LastLogTerm == entry.Term && args.LastLogIndex < rf.lastLogIndex {
		// Deny vote
		DPrintf("Server %v. State: %v. Term: %v. Get RequestVote from Server %v. Refuse it for up-to-date. args.LastLogIndex: %v < rf.lastLogIndex: %v", rf.me, rf.serverState, rf.currentTerm, args.CandidateId, args.LastLogIndex, rf.lastLogIndex)
		return
	}

	// Finally, grant vote
	reply.VoteGranted = true
	rf.votedFor = args.CandidateId
	rf.lastActiveTime = time.Now() // Update election timeout
	rf.persist()
	DPrintf("Server %v. State: %v. Term: %v. Get RequestVote from Server %v. Grant vote", rf.me, rf.serverState, rf.currentTerm, args.CandidateId)
}

func (rf *Raft) RequestAppendEntries(args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("Server %v. State: %v. Term: %v. Get RequestAppendEntries from Server %v.\n AppendEntriesInfo: rf.log len: %v. rf.lastLogIndex: %v. len args.Entries: %v. args.LeaderCommit: %v. args.preLogIndex: %v", rf.me, rf.serverState, rf.currentTerm, args.LeaderID, len(rf.log), rf.lastLogIndex, len(args.Entries), args.LeaderCommit, args.PrevLogIndex)
	reply.Term = rf.currentTerm // reply currentTerm
	reply.Success = false
	reply.ConflictingTerm = 0
	reply.ConflictingFisrtIndex = 0

	if args.Term < rf.currentTerm {
		// agrs.Term < currentTerm. Reject this request.
		DPrintf("Server %v. State: %v. Term: %v. Get RequestAppendEntries from Server %v. Refuse it for args.Term %v < rf.currentTerm %v", rf.me, rf.serverState, rf.currentTerm, args.LeaderID, args.Term, rf.currentTerm)
		return
	}

	if args.Term == rf.currentTerm {
		if rf.serverState == ServerStateLeader {
			// I am Leader. Reject it. There may be a brain-splited
			DPrintf("Server %v. State: %v. Term: %v. Get RequestAppendEntries from Server %v. Refuse it for state in Leader", rf.me, rf.serverState, rf.currentTerm, args.LeaderID)
			return
		}
	}

	rf.BeFollower(args.Term, args.LeaderID)
	rf.persist()
	DPrintf("Server %v. State: %v. Term: %v. Get RequestAppendEntries from Server %v. Update lastActiveTime", rf.me, rf.serverState, rf.currentTerm, args.LeaderID)

	// for old entries
	// Reply false if args.PrevLogIndex > lastLogIndex
	if args.PrevLogIndex > rf.lastLogIndex {
		// optimize for Part2C
		reply.ConflictingTerm = -1
		reply.ConflictingFisrtIndex = rf.lastLogIndex
		DPrintf("Server %v. State: %v. Term: %v. Get RequestAppendEntries from Server %v. Reply false if args.PrevLogIndex: %v > lastLogIndex: %v", rf.me, rf.serverState, rf.currentTerm, args.LeaderID, args.PrevLogIndex, rf.lastLogIndex)
		return
	}
	// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
	matchLogEntry, err := rf.GetLogEntry(args.PrevLogIndex)
	if err != nil {
		// should snapshot install
		DPrintf("Server %v. State: %v. Term: %v. Get RequestAppendEntries from Server %v. Reply false err: %v", rf.me, rf.serverState, rf.currentTerm, args.LeaderID, err)
		return
	}
	if args.PrevLogTerm != matchLogEntry.Term {
		// optimize for Part2C
		reply.ConflictingTerm = matchLogEntry.Term
		conflictFirstIndex := args.PrevLogIndex - 1
		for ; conflictFirstIndex > 0; conflictFirstIndex-- {
			entry, err := rf.GetLogEntry(conflictFirstIndex)
			if err != nil {
				// should snapshot install
				DPrintf("Server %v. State: %v. Term: %v. Get RequestAppendEntries from Server %v. Conflict. entryIndex: %v err: %v", rf.me, rf.serverState, rf.currentTerm, args.LeaderID, conflictFirstIndex, err)
				break
			}
			if entry.Term != matchLogEntry.Term {
				conflictFirstIndex++
				break
			}
		}
		reply.ConflictingFisrtIndex = conflictFirstIndex
		DPrintf("Server %v. State: %v. Term: %v. Get RequestAppendEntries from Server %v. Reply false if args.PrevLogTerm: %v != rf.log[args.PrevLogIndex].Term: %v", rf.me, rf.serverState, rf.currentTerm, args.LeaderID, args.PrevLogTerm, matchLogEntry.Term)
		return
	}

	// Add new entries
	// upon receiving a heartbeat, they would truncate the follower’s log following prevLogIndex, and then append any entries included in the AppendEntries arguments.
	// This is also not correct.
	// rf.AppendLogEntryToRaftLog(args.PrevLogIndex+1, args.Entries)
	for i, leaderEntry := range args.Entries {
		index := i + args.PrevLogIndex + 1
		if index > rf.lastLogIndex {
			rf.AppendLogEntryToRaftLog(rf.lastLogIndex+1, make([]LogEntry, 1))
			rf.SetRaftLogEntry(rf.lastLogIndex, leaderEntry.Term, leaderEntry.Command, leaderEntry.CommandIndex)
		} else {
			compareEntry, err1 := rf.GetLogEntry(index)
			if err1 != nil {
				DPrintf("Server %v. State: %v. Term: %v. Get RequestAppendEntries from Server %v. Append Error: %v", rf.me, rf.serverState, rf.currentTerm, args.LeaderID, err1)
				return
			}
			if compareEntry.Term == leaderEntry.Term {
				continue
			}

			rf.AppendLogEntryToRaftLog(index, make([]LogEntry, 1))
			rf.SetRaftLogEntry(rf.lastLogIndex, leaderEntry.Term, leaderEntry.Command, leaderEntry.CommandIndex)
			DPrintf("Server %v. State: %v. Term: %v. Get RequestAppendEntries from Server %v. Delete the existing entry: %v. New Entry: %v", rf.me, rf.serverState, rf.currentTerm, args.LeaderID, compareEntry, leaderEntry)
		}
	}

	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	// It means server need to commit.
	if args.LeaderCommit > rf.commandIndex {
		DPrintf("Server %v. State: %v. Term: %v. Get RequestAppendEntries from Server %v. Change CommitIndex: %v. args.LeaderCommit: %v", rf.me, rf.serverState, rf.currentTerm, args.LeaderID, rf.commandIndex, args.LeaderCommit)
		rf.CommitLogEntries(MinInt(args.LeaderCommit, rf.lastLogIndex))
	}
	reply.Success = true
	rf.persist()
	// DPrintf("Server %v. State: %v. Term: %v. Get RequestAppendEntries from Server %v. End \n AppendEntriesEndInfo: rf.log %v. ", rf.me, rf.serverState, rf.currentTerm, args.LeaderID, rf.log)
	DPrintf("Server %v. State: %v. Term: %v. Get RequestAppendEntries from Server %v. CommitIndex: %v End.", rf.me, rf.serverState, rf.currentTerm, args.LeaderID, rf.commandIndex)
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
func (rf *Raft) sendRequestVote(serverId int, cond *sync.Cond, countVote *int, finish *int) bool {
	rf.mu.Lock()

	// make sure state still is candidate
	if rf.serverState != ServerStateCandidate {
		DPrintf("Server %v. State: %v. Term: %v. Stop sending RequestVote() to Server %v for state %v", rf.me, rf.serverState, rf.currentTerm, serverId, rf.serverState)
		cond.Broadcast() // to awake routine is stucked by cond.wait()
		rf.mu.Unlock()
		return false
	}

	DPrintf("Server %v. State: %v. Term: %v. Send RequestVote() to Server %v", rf.me, rf.serverState, rf.currentTerm, serverId)
	entry, _ := rf.GetLogEntry(rf.lastLogIndex)
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.lastLogIndex,
		LastLogTerm:  entry.Term,
	}
	reply := RequestVoteReply{}

	rf.mu.Unlock()

	ok := rf.peers[serverId].Call("Raft.RequestVote", &args, &reply) // PRC RequestVote. Ask for vote

	rf.mu.Lock()
	defer rf.mu.Unlock()

	*finish = *finish + 1 // finish one RPC and ignore RPC state
	if !ok {
		// RPC Fail.
		DPrintf("Server %v. State: %v. Term: %v. Send RequestVote() to Server %v Fail!", rf.me, args.Term, rf.currentTerm, serverId)
		return ok
	}

	// PRC Success.
	DPrintf("Server %v. State: %v. Term: %v. Send RequestVote() to Server %v Succeed!", rf.me, rf.serverState, rf.currentTerm, serverId)
	// confirm whether remote server has higher term.
	if rf.currentTerm < reply.Term {
		// get higher term.
		rf.BeFollower(reply.Term, -1)
		rf.persist()
		DPrintf("Server %v. State: %v. Term: %v. Get higher term. To be follower", rf.me, rf.serverState, rf.currentTerm)
	}

	if reply.VoteGranted {
		// get a vote
		*countVote = *countVote + 1
	}

	cond.Broadcast() // to awake routine is stucked by cond.wait()
	return ok
}

func (rf *Raft) sendRequestAppendEntries(serverId int, cond *sync.Cond, countAgree *int, finish *int, heartbeatID int) bool {
	counterRPC := 1
	rand.Seed(time.Now().Unix() + int64(serverId*20000) + int64(heartbeatID*60000))
	taskId := rand.Int63n(100000)
	for !rf.killed() {
		rf.mu.Lock()

		DPrintf("Server %v. State: %v. Term: %v. Send RequestAppendEntries() to Server %v. Times: %v! Get Lock! HeartBeatID: %v. TaskID: %v", rf.me, rf.serverState, rf.currentTerm, serverId, counterRPC, heartbeatID, taskId)
		// make sure state still is candidate or follower
		if rf.serverState != ServerStateLeader {
			DPrintf("Server %v. State: %v. Term: %v. Send RequestAppendEntries() to Server %v. Times: %v! Stoped! rf.serverState != ServerStateLeader HeartBeatID: %v. TaskID: %v", rf.me, rf.serverState, rf.currentTerm, serverId, counterRPC, heartbeatID, taskId)
			cond.Broadcast()
			rf.mu.Unlock()
			return false
		}
		args := RequestAppendEntriesArgs{}
		reply := RequestAppendEntriesReply{}

		args.Term = rf.currentTerm
		args.LeaderID = rf.me
		// args.PrevLogIndex = MinInt(len(rf.log)-1, rf.leaderStateRecord.nextIndex[serverId])
		args.PrevLogIndex = MaxInt(MinInt(rf.lastLogIndex, rf.leaderStateRecord.nextIndex[serverId]-1), 0)
		preEntry, err1 := rf.GetLogEntry(args.PrevLogIndex)
		if err1 != nil {
			// need to install snapshot
			DPrintf("Server %v. State: %v. Term: %v. Send RequestAppendEntries() to Server %v. Times: %v! STOP! NEED TO INSALL SNAPSHOT! args.PrevLogIndex: %v, LeaderCommit: %v HeartBeatID: %v. TaskID: %v", rf.me, rf.serverState, rf.currentTerm, serverId, counterRPC, args.PrevLogIndex, rf.commandIndex, heartbeatID, taskId)
			*finish = *finish + 1
			cond.Broadcast()
			rf.mu.Unlock()
			return false
		}

		args.PrevLogTerm = preEntry.Term
		start := args.PrevLogIndex + 1
		end := rf.lastLogIndex + 1
		if args.PrevLogIndex == 0 {
			start = rf.GetLogFirstEntryIndex()
		}
		// can't include [{0 <nil> 0}]
		entries := make([]LogEntry, 0)
		if start != 0 {
			entries, _ = rf.GetLogEntrise(start, end)
		}

		args.Entries = entries // If last log index ≥ nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex
		args.LeaderCommit = rf.commandIndex
		DPrintf("Server %v. State: %v. Term: %v. Send RequestAppendEntries() to Server %v. Times: %v! args.PrevLogIndex: %v, args.PrevLogTerm: %v, args.Entries: %v args.LeaderCommit: %v HeartBeatID: %v. TaskID: %v", rf.me, rf.serverState, rf.currentTerm, serverId, counterRPC, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries), args.LeaderCommit, heartbeatID, taskId)

		rf.mu.Unlock()

		ok := rf.peers[serverId].Call("Raft.RequestAppendEntries", &args, &reply) // PRC RequestAppendEntries.

		rf.mu.Lock()

		if !ok {
			// RPC Fail.
			DPrintf("Server %v. State: %v. Term: %v. Send RequestAppendEntries() to Server %v. Times: %v! RPC Fail! HeartBeatID: %v. TaskID: %v", rf.me, rf.serverState, rf.currentTerm, serverId, counterRPC, heartbeatID, taskId)
			*finish = *finish + 1 // finish one RPC and ignore RPC state
			cond.Broadcast()
			rf.mu.Unlock()
			return ok
		}

		// RPC Success.
		DPrintf("Server %v. State: %v. Term: %v. Send RequestAppendEntries() to Server %v. Times: %v! RPC Succeed! HeartBeatID: %v. TaskID: %v", rf.me, rf.serverState, args.Term, serverId, counterRPC, heartbeatID, taskId)

		// confirm whether remote server has higher term.
		if rf.currentTerm < reply.Term {
			// get higher term.
			rf.BeFollower(reply.Term, -1)
			rf.persist()
			*finish = *finish + 1 // finish one RPC and ignore RPC state
			DPrintf("Server %v. State: %v. Term: %v. sendRequestAppendEntries. Times: %v! Get Higher term %v from server %v  HeartBeatID: %v. TaskID: %v", rf.me, rf.serverState, rf.currentTerm, reply.Term, serverId, counterRPC, heartbeatID, taskId)
			cond.Broadcast()
			rf.mu.Unlock()
			return ok
		}

		if rf.serverState != ServerStateLeader && rf.currentTerm != args.Term {
			DPrintf("Server %v. State: %v. Term: %v. Send RequestAppendEntries() to Server %v. Times: %v! no leader Quit HeartBeatID: %v. TaskID: %v", rf.me, rf.serverState, rf.currentTerm, serverId, counterRPC, heartbeatID, taskId)
			cond.Broadcast()
			rf.mu.Unlock()
			return ok
		}

		// If AppendEntries fails because of log inconsistency: decrement nextIndex and retry
		// optimize for Part2C
		if !reply.Success {
			// If it does not find an entry with that term, it should set nextIndex = conflictIndex.
			// If it finds an entry in its log with that term, it should set nextIndex to be the one beyond the index of the last entry in that term in its log.
			lastLogIndex := rf.lastLogIndex
			rf.leaderStateRecord.nextIndex[serverId] = reply.ConflictingFisrtIndex
			if reply.ConflictingTerm != -1 {
				for i := lastLogIndex; i > 0; i-- {
					entry, err := rf.GetLogEntry(i)
					if err != nil || entry.Term < reply.ConflictingTerm {
						break
					}
					if entry.Term == reply.ConflictingTerm {
						rf.leaderStateRecord.nextIndex[serverId] = i + 1
						break
					}
				}
			}
			DPrintf("Server %v. State: %v. Term: %v. Send RequestAppendEntries() to Server %v. Times: %v! Index not matched. rf.leaderStateRecord.nextIndex[serverId]: %v. continue HeartBeatID: %v. TaskID: %v", rf.me, rf.serverState, rf.currentTerm, serverId, counterRPC, rf.leaderStateRecord.nextIndex[serverId], heartbeatID, taskId)
			counterRPC++
			rf.mu.Unlock()
			continue
			// rf.leaderStateRecord.nextIndex[serverId]--
		}

		rf.leaderStateRecord.matchIndex[serverId] = args.PrevLogIndex + len(args.Entries)
		rf.leaderStateRecord.nextIndex[serverId] = rf.leaderStateRecord.matchIndex[serverId] + 1
		*countAgree = *countAgree + 1
		*finish = *finish + 1 // finish one RPC and ignore RPC state
		DPrintf("Server %v. State: %v. Term: %v. Send RequestAppendEntries() to Server %v. Times: %v! Success. matchIndex: %v. MatchArray: %v HeartBeatID: %v. TaskID: %v", rf.me, rf.serverState, rf.currentTerm, serverId, counterRPC, rf.leaderStateRecord.matchIndex[serverId], rf.leaderStateRecord.matchIndex, heartbeatID, taskId)

		cond.Broadcast()
		rf.mu.Unlock()
		return ok
	}
	return false
}

//
// 1
// 2
//
func (rf *Raft) StartElection() {
	cond := sync.NewCond(&rf.mu) // create Cond with Locker rf.mu

	rf.mu.Lock()

	countVote := 1      // Number of votes obtained
	finish := 1         // Number of completed RPC
	rf.votedFor = rf.me // vote for rf.me
	currentTerm := rf.currentTerm
	rf.persist()
	DPrintf("Server %v. State: %v. Term: %v. StartElection()!", rf.me, rf.serverState, rf.currentTerm)

	// for each server start routine sendRequestVote()
	for i := 0; i < len(rf.peers); i++ {
		if rf.me != i {
			DPrintf("Server %v. State:f1. Term: %v. StartElection(). countVote  is %v. finish is %v. go rf.sendRequestVote serverID: %v", rf.me, currentTerm, countVote, finish, i)
			go rf.sendRequestVote(i, cond, &countVote, &finish)
		}
	}

	rf.mu.Unlock()

	rf.mu.Lock()

	for countVote <= len(rf.peers)/2 && finish != len(rf.peers) && rf.serverState == ServerStateCandidate && currentTerm == rf.currentTerm {
		DPrintf("Server %v. State: %v. Term: %v. countVote is %v. finish is %v.", rf.me, rf.serverState, rf.currentTerm, countVote, finish)
		cond.Wait() // unlock rf.mu. block this thread until get cond signal
	}

	// confirm state
	if rf.serverState != ServerStateCandidate || currentTerm != rf.currentTerm {
		// still in candidate
		DPrintf("Server %v. State: %v. Term: %v. countVote is %v. finish is %v. End the election! recordTerm: %v. rf.currentTerm: %v", rf.me, rf.serverState, rf.currentTerm, countVote, finish, currentTerm, rf.currentTerm)
		rf.mu.Unlock()
		return
	}

	if countVote <= len(rf.peers)/2 {
		// lost the election.
		DPrintf("Server %v. State: %v. Term: %v. countVote is %v. finish is %v. Lose in election!", rf.me, rf.serverState, rf.currentTerm, countVote, finish)
		rf.mu.Unlock()
		return
	}

	// win the election. become leader and send heartbeat to other server at once
	DPrintf("Server %v. State: %v. Term: %v. countVote is %v. finish is %v. Become Leader!", rf.me, rf.serverState, rf.currentTerm, countVote, finish)
	rf.serverState = ServerStateLeader
	rf.leaderStateRecord.matchIndex = make([]int, len(rf.peers))
	rf.leaderStateRecord.matchIndex[rf.me] = rf.lastLogIndex // own log is always matched
	rf.leaderStateRecord.nextIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.leaderStateRecord.nextIndex); i++ {
		rf.leaderStateRecord.nextIndex[i] = rf.lastLogIndex + 1
	}

	rf.mu.Unlock()
}

//
// sned AppendEntries RPCs that carry no log entries
// to prevent election timeouts
//
func (rf *Raft) SpreadHeartbeat() {
	cond := sync.NewCond(&rf.mu) // create Cond with Locker rf.mu
	rf.mu.Lock()

	currentTerm := rf.currentTerm
	countAgree := 1 // Number of agreement obtained
	finish := 1
	rf.heartbeatTimes++
	taskId := rf.heartbeatTimes

	DPrintf("Server %v. State: %v. Term: %v. SpreadHeartbeat", rf.me, rf.serverState, rf.currentTerm)
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			DPrintf("Server %v. State:f0. Term: %v. SpreadHeartbeat. countAgree  is %v. finish is %v. go rf.sendRequestAppendEntries serverID: %v. HeartBeatID: %v", rf.me, currentTerm, countAgree, finish, i, taskId)
			go rf.sendRequestAppendEntries(i, cond, &countAgree, &finish, taskId)
		}
	}

	rf.mu.Unlock()

	rf.mu.Lock()

	DPrintf("Server %v. State:f0. Term: %v. SpreadHeartbeat. countAgree  is %v. finish is %v. Start Check! HeartBeatID: %v", rf.me, currentTerm, countAgree, finish, taskId)
	for countAgree <= len(rf.peers)/2 && finish != len(rf.peers) && rf.serverState == ServerStateLeader && rf.currentTerm == currentTerm {
		DPrintf("Server %v. State: %v. Term: %v. SpreadHeartbeat. countAgree  is %v. finish is %v. Wati HeartBeatID: %v", rf.me, rf.serverState, rf.currentTerm, countAgree, finish, taskId)
		cond.Wait() // unlock mu. block this thread until get cond signal
	}

	// confirm state
	if rf.serverState != ServerStateLeader || rf.currentTerm != currentTerm {
		DPrintf("Server %v. State: %v. Term: %v. SpreadHeartbeat. countAgree  is %v. finish is %v. recordTerm: %v. Log replication fail! HeartBeatID: %v", rf.me, rf.serverState, rf.currentTerm, countAgree, finish, currentTerm, taskId)
		rf.mu.Unlock()
		return
	}

	if countAgree <= len(rf.peers)/2 {
		DPrintf("Server %v. State: %v. Term: %v. SpreadHeartbeat. countAgree  is %v. finish is %v. countAgree <= len(rf.peers)/2 Log replication fail! HeartBeatID: %v", rf.me, rf.serverState, rf.currentTerm, countAgree, finish, taskId)
		rf.mu.Unlock()
		return
	}

	// majority grant
	DPrintf("Server %v. State: %v. Term: %v. SpreadHeartbeat. countAgree  is %v. finish is %v. Log replication Succeed! HeartBeatID: %v", rf.me, rf.serverState, rf.currentTerm, countAgree, finish, taskId)
	rf.leaderStateRecord.matchIndex[rf.me] = rf.lastLogIndex // own log is always matched
	// Get N
	// If there exists an N such that N > commitIndex, a majority
	// of matchIndex[i] ≥ N, and log[N].term == currentTerm:
	// set commitIndex = N
	minCommitIndex := rf.commandIndex
	// don't use commits := rf.leaderStateRecord.matchIndex TT
	commits := make([]int, len(rf.leaderStateRecord.matchIndex))
	copy(commits, rf.leaderStateRecord.matchIndex)
	DPrintf("Server %v. State: %v. Term: %v. SpreadHeartbeat. countAgree  is %v. finish is %v. Log replication Succeed! commits: %v HeartBeatID: %v", rf.me, rf.serverState, rf.currentTerm, countAgree, finish, commits, taskId)
	sort.Ints(commits)
	DPrintf("Server %v. State: %v. Term: %v. SpreadHeartbeat. countAgree  is %v. finish is %v. Log replication Succeed! Sortcommits: %v HeartBeatID: %v", rf.me, rf.serverState, rf.currentTerm, countAgree, finish, commits, taskId)
	N := commits[(len(rf.peers)-1)/2] // initialize to MaxN
	for ; N > minCommitIndex; N-- {
		entry, _ := rf.GetLogEntry(N)
		if entry.Term == rf.currentTerm {
			break
		}
	}
	rf.CommitLogEntries(N)
	DPrintf("Server %v. State: %v. Term: %v. SpreadHeartbeat. Log replication Succeed End! HeartBeatID: %v", rf.me, rf.serverState, rf.currentTerm, taskId)
	rf.persist()
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	isLeader = rf.serverState == ServerStateLeader
	if !isLeader {
		DPrintf("Server %v. State: %v. Term: %v. Recieve a command from client. Refuse it", rf.me, rf.serverState, rf.currentTerm)
		return index, term, isLeader
	}

	DPrintf("Server %v. State: %v. Term: %v. rf.lastLogIndex: %v. rf.log len: %v. Recieve a command from client. %v", rf.me, rf.serverState, rf.currentTerm, rf.lastLogIndex, len(rf.log), command)
	// Update leader's log info
	rf.AppendLogEntryToRaftLog(rf.lastLogIndex+1, make([]LogEntry, 1))
	rf.SetRaftLogEntry(rf.lastLogIndex, rf.currentTerm, command, rf.lastLogIndex)
	index = rf.lastLogIndex
	term = rf.currentTerm
	rf.persist()
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

// The ElectionTimer go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ElectionTimer() {
	var recordTime time.Time // record this thread awake time
	var electionTimeout time.Duration
	for !rf.killed() {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		rf.mu.Lock()

		DPrintf("Server %v. State: %v. Term: %v. ElectionTimer() Awake  rf.lastActiveTime: %v, recordTime: %v, electionTimeout: %v", rf.me, rf.serverState, rf.currentTerm, rf.lastActiveTime, recordTime, electionTimeout)
		if rf.serverState != ServerStateLeader && !recordTime.Before(rf.lastActiveTime) {
			// didn't update lastActiveTime in electionTimeout. to start new election
			rf.serverState = ServerStateCandidate // transfer to be candidate
			rf.currentTerm++                      // increase term
			go rf.StartElection()                 // StartElection
		}

		rand.Seed(time.Now().Unix() + int64(rf.me*20000))
		randNum := (ElectionTimeoutMin + rand.Intn(ElectionTimeoutMax-ElectionTimeoutMin))
		electionTimeout = time.Millisecond * time.Duration(randNum) // rerandom election timeout
		rf.lastActiveTime = time.Now()                              // update lastActiveTime
		recordTime = rf.lastActiveTime

		rf.mu.Unlock()
		time.Sleep(electionTimeout)
	}
	DPrintf("Server %v. State: %v. Term: %v. ElectionTimer() Quit. rf.killed", rf.me, rf.serverState, rf.currentTerm)
}

//
// A timer regularly checks whether it is a leader.
// If it is leader start rountine  HeartBeatTest() to send
// headbeat to other servers.
//
func (rf *Raft) HeartBeatTimer() {
	for !rf.killed() {
		time.Sleep(UpdateHeartbeatInterval * time.Millisecond)
		rf.mu.Lock()

		DPrintf("Server %v. State: %v. Term: %v. HeartBeatTimer() Awake", rf.me, rf.serverState, rf.currentTerm)
		if rf.serverState == ServerStateLeader {
			// leader start HeartBeatTest()
			// DPrintf("Leader %v. State: %v. Term: %v. RHeartBeatTest() Start", rf.me, rf.serverState, rf.currentTerm)
			DPrintf("Server %v. State: %v. Term: %v. SpreadHeartbeat() Start", rf.me, rf.serverState, rf.currentTerm)
			go rf.SpreadHeartbeat()
		}

		rf.mu.Unlock()
	}
	DPrintf("Server %v. State: %v. Term: %v. HeartBeatTimer() Quit. rf.killed", rf.me, rf.serverState, rf.currentTerm)
}

//
// A timer regularly saves state
//
func (rf *Raft) CreateSnapShotTimer() {
	for !rf.killed() {
		time.Sleep(UpdateSnapShotInterval * time.Millisecond)
		rf.mu.Lock()
		DPrintf("Server %v. State: %v. Term: %v. CreateSnapShotTimer() Awake. rf.lastApplied: %v. rf.lastLogIndex: %v.", rf.me, rf.serverState, rf.currentTerm, rf.lastApplied, rf.lastLogIndex)
		if len(rf.log) <= MaxLogLen || (rf.lastLogIndex-rf.lastApplied+1) > MaxLogLen {
			rf.mu.Unlock()
			continue
		}

		// rf.lastLogIndex - maxLogLen <= rf.lastApplied - 1 && len(rf.log) > maxLogLen
		// delete [1, includeIndex]. new log: [0, includeIndex+1, ...]
		DPrintf("Server %v. State: %v. Term: %v. CreateSnapShotTimer(). Clean Log. rf.lastApplied: %v. rf.lastLogIndex: %v.", rf.me, rf.serverState, rf.currentTerm, rf.lastApplied, rf.lastLogIndex)
		includeIndex := rf.lastApplied - 1
		temp := rf.lastLogIndex
		entries, _ := rf.GetLogEntrise(includeIndex+1, rf.lastLogIndex+1)
		rf.AppendLogEntryToRaftLog(rf.GetLogFirstEntryIndex(), entries)
		rf.lastLogIndex = temp
		DPrintf("Server %v. State: %v. Term: %v. CreateSnapShotTimer(). Clean Log Succees! rf.lastApplied: %v. rf.lastLogIndex: %v. log: %v", rf.me, rf.serverState, rf.currentTerm, rf.lastApplied, rf.lastLogIndex, rf.log)
		rf.mu.Unlock()
	}
	DPrintf("Server %v. State: %v. Term: %v. CreateSnapShotTimer() Quit. rf.killed", rf.me, rf.serverState, rf.currentTerm)
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
	rf.lastLogIndex = 0
	rf.log = make([]LogEntry, 1)
	rf.lastActiveTime = time.Now()
	rf.serverState = ServerStateFollower
	rf.applyCh = applyCh
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.heartbeatTimes = 0
	DPrintf("Raft  %v Make() initialization finished!\n", rf.me)
	// start ElectionTimer goroutine to start elections
	go rf.ElectionTimer()  // routine check whether elcetion time elapse.
	go rf.HeartBeatTimer() // routine send heartbeat regularly if sever is leader
	// go rf.CreateSnapShotTimer()
	return rf
}

//
// Call it when server need to be follower.
// Make sure you hold the sycn.lock when you call it
//
func (rf *Raft) BeFollower(currentTerm int, voteFor int) {
	rf.serverState = ServerStateFollower // transfer to Follower
	rf.currentTerm = currentTerm         // Update currentTerm
	rf.lastActiveTime = time.Now()       // Update election timeout
	// rf.votedFor = -1                     // Reset // ban it. It will cause brain-splited
	rf.votedFor = voteFor
	DPrintf("Server %v. State: %v. Term: %v. BeFollower(). rf.lastLogIndex: %v", rf.me, rf.serverState, rf.currentTerm, len(rf.log)-1)
}

//
// Commit log and apply
// Make sure you hold the sycn.lock when you call it
//
func (rf *Raft) CommitLogEntries(commitIndex int) {
	if commitIndex <= rf.lastApplied {
		return
	}

	applyStartIndex := rf.lastApplied + 1
	//entries := append([]LogEntry{}, rf.log[(applyStartIndex):(rf.commandIndex+1)]...) // get slice that need to be committed. replace : entries := rf.log[(applyStartIndex):(rf.commandIndex+1)] to prevent race.
	entries, err := rf.GetLogEntrise(applyStartIndex, commitIndex+1)
	if err != nil {
		DPrintf("Server %v. State: %v. Term: %v. CommitLogEntries(). CommitStartIndex %v CommitEndIndex %v. Error: %v", rf.me, rf.serverState, rf.currentTerm, rf.lastApplied+1, commitIndex, err)
		return
	}

	rf.commandIndex = commitIndex
	DPrintf("Server %v. State: %v. Term: %v. CommitLogEntries(). CommitStartIndex %v CommitEndIndex %v", rf.me, rf.serverState, rf.currentTerm, rf.lastApplied+1, rf.commandIndex)
	// routine casue channel
	go func(applyStartIndex int, entries []LogEntry) {
		for index, item := range entries {
			msg := ApplyMsg{
				CommandValid: true,
				Command:      item.Command,
				CommandIndex: index + applyStartIndex,
			}

			rf.applyCh <- msg

			rf.mu.Lock()

			DPrintf("Server %v. State: %v. Term: %v. CommitLogEntries(). Commit %v", rf.me, rf.serverState, rf.currentTerm, msg)
			if rf.lastApplied < msg.CommandIndex {
				rf.lastApplied = msg.CommandIndex
			}

			rf.mu.Unlock()
		}
		// DPrintf("Server %v. State: %v. Term: %v. CommitLogEntries() end. log: %v", rf.me, rf.serverState, rf.currentTerm, rf.log)
		// DPrintf("Server %v. State: %v. Term: %v. CommitLogEntries() end. loglen: %v", rf.me, rf.serverState, rf.currentTerm, len(rf.log))
	}(applyStartIndex, entries)
}

/* func (rf *Raft) CommitLogEntries(commitIndex int) {
	if commitIndex <= rf.lastApplied {
		return
	}

	rf.commandIndex = commitIndex
	entries := append([]LogEntry{}, rf.log[(rf.lastApplied+1):(rf.commandIndex+1)]...) // get slice that need to be committed
	for index, item := range entries {
		msg := ApplyMsg{
			CommandValid: true,
			Command:      item.Command,
			CommandIndex: index + rf.lastApplied + 1,
		}
		DPrintf("Server %v. State: %v. Term: %v. CommitLogEntries(). Commit %v", rf.me, rf.serverState, rf.currentTerm, msg)
		// DPrintf("Server %v. State: %v. Term: %v. CommitLogEntries(). Commit %v", rf.me, rf.serverState, rf.currentTerm, commitIndex)
		rf.applyCh <- msg
	}
	rf.lastApplied = rf.commandIndex
} */

//
// Like go slice. return slice[startIndex:endIndex]
// Make sure you hold the sycn.lock when you call it
//
func (rf *Raft) GetLogEntrise(startIndex int, endIndex int) ([]LogEntry, error) {
	DPrintf("Server %v. State: %v. Term: %v. GetLogEntrise(). startIndex: %v. endIndex: %v. rf.log len: %v.", rf.me, rf.serverState, rf.currentTerm, startIndex, endIndex, len(rf.log))
	start := rf.MappingLogicIndexToArrayIndex(startIndex)
	end := rf.MappingLogicIndexToArrayIndex(endIndex)
	DPrintf("Server %v. State: %v. Term: %v. GetLogEntrise(). startIndex: %v. endIndex: %v. rf.log len: %v. start: %v. end: %v.", rf.me, rf.serverState, rf.currentTerm, startIndex, endIndex, len(rf.log), start, end)

	if start < 0 || end < 0 || start > end {
		DPrintf("Server %v. State: %v. Term: %v. GetLogEntrise(). startIndex: %v. endIndex: %v. rf.log len: %v. start: %v. end: %v. Index Error.", rf.me, rf.serverState, rf.currentTerm, startIndex, endIndex, len(rf.log), start, end)
		return []LogEntry{}, errors.New("index error")
	}

	entries := make([]LogEntry, end-start)
	copy(entries, rf.log[start:end])
	DPrintf("Server %v. State: %v. Term: %v. GetLogEntrise(). Success! startIndex: %v. endIndex: %v. rf.log len: %v. start: %v. end: %v. Get entries: %v.", rf.me, rf.serverState, rf.currentTerm, startIndex, endIndex, len(rf.log), start, end, entries)
	return entries, nil
}

//
// Make sure you hold the sycn.lock when you call it
//
func (rf *Raft) SetRaftLogEntry(index int, term int, command interface{}, commandIndex int) error {
	DPrintf("Server %v. State: %v. Term: %v. SetRaftLogEntry(). Index: %v. rf.log len: %v.", rf.me, rf.serverState, rf.currentTerm, index, len(rf.log))
	i := rf.MappingLogicIndexToArrayIndex(index)

	if i < 0 || i == len(rf.log) {
		DPrintf("Server %v. State: %v. Term: %v. SetRaftLogEntry(). Index: %v. rf.log len: %v. Index Error: %v.", rf.me, rf.serverState, rf.currentTerm, index, len(rf.log), i)
		return errors.New("index error")
	}

	rf.log[i].Term = term
	rf.log[i].Command = command
	rf.log[i].CommandIndex = commandIndex
	DPrintf("Server %v. State: %v. Term: %v. SetRaftLogEntry(). Success! Index: %v. rf.log len: %v. i: %v. rf.log[i]: %v.", rf.me, rf.serverState, rf.currentTerm, index, len(rf.log), i, rf.log[i])
	return nil
}

//
// return rf.log[index], nil if it can find. else return {} error
// Make sure you hold the sycn.lock when you call it
//
func (rf *Raft) GetLogEntry(index int) (LogEntry, error) {
	DPrintf("Server %v. State: %v. Term: %v. GetLogEntry(). Index: %v. rf.log len: %v.", rf.me, rf.serverState, rf.currentTerm, index, len(rf.log))
	i := rf.MappingLogicIndexToArrayIndex(index)

	if i < 0 || i == len(rf.log) {
		DPrintf("Server %v. State: %v. Term: %v. GetLogEntry(). Index: %v. rf.log len: %v. Index Error: %v.", rf.me, rf.serverState, rf.currentTerm, index, len(rf.log), i)
		return LogEntry{}, errors.New("index error")
	}

	entry := rf.log[i]
	DPrintf("Server %v. State: %v. Term: %v. GetLogEntry(). Success! Index: %v. rf.log len: %v. i: %v. entry: %v.", rf.me, rf.serverState, rf.currentTerm, index, len(rf.log), i, entry)
	return entry, nil
}

//
// like append(rf.log[:appendIndex], Entries...)
// Make sure you hold the sycn.lock when you call it
//
func (rf *Raft) AppendLogEntryToRaftLog(appendIndex int, entires []LogEntry) error {
	DPrintf("Server %v. State: %v. Term: %v. AppendLogEntryToRaftLog(). appendIndex: %v. log lne: %v. entries len: %v.", rf.me, rf.serverState, rf.currentTerm, appendIndex, len(rf.log), len(entires))
	i := rf.MappingLogicIndexToArrayIndex(appendIndex)

	if i <= 0 {
		// can't cover index = 0
		DPrintf("Server %v. State: %v. Term: %v. AppendLogEntryToRaftLog(). Index: %v. rf.log len: %v. Index Error: %v.", rf.me, rf.serverState, rf.currentTerm, appendIndex, len(rf.log), i)
		return errors.New("index error")
	}

	rf.log = append(rf.log[:i], entires...) // cover i...
	// add: rf.lastLogIndex = appendIndex - 1 + len(entires)
	// delete:
	rf.lastLogIndex = appendIndex - 1 + len(entires)
	DPrintf("Server %v. State: %v. Term: %v. AppendLogEntryToRaftLog(). Success! appendIndex: %v. rf.log len: %v. rf.lastLogIndex: %v. i: %v.", rf.me, rf.serverState, rf.currentTerm, appendIndex, len(rf.log), rf.lastLogIndex, i)
	return nil
}

//
// Make sure you hold the sycn.lock when you call it.
// return [0, includeIndex+1, ...,len(rf.log)]. or -1
//
func (rf *Raft) MappingLogicIndexToArrayIndex(logicIndex int) int {
	DPrintf("Server %v. State: %v. Term: %v. MappingLogicIndexToArrayIndex(). logicIndex: %v. rf.log len: %v.", rf.me, rf.serverState, rf.currentTerm, logicIndex, len(rf.log))
	if logicIndex > rf.lastLogIndex+1 {
		DPrintf("Server %v. State: %v. Term: %v. MappingLogicIndexToArrayIndex(). logicIndex: %v. rf.lastLogIndex: %v. rf.log len: %v. logicIndex > rf.lastLogIndex + 1.", rf.me, rf.serverState, rf.currentTerm, logicIndex, rf.lastLogIndex, len(rf.log))
		return -1
	}

	// 0
	if logicIndex == 0 {
		DPrintf("Server %v. State: %v. Term: %v. MappingLogicIndexToArrayIndex(). logicIndex: %v. rf.lastLogIndex: %v. rf.log len: %v.", rf.me, rf.serverState, rf.currentTerm, logicIndex, rf.lastLogIndex, len(rf.log))
		return 0
	}

	offset := rf.lastLogIndex - logicIndex // offset in [-1, rf.lastLogIndex-1]
	index := len(rf.log) - 1 - offset      // index in [len(rf.log)-rf.lastLogIndex, ..., len(rf.log)]]
	if index <= 0 {
		DPrintf("Server %v. State: %v. Term: %v. MappingLogicIndexToArrayIndex(). logicIndex: %v. rf.lastLogIndex: %v. rf.log len: %v. index:  %v doesn't exist.", rf.me, rf.serverState, rf.currentTerm, logicIndex, rf.lastLogIndex, len(rf.log), index)
		return -1
	}
	DPrintf("Server %v. State: %v. Term: %v. MappingLogicIndexToArrayIndex(). Success! logicIndex: %v. rf.lastLogIndex: %v. rf.log len: %v. index: %v.", rf.me, rf.serverState, rf.currentTerm, logicIndex, rf.lastLogIndex, len(rf.log), index)
	return index
}

//
// Make sure you hold the sycn.lock when you call it.
//
func (rf *Raft) GetLogFirstEntryIndex() int {
	DPrintf("Server %v. State: %v. Term: %v. GetLogFirstEntryIndex(). rf.log len: %v. rf.lastLogIndex: %v", rf.me, rf.serverState, rf.currentTerm, len(rf.log), rf.lastLogIndex)
	if len(rf.log) == 1 {
		return 0
	}
	return rf.log[1].CommandIndex
}

func MaxInt(a int, b int) int {
	if a > b {
		return a
	}
	return b
}

func MinInt(a int, b int) int {
	if a > b {
		return b
	}
	return a
}
