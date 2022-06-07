package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = true
const ApplyTimeout = time.Millisecond * 300

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type Command struct {
	Key   string
	Value string
	Op    string
	// specify request
	ClinetId    int64
	SequenceNum int
}

// server will get this msg when command reachs a consensue in Raft.
type NotifyMsg struct {
	Value       string
	Err         Err
	ClinetId    int64
	SequenceNum int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	lastApplied    int                    // the index of raft log has applied in state machine
	lastRequestNum map[int64]int          // tracks the latest serial number processed for the client
	rpcGetCache    map[int64]NotifyMsg    // cache for duplicate Get request
	kvTable        map[string]string      // key-value table saves in memory
	notifyChannel  map[int]chan NotifyMsg // notify server to respond the request
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	// to check duplicate request
	// the requests that are from different clinet can be concurrent.
	// we can assume a client will make only one call into a Clerk at a time,
	request, ok := kv.lastRequestNum[args.ClientId]
	if !ok {
		request = 0
	}
	// a command whose serial number has already been executed
	if request >= args.SequenceNum {
		reply.Err = kv.rpcGetCache[args.ClientId].Err
		reply.Value = kv.rpcGetCache[args.ClientId].Value
		DPrintf("StateMachine: %v. Get(). request %v >= args.SequenceNum %v. arg: %v. reply %v.", kv.me, request, args.SequenceNum, args, reply)
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	command := Command{
		Key:         args.Key,
		Value:       "",
		Op:          args.Op,
		ClinetId:    args.ClientId,
		SequenceNum: args.SequenceNum,
	}
	index, _, isLeader := kv.rf.Start(command) // try to append
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	ch := kv.getNotifyChannel(index)
	kv.mu.Unlock()

	// wati the log is applied to state machine
	select {
	case res := <-ch:
		// check whether the log match this request
		if res.ClinetId != args.ClientId || res.SequenceNum != args.SequenceNum {
			// tell the clinet to retry
			reply.Err = "" // TODO
			DPrintf("StateMachine: %v. Get(). Command doesn't match. Send Start. arg: %v. res: %v. reply %v.", kv.me, args, res, reply)
			ch <- res // send msg to channel, because there is another thread matched this log is still waitting. It is safe for buffer is 1.
			DPrintf("StateMachine: %v. Get(). Command doesn't match. Send End. arg: %v. res: %v. reply %v.", kv.me, args, res, reply)
			return
		}
		reply.Err = res.Err
		reply.Value = res.Value
		DPrintf("StateMachine: %v. Get(). arg: %v. res: %v. reply %v.", kv.me, args, res, reply)
	case <-time.After(ApplyTimeout):
		reply.Err = ErrTimeout
		DPrintf("StateMachine: %v. Get(). ErrTimeout. arg: %v.", kv.me, args)
	}

	// Fixed: it is possible to happend 向已关闭通道发送数据触发 panic。
	kv.mu.Lock()
	kv.removeNotifyChannel(index)
	kv.mu.Unlock()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	request, ok := kv.lastRequestNum[args.ClientId]
	if !ok {
		request = 0
	}

	// a command whose serial number has already been executed
	if request >= args.SequenceNum {
		reply.Err = OK
		DPrintf("StateMachine: %v. PutAppend(). request %v >= args.SequenceNum %v. arg: %v. reply %v.", kv.me, request, args.SequenceNum, args, reply)
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	command := Command{
		Key:         args.Key,
		Value:       args.Value,
		Op:          args.Op,
		ClinetId:    args.ClientId,
		SequenceNum: args.SequenceNum,
	}
	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	ch := kv.getNotifyChannel(index)
	kv.mu.Unlock()

	select {
	case res := <-ch:
		if res.ClinetId != args.ClientId || res.SequenceNum != args.SequenceNum {
			reply.Err = "" // TODO
			DPrintf("StateMachine: %v. PutAppend(). Command doesn't match. Send Start. arg: %v. res: %v. reply %v.", kv.me, args, res, reply)
			ch <- res
			DPrintf("StateMachine: %v. PutAppend(). Command doesn't match. Send End. arg: %v. res: %v. reply %v.", kv.me, args, res, reply)
			return
		}
		reply.Err = res.Err
		DPrintf("StateMachine: %v. PutAppend(). res: %v. arg: %v.", kv.me, res, args)
	case <-time.After(ApplyTimeout):
		DPrintf("StateMachine: %v. PutAppend(). ErrTimeout. arg: %v.", kv.me, args)
		reply.Err = ErrTimeout
	}

	kv.mu.Lock()
	kv.removeNotifyChannel(index)
	kv.mu.Unlock()
}

// recieve msg from Raft and apply the log to state machine
func (kv *KVServer) apply() {
	for !kv.killed() {
		msg := <-kv.applyCh
		DPrintf("StateMachine: %v. apply(). get msg. msg.CommandValid: %v. msg.SnapshotValid: %v. index: %v. msg: %v.", kv.me, msg.CommandValid, msg.SnapshotValid, msg.CommandIndex, msg)
		if msg.CommandValid {
			kv.mu.Lock()
			// to check duplicate log
			if kv.lastApplied >= msg.CommandIndex {
				DPrintf("StateMachine: %v. apply(). Command. Command has execute. msg: %v.", kv.me, msg)
				kv.mu.Unlock()
				continue
			}
			notify := kv.dealWithCommandMsg(msg)
			kv.lastApplied = msg.CommandIndex // update lastApplied
			go kv.createSnapshot()
			_, isLeader := kv.rf.GetState() // check state
			if !isLeader {
				kv.mu.Unlock()
				continue
			}
			// state is leader. to nofity the sever to reply to request
			DPrintf("StateMachine: %v. apply(). Command. msg.CommandIndex: %v. Notify: %v.", kv.me, msg.CommandIndex, notify)
			ch := kv.getNotifyChannel(msg.CommandIndex)
			DPrintf("StateMachine: %v. apply(). Command. Send nofityChan Start. msg.CommandIndex: %v. Notify: %v.", kv.me, msg.CommandIndex, notify)
			ch <- notify // hold on the lock
			DPrintf("StateMachine: %v. apply(). Command. Send nofityChan End. msg.CommandIndex: %v. Notify: %v.", kv.me, msg.CommandIndex, notify)
			kv.mu.Unlock()
		} else if msg.SnapshotValid {
			kv.mu.Lock()
			term, _ := kv.rf.GetState()
			DPrintf("StateMachine: %v. apply(). Snapshot. Term: %v. msg.SnapshotTerm: %v.", kv.me, term, msg.SnapshotTerm)
			approve := kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot)
			if !approve {
				kv.mu.Unlock()
				continue
			}

			kv.readSnapShot(msg.Snapshot)
			kv.lastApplied = msg.SnapshotIndex
			DPrintf("StateMachine: %v. apply(). Snapshot. End. kv.lastApplied: %v. msg.SnapshotTerm: %v.", kv.me, kv.lastApplied, msg.SnapshotTerm)
			kv.mu.Unlock()
		}
	}
}

// deal with log and return the msg
func (kv *KVServer) dealWithCommandMsg(msg raft.ApplyMsg) NotifyMsg {
	var value string
	switch msg.Command.(type) {
	case Command:
		command := msg.Command.(Command) // panic: interface conversion: interface {} is nil, not kvraft.Command？
		notify := NotifyMsg{
			Value:       value, // "" is default
			Err:         OK,    //OK is default
			ClinetId:    command.ClinetId,
			SequenceNum: command.SequenceNum,
		}
		DPrintf("StateMachine: %v. dealWithCommandMsg(). command: %v.", kv.me, command)
		// to check duplicate
		if kv.lastRequestNum[command.ClinetId] >= command.SequenceNum {
			if command.Op != OpGet {
				return notify
			}
			// read cache
			DPrintf("StateMachine: %v. dealWithCommandMsg(). OpGet. msg: %v. check duplicate. kv.rpcGetCache[command.ClinetId]: %v.", kv.me, msg, kv.rpcGetCache[command.ClinetId])
			return kv.rpcGetCache[command.ClinetId]
		}
		switch {
		case command.Op == OpPut:
			kv.kvTable[command.Key] = command.Value
			DPrintf("StateMachine: %v. dealWithCommandMsg(). OpPut. msg: %v. kv.kvTable[command.Key]: %v.", kv.me, msg, kv.kvTable[command.Key])
		case command.Op == OpAppend:
			kv.kvTable[command.Key] += command.Value
			DPrintf("StateMachine: %v. dealWithCommandMsg(). OpAppend. msg: %v. kv.kvTable[command.Key]: %v.", kv.me, msg, kv.kvTable[command.Key])
		case command.Op == OpGet:
			_, ok := kv.kvTable[command.Key]
			if !ok {
				notify.Err = ErrNoKey
				DPrintf("StateMachine: %v. dealWithCommandMsg(). OpGet. msg: %v. ErrNoKey", kv.me, msg)
			} else {
				notify.Value = kv.kvTable[command.Key]
			}
			kv.rpcGetCache[command.ClinetId] = notify // ceche get
		default:
			notify.Err = ErrType
			// DPrintf("StateMachine: %v. Apply(). Msg type error. msg: %v.", kv.me, msg)
		}
		kv.lastRequestNum[command.ClinetId] = command.SequenceNum // update lastRequestNum
		return notify
	}
	return NotifyMsg{
		Value:       value,
		Err:         OK,
		ClinetId:    -1,
		SequenceNum: -1,
	}
}

func (kv *KVServer) readSnapShot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var lastRequestNum map[int64]int
	var rpcGetCache map[int64]NotifyMsg
	var kvTable map[string]string
	if d.Decode(&lastRequestNum) == nil && d.Decode(&rpcGetCache) == nil && d.Decode(&kvTable) == nil {
		kv.lastRequestNum = lastRequestNum
		kv.rpcGetCache = rpcGetCache
		kv.kvTable = kvTable
	}
	DPrintf("StateMachine: %v. readSnapShot(). kv.lastRequestNum: %v. kv.rpcGetCache: %v. kv.kvTable: %v.", kv.me, kv.lastRequestNum, kv.rpcGetCache, kv.kvTable)
}

func (kv *KVServer) createSnapshot() {
	if kv.maxraftstate < 0 {
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	size := kv.rf.GetRaftStateSize()
	if size < kv.maxraftstate {
		return
	}
	DPrintf("StateMachine: %v. createSnapshot(). RaftStateSize: %v. maxraftstate: %v.", kv.me, size, kv.maxraftstate)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.lastRequestNum)
	e.Encode(kv.rpcGetCache)
	e.Encode(kv.kvTable)
	data := w.Bytes()
	DPrintf("StateMachine: %v. createSnapshot(). Start snapshot. kv.lastRequestNum: %v. kv.rpcGetCache: %v. kv.kvTable: %v.", kv.me, kv.lastRequestNum, kv.rpcGetCache, kv.kvTable)
	kv.rf.Snapshot(kv.lastApplied, data)
	DPrintf("StateMachine: %v. createSnapshot(). End snapshot. kv.lastRequestNum: %v. kv.rpcGetCache: %v. kv.kvTable: %v.", kv.me, kv.lastRequestNum, kv.rpcGetCache, kv.kvTable)
}

// return a channel for notify server that log has been applied to state machine.
// One log is bound to one channel,
// but one channel may be bound to many request from different clinet.
func (kv *KVServer) getNotifyChannel(commandIndex int) chan NotifyMsg {
	_, ok := kv.notifyChannel[commandIndex]
	if !ok {
		kv.notifyChannel[commandIndex] = make(chan NotifyMsg, 1) // set channel's buffer to be 1.
	}
	return kv.notifyChannel[commandIndex]
}

// close NotifyChannel
func (kv *KVServer) removeNotifyChannel(commandIndex int) {
	DPrintf("StateMachine: %v. removeNotifyChannel(). commandIndex: %v.", kv.me, commandIndex)
	_, ok := kv.notifyChannel[commandIndex]
	if !ok {
		DPrintf("StateMachine: %v. removeNotifyChannel(). commandIndex: %v. Not OK.", kv.me, commandIndex)
		return
	}
	close(kv.notifyChannel[commandIndex])
	delete(kv.notifyChannel, commandIndex)
	DPrintf("StateMachine: %v. removeNotifyChannel(). commandIndex: %v. Delete.", kv.me, commandIndex)
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(Command{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.lastRequestNum = make(map[int64]int)
	kv.rpcGetCache = make(map[int64]NotifyMsg)
	kv.kvTable = make(map[string]string)
	kv.notifyChannel = make(map[int]chan NotifyMsg)
	kv.readSnapShot(kv.rf.ReadSnapshot())
	go kv.apply()

	// You may need initialization code here.

	return kv
}
