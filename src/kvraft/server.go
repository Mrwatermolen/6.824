package kvraft

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false
const ApplyTimeout = time.Millisecond * 500

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

type NotifyMsg struct {
	value       string
	err         Err
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
	rpcGetCache    map[int64]NotifyMsg    //  cache for outdate Get request
	kvTable        map[string]string      // key-value table saves in memory
	notifyChannel  map[int]chan NotifyMsg // notify server to respond the request
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	request, ok := kv.lastRequestNum[args.ClientId]
	if !ok {
		request = 0
	}

	// a command whose serial number has already been executed
	if request >= args.SequenceNum {
		reply.Err = kv.rpcGetCache[args.ClientId].err
		reply.Value = kv.rpcGetCache[args.ClientId].value
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
	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	ch := kv.GetNotifyChannel(index)
	kv.mu.Unlock()

	select {
	case res := <-ch:
		if res.ClinetId != args.ClientId || res.SequenceNum != args.SequenceNum {
			reply.Err = "" // TODO
			ch <- res
			return
		}
		/* kv.mu.Lock()
		kv.lastRequestNum[args.ClientId] = args.SequenceNum
		kv.mu.Unlock() */
		reply.Err = res.err
		reply.Value = res.value
		DPrintf("StateMachine: %v. Get(). res: %v. arg: %v. reply %v", kv.me, res, args, reply)
	case <-time.After(ApplyTimeout):
		DPrintf("StateMachine: %v. Get(). ErrTimeout. arg: %v.", kv.me, args)
		reply.Err = ErrTimeout
	}

	kv.mu.Lock()
	kv.RemoveNotifyChannel(index)
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
	ch := kv.GetNotifyChannel(index)
	kv.mu.Unlock()

	select {
	case res := <-ch:
		if res.ClinetId != args.ClientId || res.SequenceNum != args.SequenceNum {
			reply.Err = "" // TODO
			ch <- res
			return
		}
		/* kv.mu.Lock()
		kv.lastRequestNum[args.ClientId] = args.SequenceNum
		kv.mu.Unlock() */
		reply.Err = res.err
		DPrintf("StateMachine: %v. PutAppend(). res: %v. arg: %v.", kv.me, res, args)
	case <-time.After(ApplyTimeout):
		DPrintf("StateMachine: %v. PutAppend(). ErrTimeout. arg: %v.", kv.me, args)
		reply.Err = ErrTimeout
	}

	kv.mu.Lock()
	kv.RemoveNotifyChannel(index)
	kv.mu.Unlock()
}

func (kv *KVServer) Apply() {
	for !kv.killed() {
		msg := <-kv.applyCh

		if msg.CommandValid {
			kv.mu.Lock()
			if kv.lastApplied >= msg.CommandIndex {
				kv.mu.Unlock()
				continue
			}
			notify := kv.DealWithMsg(msg)
			kv.lastApplied = msg.CommandIndex
			_, isLeader := kv.rf.GetState()
			if !isLeader {
				kv.mu.Unlock()
				continue
			}
			DPrintf("StateMachine: %v. Apply(). Notify. msg.CommandIndex: %v. Notify. %v.", kv.me, msg.CommandIndex, notify)
			ch := kv.GetNotifyChannel(msg.CommandIndex)
			kv.mu.Unlock()
			ch <- notify
		}
	}
}

func (kv *KVServer) DealWithMsg(msg raft.ApplyMsg) NotifyMsg {
	var err Err
	err = OK
	var value string
	command := msg.Command.(Command)
	notify := NotifyMsg{
		value:       value,
		err:         err,
		ClinetId:    command.ClinetId,
		SequenceNum: command.SequenceNum,
	}

	if kv.lastRequestNum[command.ClinetId] >= command.SequenceNum {
		if command.Op != OpGet {
			return notify
		}
		// read Cache
		return kv.rpcGetCache[command.ClinetId]
	}
	switch {
	case command.Op == OpPut:
		kv.kvTable[command.Key] = command.Value
		// DPrintf("StateMachine: %v. Apply(). OpPut. msg: %v.", kv.me, msg)
	case command.Op == OpAppend:
		kv.kvTable[command.Key] += command.Value
		// DPrintf("StateMachine: %v. Apply(). OpAppend. msg: %v.", kv.me, msg)
	case command.Op == OpGet:
		// DPrintf("StateMachine: %v. Apply(). OpGet. msg: %v.", kv.me, msg)
		_, ok := kv.kvTable[command.Key]
		if !ok {
			err = ErrNoKey
		} else {
			notify.value = kv.kvTable[command.Key]
		}
		kv.rpcGetCache[command.ClinetId] = notify // ceche get
	default:
		err = ErrType
		// DPrintf("StateMachine: %v. Apply(). Msg type error. msg: %v.", kv.me, msg)
	}
	kv.lastRequestNum[command.ClinetId] = command.SequenceNum
	return notify
}

func (kv *KVServer) GetNotifyChannel(commandIndex int) chan NotifyMsg {
	_, ok := kv.notifyChannel[commandIndex]
	if !ok {
		kv.notifyChannel[commandIndex] = make(chan NotifyMsg)
	}
	return kv.notifyChannel[commandIndex]
}

func (kv *KVServer) RemoveNotifyChannel(commandIndex int) {
	_, ok := kv.notifyChannel[commandIndex]
	if !ok {
		return
	}
	close(kv.notifyChannel[commandIndex])
	delete(kv.notifyChannel, commandIndex)
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
	go kv.Apply()

	// You may need initialization code here.

	return kv
}
