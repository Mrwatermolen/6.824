package shardkv

import (
	"bytes"
	"log"
	"sync"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
)

const Debug = true
const ApplyTimeout = time.Millisecond * 300
const QueryConfigTime = time.Millisecond * 90

type KVData map[string]string
type ShardOp string

const (
	ShardOpJoin  = "Join"
	ShardOpLeave = "Leave"
)

type RuningState string

const (
	ReConfiging = "ReConfiging"
	Working     = "Working"
)

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
	Key        string
	Value      string
	ChangesLog ConfigChangesLog
	ConfigNum  int
	Shard      int
	Op         string
	// specify request
	ClinetId    int64
	SequenceNum int
}

// server will get this msg when command reachs a consensue in Raft.
type NotifyMsg struct {
	Key         string
	Value       string
	ConfigNum   int
	Data        []byte
	Err         Err
	ClinetId    int64
	SequenceNum int
}

type ConfigChange struct {
	Shard      int
	Op         ShardOp
	FormerGid  int
	CurrentGid int
}

type ConfigChangesLog struct {
	Data      []byte
	NewConfig shardctrler.Config
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	lastApplied    int                    // the index of raft log has applied in state machine
	lastRequestNum map[int64]int          // tracks the latest serial number processed for the client
	rpcGetCache    map[int64]NotifyMsg    // cache for duplicate Get request
	kvTable        map[string]string      // key-value table saves in memory
	notifyChannel  map[int]chan NotifyMsg // notify server to respond the request
	clerk          *shardctrler.Clerk     // a clinet communicate with shardcontroller
	curConfig      shardctrler.Config
	runningState   RuningState
	configChannel  map[int]chan []byte
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
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
		DPrintf("Gourp: %v. StateMachine: %v. RunState: %v. Get(). request %v >= args.SequenceNum %v. arg: %v. reply %v.", kv.gid, kv.me, kv.runningState, request, args.SequenceNum, args, reply)
		kv.mu.Unlock()
		return
	}
	if kv.runningState != Working {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	if kv.curConfig.Shards[key2shard(args.Key)] != kv.gid || kv.curConfig.Num != args.ConfigNum {
		reply.Err = ErrWrongGroup
		DPrintf("Gourp: %v. StateMachine: %v. RunState: %v. Get(). ErrWrongGroup. kv.curConfig: %v. args.config: %v. Shards: %v to Gid: %v. arg: %v.", kv.gid, kv.me, kv.runningState, kv.curConfig.Num, args.ConfigNum, key2shard(args.Key), kv.curConfig.Shards[key2shard(args.Key)], args)
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	command := Command{
		Key:         args.Key,
		Value:       "",
		Op:          args.Op,
		ConfigNum:   args.ConfigNum,
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
			DPrintf("Gourp: %v. StateMachine: %v. RunState: %v. Get(). Command doesn't match. Send Start. arg: %v. res: %v. reply %v.", kv.gid, kv.me, kv.runningState, args, res, reply)
			ch <- res // send msg to channel, because there is another thread matched this log is still waitting. It is safe for buffer is 1.
			DPrintf("Gourp: %v. StateMachine: %v. RunState: %v. Get(). Command doesn't match. Send End. arg: %v. res: %v. reply %v.", kv.gid, kv.me, kv.runningState, args, res, reply)
			return
		}
		reply.Err = res.Err
		reply.Value = res.Value
		DPrintf("Gourp: %v. StateMachine: %v. RunState: %v. Get(). arg: %v. res: %v. reply %v.", kv.gid, kv.me, kv.runningState, args, res, reply)
	case <-time.After(ApplyTimeout):
		reply.Err = ErrTimeout
		DPrintf("Gourp: %v. StateMachine: %v. RunState: %v. Get(). ErrTimeout. arg: %v.", kv.gid, kv.me, kv.runningState, args)
	}

	kv.mu.Lock()
	kv.removeNotifyChannel(index)
	kv.mu.Unlock()
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	request, ok := kv.lastRequestNum[args.ClientId]
	if !ok {
		request = 0
	}

	// a command whose serial number has already been executed
	if request >= args.SequenceNum {
		reply.Err = OK
		DPrintf("Gourp: %v. StateMachine: %v. RunState: %v. PutAppend(). request %v >= args.SequenceNum %v. arg: %v. reply %v.", kv.gid, kv.me, kv.runningState, request, args.SequenceNum, args, reply)
		kv.mu.Unlock()
		return
	}
	if kv.runningState != Working {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	if kv.curConfig.Shards[key2shard(args.Key)] != kv.gid || kv.curConfig.Num != args.ConfigNum {
		reply.Err = ErrWrongGroup
		DPrintf("Gourp: %v. StateMachine: %v. RunState: %v. PutAppend(). ErrWrongGroup. kv.curConfig: %v. args.config: %v. Shards: %v to Gid: %v. arg: %v.", kv.gid, kv.me, kv.runningState, kv.curConfig.Num, args.ConfigNum, key2shard(args.Key), kv.curConfig.Shards[key2shard(args.Key)], args)
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	command := Command{
		Key:         args.Key,
		Value:       args.Value,
		Op:          args.Op,
		ConfigNum:   args.ConfigNum,
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
			DPrintf("Gourp: %v. StateMachine: %v. RunState: %v. PutAppend(). Command doesn't match. Send Start. arg: %v. res: %v. reply %v.", kv.gid, kv.me, kv.runningState, args, res, reply)
			ch <- res
			DPrintf("Gourp: %v. StateMachine: %v. RunState: %v. PutAppend(). Command doesn't match. Send End. arg: %v. res: %v. reply %v.", kv.gid, kv.me, kv.runningState, args, res, reply)
			return
		}
		reply.Err = res.Err
		DPrintf("Gourp: %v. StateMachine: %v. RunState: %v. PutAppend(). res: %v. arg: %v.", kv.gid, kv.me, kv.runningState, res, args)
	case <-time.After(ApplyTimeout):
		DPrintf("Gourp: %v. StateMachine: %v. RunState: %v. PutAppend(). ErrTimeout. arg: %v.", kv.gid, kv.me, kv.runningState, args)
		reply.Err = ErrTimeout
	}

	kv.mu.Lock()
	kv.removeNotifyChannel(index)
	kv.mu.Unlock()
}

func (kv *ShardKV) GetShardData(args *shardctrler.GetShardDataArgs, reply *shardctrler.GetShardDatareply) {
	kv.mu.Lock()
	request, ok := kv.lastRequestNum[args.ClientId]
	if !ok {
		request = 0
	}

	// a command whose serial number has already been executed
	if request >= args.SequenceNum {
		if kv.rpcGetCache[args.ClientId].Err == OK {
			reply.Err = "OK"
		} else if kv.rpcGetCache[args.ClientId].Err == ErrTimeout {
			reply.Err = "ErrTimeout"
		} else if kv.rpcGetCache[args.ClientId].Err == ErrWrongLeader {
			reply.Err = "ErrWrongLeader"
		}
		reply.ConfigNum = kv.rpcGetCache[args.ClientId].ConfigNum
		reply.Data = kv.rpcGetCache[args.ClientId].Data
		DPrintf("Gourp: %v. StateMachine: %v. RunState: %v. GetShardData(). request %v >= args.SequenceNum %v. arg: %v.", kv.gid, kv.me, kv.runningState, request, args.SequenceNum, args)
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	command := Command{
		ConfigNum:   args.ConfigNum,
		Shard:       args.Shard,
		Op:          OpGetShardData,
		ClinetId:    args.ClientId,
		SequenceNum: args.SequenceNum,
	}
	index, _, isLeader := kv.rf.Start(command) // try to append
	if !isLeader {
		// DPrintf("Gourp: %v. StateMachine: %v. GetShardData(). Not leader. arg: %v.", kv.gid, kv.me, args)
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	DPrintf("Gourp: %v. StateMachine: %v. GetShardData(). leader. arg: %v. index: %v", kv.gid, kv.me, args, index)
	ch := kv.getNotifyChannel(index)
	kv.mu.Unlock()

	// wati the log is applied to state machine
	select {
	case res := <-ch:
		// check whether the log match this request
		if res.ClinetId != args.ClientId || res.SequenceNum != args.SequenceNum {
			// tell the clinet to retry
			reply.Err = "" // TODO
			DPrintf("Gourp: %v. StateMachine: %v. RunState: %v. GetShardData(). Command doesn't match. Send Start. arg: %v. res: %v. reply %v.", kv.gid, kv.me, kv.runningState, args, res, reply)
			ch <- res // send msg to channel, because there is another thread matched this log is still waitting. It is safe for buffer is 1.
			DPrintf("Gourp: %v. StateMachine: %v. RunState: %v. GetShardData(). Command doesn't match. Send End. arg: %v. res: %v. reply %v.", kv.gid, kv.me, kv.runningState, args, res, reply)
			return
		}
		if kv.rpcGetCache[args.ClientId].Err == OK {
			reply.Err = "OK"
		} else if kv.rpcGetCache[args.ClientId].Err == ErrTimeout {
			reply.Err = "ErrTimeout"
		} else if kv.rpcGetCache[args.ClientId].Err == ErrWrongLeader {
			reply.Err = "ErrWrongLeader"
		}
		reply.ConfigNum = res.ConfigNum
		reply.Data = res.Data
		DPrintf("Gourp: %v. StateMachine: %v. RunState: %v. GetShardData(). arg: %v.", kv.gid, kv.me, kv.runningState, args)
	case <-time.After(ApplyTimeout):
		reply.Err = ErrTimeout
		DPrintf("Gourp: %v. StateMachine: %v. RunState: %v. GetShardData(). ErrTimeout. arg: %v.", kv.gid, kv.me, kv.runningState, args)
	}

	kv.mu.Lock()
	kv.removeNotifyChannel(index)
	kv.mu.Unlock()
}

// recieve msg from Raft and apply the log to state machine
func (kv *ShardKV) apply() {
	for {
		msg := <-kv.applyCh
		DPrintf("Gourp: %v. StateMachine: %v. RunState: %v. apply(). get msg. msg.CommandValid: %v. msg.SnapshotValid: %v. index: %v. msg: %v.", kv.gid, kv.me, kv.runningState, msg.CommandValid, msg.SnapshotValid, msg.CommandIndex, msg)
		if msg.CommandValid {
			kv.mu.Lock()
			// to check duplicate log
			if kv.lastApplied >= msg.CommandIndex {
				DPrintf("Gourp: %v. StateMachine: %v. RunState: %v. apply(). Command. Command has execute. msg: %v.", kv.gid, kv.me, kv.runningState, msg)
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
			DPrintf("Gourp: %v. StateMachine: %v. RunState: %v. apply(). Command. msg.CommandIndex: %v. Notify: %v.", kv.gid, kv.me, kv.runningState, msg.CommandIndex, notify)
			ch := kv.getNotifyChannel(msg.CommandIndex)
			DPrintf("Gourp: %v. StateMachine: %v. RunState: %v. apply(). Command. Send nofityChan Start. msg.CommandIndex: %v. Notify: %v.", kv.gid, kv.me, kv.runningState, msg.CommandIndex, notify)
			ch <- notify // hold on the lock
			DPrintf("Gourp: %v. StateMachine: %v. RunState: %v. apply(). Command. Send nofityChan End. msg.CommandIndex: %v. Notify: %v.", kv.gid, kv.me, kv.runningState, msg.CommandIndex, notify)
			kv.mu.Unlock()
		} else if msg.SnapshotValid {
			kv.mu.Lock()
			term, _ := kv.rf.GetState()
			DPrintf("Gourp: %v. StateMachine: %v. RunState: %v. apply(). Snapshot. Term: %v. msg.SnapshotTerm: %v.", kv.gid, kv.me, kv.runningState, term, msg.SnapshotTerm)
			approve := kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot)
			if !approve {
				kv.mu.Unlock()
				continue
			}

			kv.readSnapShot(msg.Snapshot)
			kv.lastApplied = msg.SnapshotIndex
			DPrintf("Gourp: %v. StateMachine: %v. RunState: %v. apply(). Snapshot. End. kv.lastApplied: %v. msg.SnapshotTerm: %v.", kv.gid, kv.me, kv.runningState, kv.lastApplied, msg.SnapshotTerm)
			kv.mu.Unlock()
		}
	}
}

// deal with log and return the msg
func (kv *ShardKV) dealWithCommandMsg(msg raft.ApplyMsg) NotifyMsg {
	var value string
	switch msg.Command.(type) {
	case Command:
		command := msg.Command.(Command) // panic: interface conversion: interface {} is nil, not kvraft.Command？
		notify := NotifyMsg{
			Value:       value, // "" is default
			Err:         OK,    //OK is default
			ConfigNum:   kv.curConfig.Num,
			ClinetId:    command.ClinetId,
			SequenceNum: command.SequenceNum,
		}
		DPrintf("Gourp: %v. StateMachine: %v. RunState: %v. dealWithCommandMsg(). command: %v.", kv.gid, kv.me, kv.runningState, command)
		// to check duplicate
		if kv.lastRequestNum[command.ClinetId] >= command.SequenceNum && command.Op != OpConfigChange {
			if command.Op != OpGet && command.Op != OpGetShardData {
				return notify
			}
			// read cache
			DPrintf("Gourp: %v. StateMachine: %v. RunState: %v. dealWithCommandMsg(). Op: %v. msg: %v. check duplicate. kv.rpcGetCache[command.ClinetId]: %v.", kv.gid, kv.me, kv.runningState, command.Op, msg, kv.rpcGetCache[command.ClinetId])
			return kv.rpcGetCache[command.ClinetId]
		}
		switch {
		case command.Op == OpPut:
			if kv.curConfig.Shards[key2shard(command.Key)] != kv.gid || command.ConfigNum != kv.curConfig.Num {
				DPrintf("Gourp: %v. StateMachine: %v. RunState: %v. dealWithCommandMsg(). OpPut. ErrWrongGroup. msg: %v. command.Key: %v.", kv.gid, kv.me, kv.runningState, msg, kv.kvTable[command.Key])
				notify.Err = ErrWrongGroup
				return notify
			}
			kv.kvTable[command.Key] = command.Value
			DPrintf("Gourp: %v. StateMachine: %v. RunState: %v. dealWithCommandMsg(). OpPut. msg: %v. kv.kvTable[command.Key]: %v.", kv.gid, kv.me, kv.runningState, msg, kv.kvTable[command.Key])
		case command.Op == OpAppend:
			if kv.curConfig.Shards[key2shard(command.Key)] != kv.gid || command.ConfigNum != kv.curConfig.Num {
				DPrintf("Gourp: %v. StateMachine: %v. RunState: %v. dealWithCommandMsg(). OpAppend. ErrWrongGroup. msg: %v. command.Key: %v.", kv.gid, kv.me, kv.runningState, msg, kv.kvTable[command.Key])
				notify.Err = ErrWrongGroup
				return notify
			}
			kv.kvTable[command.Key] += command.Value
			DPrintf("Gourp: %v. StateMachine: %v. RunState: %v. dealWithCommandMsg(). OpAppend. msg: %v. kv.kvTable[command.Key]: %v.", kv.gid, kv.me, kv.runningState, msg, kv.kvTable[command.Key])
		case command.Op == OpGet:
			if kv.curConfig.Shards[key2shard(command.Key)] != kv.gid {
				DPrintf("Gourp: %v. StateMachine: %v. RunState: %v. dealWithCommandMsg(). OpGet. ErrWrongGroup. msg: %v. command.Key: %v.", kv.gid, kv.me, kv.runningState, msg, kv.kvTable[command.Key])
				notify.Err = ErrWrongGroup
				return notify // don't save cache and request
			}
			_, ok := kv.kvTable[command.Key]
			if !ok {
				notify.Err = ErrNoKey
				DPrintf("Gourp: %v. StateMachine: %v. RunState: %v. dealWithCommandMsg(). OpGet. msg: %v. ErrNoKey", kv.gid, kv.me, kv.runningState, msg)
			} else {
				notify.Value = kv.kvTable[command.Key]
			}
			// kv.rpcGetCache[command.ClinetId] = notify // ceche get
		case command.Op == OpGetShardData:
			shardsMap := make(KVData, 0)
			requestReplic := make(map[int64]int, 0)
			cacheReplica := make(map[int64]NotifyMsg, 0)
			for key, value := range kv.kvTable {
				if command.Shard == key2shard(key) {
					DPrintf("Gourp: %v. StateMachine: %v. RunState: %v. dealWithCommandMsg(). OpGetShardData. shard: %v match key: %v.", kv.gid, kv.me, kv.runningState, command.Shard, key)
					shardsMap[key] = value
				}
			}
			for k, v := range kv.lastRequestNum {
				if command.Shard == key2shard(kv.rpcGetCache[k].Key) {
					requestReplic[k] = v
					cacheReplica[k] = kv.rpcGetCache[k]
				}
			}
			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)
			e.Encode(shardsMap)
			e.Encode(requestReplic)
			e.Encode(cacheReplica)
			data := w.Bytes()
			notify.Data = data
			// kv.rpcGetCache[command.ClinetId] = notify
			DPrintf("Gourp: %v. StateMachine: %v. RunState: %v. dealWithCommandMsg(). OpGetShardData. End. Config: %v. Shard: %v. notify.Err: %v.", kv.gid, kv.me, kv.runningState, command.ConfigNum, command.Shard, notify.Err)
		case command.Op == OpConfigChange:
			//kv.runningState = Working
			if kv.curConfig.Num >= command.ChangesLog.NewConfig.Num {
				DPrintf("Gourp: %v. StateMachine: %v. RunState: %v. dealWithCommandMsg(). OpConfigChange. NoData End. curNum: %v. newConfig: %v.", kv.gid, kv.me, kv.runningState, kv.curConfig.Num, kv.curConfig.Num)
				break
			}
			kv.curConfig = command.ChangesLog.NewConfig
			if len(command.ChangesLog.Data) < 1 {
				DPrintf("Gourp: %v. StateMachine: %v. RunState: %v. dealWithCommandMsg(). OpConfigChange. NoData End. newConfig: %v.", kv.gid, kv.me, kv.runningState, kv.curConfig.Num)
				break
			}
			r := bytes.NewBuffer(command.ChangesLog.Data)
			d := labgob.NewDecoder(r)
			var table KVData
			var requestNum map[int64]int
			var caches map[int64]NotifyMsg
			d.Decode(&table)
			d.Decode(&requestNum)
			d.Decode(&caches)
			for key, value := range table {
				kv.kvTable[key] = value
			}
			for key, value := range requestNum {
				v, ok := kv.lastRequestNum[key]
				DPrintf("Gourp: %v. StateMachine: %v. RunState: %v. dealWithCommandMsg(). OpConfigChange. Record. id: %v. kv: %v. new: %v.", kv.gid, kv.me, kv.runningState, key, v, value)
				if ok && value <= v {
					continue
				}
				kv.lastRequestNum[key] = value
				kv.rpcGetCache[key] = caches[key]
			}
			DPrintf("Gourp: %v. StateMachine: %v. RunState: %v. dealWithCommandMsg(). OpConfigChange. End. newConfig: %v.", kv.gid, kv.me, kv.runningState, kv.curConfig.Num)
			DPrintf("Gourp: %v. StateMachine: %v. RunState: %v. dealWithCommandMsg(). OpConfigChange. newTable: %v. map: %v.", kv.gid, kv.me, kv.runningState, table, kv.kvTable)
		default:
			notify.Err = ErrType
			// DPrintf("Gourp: %v. StateMachine: %v. RunState: %v. Apply(). Msg type error. msg: %v.", kv.gid, kv.me, kv.runningState, msg)
		}
		kv.lastRequestNum[command.ClinetId] = command.SequenceNum // update lastRequestNum
		notify.Key = command.Key
		kv.rpcGetCache[command.ClinetId] = notify // ceche get
		return notify
	}
	return NotifyMsg{
		Value:       value,
		Err:         OK,
		ClinetId:    -1,
		SequenceNum: -1,
	}
}

func (kv *ShardKV) readSnapShot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var lastRequestNum map[int64]int
	var rpcGetCache map[int64]NotifyMsg
	var kvTable KVData
	var config shardctrler.Config
	if d.Decode(&lastRequestNum) == nil && d.Decode(&rpcGetCache) == nil && d.Decode(&kvTable) == nil && d.Decode(&config) == nil {
		kv.lastRequestNum = lastRequestNum
		kv.rpcGetCache = rpcGetCache
		kv.kvTable = kvTable
		kv.curConfig = config
	}
	DPrintf("Gourp: %v. StateMachine: %v. RunState: %v. readSnapShot(). kv.lastRequestNum: %v. kv.rpcGetCache: %v. kv.kvTable: %v.", kv.gid, kv.me, kv.runningState, kv.lastRequestNum, kv.rpcGetCache, kv.kvTable)
}

func (kv *ShardKV) createSnapshot() {
	if kv.maxraftstate < 0 {
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	size := kv.rf.GetRaftStateSize()
	if size < kv.maxraftstate {
		return
	}
	DPrintf("Gourp: %v. StateMachine: %v. RunState: %v. createSnapshot(). RaftStateSize: %v. maxraftstate: %v.", kv.gid, kv.me, kv.runningState, size, kv.maxraftstate)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.lastRequestNum)
	e.Encode(kv.rpcGetCache)
	e.Encode(kv.kvTable)
	e.Encode(kv.curConfig)
	data := w.Bytes()
	DPrintf("Gourp: %v. StateMachine: %v. RunState: %v. createSnapshot(). Start snapshot. kv.lastRequestNum: %v. kv.rpcGetCache: %v. kv.kvTable: %v.", kv.gid, kv.me, kv.runningState, kv.lastRequestNum, kv.rpcGetCache, kv.kvTable)
	kv.rf.Snapshot(kv.lastApplied, data)
	DPrintf("Gourp: %v. StateMachine: %v. RunState: %v. createSnapshot(). End snapshot. kv.lastRequestNum: %v. kv.rpcGetCache: %v. kv.kvTable: %v.", kv.gid, kv.me, kv.runningState, kv.lastRequestNum, kv.rpcGetCache, kv.kvTable)
}

// return a channel for notify server that log has been applied to state machine.
// One log is bound to one channel,
// but one channel may be bound to many request from different clinet.
func (kv *ShardKV) getNotifyChannel(commandIndex int) chan NotifyMsg {
	_, ok := kv.notifyChannel[commandIndex]
	if !ok {
		kv.notifyChannel[commandIndex] = make(chan NotifyMsg, 1) // set channel's buffer to be 1.
	}
	return kv.notifyChannel[commandIndex]
}

// close NotifyChannel
func (kv *ShardKV) removeNotifyChannel(commandIndex int) {
	// DPrintf("Gourp: %v. StateMachine: %v. RunState: %v. removeNotifyChannel(). commandIndex: %v.", kv.gid, kv.me, kv.runningState, commandIndex)
	_, ok := kv.notifyChannel[commandIndex]
	if !ok {
		// DPrintf("Gourp: %v. StateMachine: %v. RunState: %v. removeNotifyChannel(). commandIndex: %v. Not OK.", kv.gid, kv.me, kv.runningState, commandIndex)
		return
	}
	close(kv.notifyChannel[commandIndex])
	delete(kv.notifyChannel, commandIndex)
	// DPrintf("Gourp: %v. StateMachine: %v. RunState: %v. removeNotifyChannel(). commandIndex: %v. Delete.", kv.gid, kv.me, kv.runningState, commandIndex)
}

func (kv *ShardKV) getConfigChannel(configNum int, buffer int) chan []byte {
	_, ok := kv.configChannel[configNum]
	if !ok {
		kv.configChannel[configNum] = make(chan []byte, buffer)
	}
	return kv.configChannel[configNum]
}

// close NotifyChannel
func (kv *ShardKV) removeConfigChannel(configNum int) {
	_, ok := kv.configChannel[configNum]
	if !ok {
		return
	}
	close(kv.configChannel[configNum])
	delete(kv.configChannel, configNum)
}

func (kv *ShardKV) queryLastestConfig() {
	for {

		/* kv.mu.Lock()
		_, isLeader := kv.rf.GetState()
		conifg := kv.clerk.Query(kv.curConfig.Num + 1)
		if conifg.Num <= kv.curConfig.Num {
			kv.runningState = Working
			kv.mu.Unlock()
			break
		}
		if !isLeader {
			kv.mu.Unlock()
			break
		}
		kv.mu.Unlock()
		kv.addChangeConfig(conifg) */
		kv.mu.Lock()
		conifg := kv.clerk.Query(kv.curConfig.Num + 1)
		if conifg.Num > kv.curConfig.Num {
			kv.runningState = ReConfiging
			_, isLeader := kv.rf.GetState()
			if isLeader {
				kv.mu.Unlock()
				kv.addChangeConfig(conifg)
			} else {
				kv.mu.Unlock()
			}
		} else {
			kv.runningState = Working
			kv.mu.Unlock()
		}
		/* _, isLeader := kv.rf.GetState()
		if isLeader {
			// DPrintf("Gourp: %v. StateMachine: %v. RunState: %v. queryLastestConfig().", kv.gid, kv.me, kv.runningState)
			kv.mu.Lock()
			conifg := kv.clerk.Query(kv.curConfig.Num + 1)
			if conifg.Num > kv.curConfig.Num {
				kv.runningState = ReConfiging
				kv.mu.Unlock()
				kv.addChangeConfig(conifg)
			} else {
				kv.runningState = Working
				kv.mu.Unlock()
			}
		} */
		time.Sleep(QueryConfigTime)
	}
}

func (kv *ShardKV) addChangeConfig(config shardctrler.Config) {
	kv.mu.Lock()
	oldConfig := kv.curConfig
	// kv.mu.Unlock()
	DPrintf("Gourp: %v. StateMachine: %v. RunState: %v. addChangeConfig(). oldConfig: %v. newConfig: %v. gid: %v.", kv.gid, kv.me, kv.runningState, oldConfig, config, kv.gid)
	changesLog := make([]ConfigChange, 0)
	opJoinNum := 0
	// Op: Join Leave
	for i := 0; i < len(oldConfig.Shards); i++ {
		// scan Leave
		if oldConfig.Shards[i] == kv.gid && config.Shards[i] != kv.gid {
			changesLog = append(changesLog, ConfigChange{
				Shard:      i,
				Op:         ShardOpLeave,
				FormerGid:  kv.gid,
				CurrentGid: config.Shards[i],
			})
		}
		// scan Join
		if config.Shards[i] == kv.gid && oldConfig.Shards[i] != kv.gid {
			changesLog = append(changesLog, ConfigChange{
				Shard:      i,
				Op:         ShardOpJoin,
				FormerGid:  oldConfig.Shards[i],
				CurrentGid: kv.gid,
			})
			opJoinNum++
		}
	}
	kv.mu.Unlock()
	for i := 0; i < len(changesLog); i++ {
		switch changesLog[i].Op {
		case ShardOpJoin:
			DPrintf("Gourp: %v. StateMachine: %v. RunState: %v. addChangeConfig(). Join. log: %v.", kv.gid, kv.me, kv.runningState, changesLog[i])
			if changesLog[i].FormerGid != 0 {
				kv.sendRequestGetShardData(changesLog[i], oldConfig, config, len(changesLog))
			} else {
				DPrintf("Gourp: %v. StateMachine: %v. RunState: %v. addChangeConfig(). Join. log: %v. %v.", kv.gid, kv.me, kv.runningState, changesLog[i], len(changesLog))
				kv.mu.Lock()
				chTemp := kv.getConfigChannel(config.Num, len(changesLog))
				kv.mu.Unlock()
				chTemp <- nil
			}
		case ShardOpLeave:
			// do nothing
		}
	}

	kv.mu.Lock()
	ch := kv.getConfigChannel(config.Num, len(changesLog))
	kv.mu.Unlock()

	finish := 0
	table := make(KVData)
	requestNum := make(map[int64]int)
	caches := make(map[int64]NotifyMsg)
	for {
		if finish >= opJoinNum {
			break
		}
		data := <-ch
		if len(data) >= 1 {
			r := bytes.NewBuffer(data)
			d := labgob.NewDecoder(r)
			var temp KVData
			var req map[int64]int
			var ca map[int64]NotifyMsg
			d.Decode(&temp)
			d.Decode(&req)
			d.Decode(&ca)
			for key, value := range temp {
				table[key] = value
			}
			for key, value := range req {
				if value > requestNum[key] {
					requestNum[key] = value
					caches[key] = ca[key]
				}
			}
		}
		finish++
		DPrintf("Gourp: %v. StateMachine: %v. RunState: %v. addChangeConfig(). Wait. finish: %v.", kv.gid, kv.me, kv.runningState, finish)
	}
	DPrintf("Gourp: %v. StateMachine: %v. RunState: %v. addChangeConfig(). Add to Log. %v", kv.gid, kv.me, kv.runningState, oldConfig.Num)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(table)
	e.Encode(requestNum)
	e.Encode(caches)
	data := w.Bytes()
	change := ConfigChangesLog{
		NewConfig: config,
		Data:      data,
	}
	command := Command{
		ChangesLog:  change,
		Op:          OpConfigChange,
		ClinetId:    0,
		SequenceNum: 0,
	}
	kv.rf.Start(command)
	kv.mu.Lock()
	kv.removeConfigChannel(config.Num)
	kv.mu.Unlock()
}

func (kv *ShardKV) sendRequestGetShardData(changeLog ConfigChange, oldConfig shardctrler.Config, newConfig shardctrler.Config, channalBuffer int) {
	kv.clerk.CreateNullRequest()
	reply := kv.clerk.GetShardData(kv.make_end, oldConfig.Groups[changeLog.FormerGid], oldConfig.Num, changeLog.Shard)
	DPrintf("Gourp: %v. StateMachine: %v. RunState: %v. applyConfigChange. shard: %v. sq: %v.", kv.gid, kv.me, kv.runningState, changeLog.Shard, reply.Sq)
	kv.mu.Lock()
	ch := kv.getConfigChannel(newConfig.Num, channalBuffer)
	kv.mu.Unlock()

	ch <- reply.Data
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(Command{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.lastRequestNum = make(map[int64]int)
	kv.rpcGetCache = make(map[int64]NotifyMsg)
	kv.kvTable = make(KVData)
	kv.notifyChannel = make(map[int]chan NotifyMsg)
	kv.clerk = shardctrler.MakeClerk(kv.ctrlers)
	kv.curConfig = shardctrler.Config{}
	kv.runningState = ReConfiging
	kv.configChannel = make(map[int]chan []byte)
	kv.readSnapShot(kv.rf.ReadSnapshot())
	go kv.apply()
	go kv.queryLastestConfig()
	return kv
}
