package shardkv

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
)

const Debug = true
const ApplyTimeout = time.Millisecond * 300
const QueryConfigTime = time.Millisecond * 100

const (
	ShardOpJoin    = "Join"
	ShardOpLeave   = "Leave"
	ConfigOpUpdate = "Update"
)

const (
	ShardStatusWorking = "Working"
	ShardStatusPushing = "Pushing"
	ShardStatusWaiting = "Waiting"
)

type KVData map[string]string
type ShardOp string
type ShardStatus string

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func (kv *ShardKV) DDDDDFPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug && kv.gid == 100 {
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
	Op        string
	Shard     int
	ConfigNum int
	Key       string       // For Get, Put, Append
	Value     string       // For Put, Append
	Change    ConfigChange // ConfigChange
	Data      []byte       // For PushShardData
	// specify request
	ClinetId    int64
	SequenceNum int
}

// server will get this msg when command reachs a consensue in Raft.
type NotifyMsg struct {
	Key   string
	Value string
	// For GetShard
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
	NewConfig  shardctrler.Config
}

type Shard struct {
	ConfigNum int
	Status    ShardStatus
	Kvtable   KVData
	Cache     map[int64]NotifyMsg
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int   // snapshot if log grows this big
	dead         int32 // set by Kill()

	// Your definitions here.
	lastApplied   int                    // the index of raft log has applied in state machine
	notifyChannel map[int]chan NotifyMsg // notify server to respond the request
	clerk         *shardctrler.Clerk     // a clinet communicate with shardcontroller
	curConfig     shardctrler.Config
	shards        []Shard
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	DPrintf("Gourp: %v. StateMachine: %v. Get(). arg: %v.", kv.gid, kv.me, args)
	n := key2shard(args.Key) // which shard
	// to check duplicate request
	// the requests that are from different clinet can be concurrent.
	// we can assume a client will make only one call into a Clerk at a time,
	request, ok := kv.shards[n].Cache[args.ClientId]
	if !ok {
		request.SequenceNum = 0
	}
	// a command whose serial number has already been executed
	if request.SequenceNum >= args.SequenceNum {
		reply.Err = request.Err
		reply.Value = request.Value
		DPrintf("Gourp: %v. StateMachine: %v. Get(). request %v >= args.SequenceNum %v. arg: %v. reply %v.", kv.gid, kv.me, request, args.SequenceNum, args, reply)
		kv.mu.Unlock()
		return
	}
	// Can shard provide service?
	if args.ConfigNum != kv.shards[n].ConfigNum || kv.shards[n].Status != ShardStatusWorking {
		reply.Err = ErrWrongGroup
		DPrintf("Gourp: %v. StateMachine: %v. PutAppend(). ErrWrongGroup. kv.curConfig: %v. args.configNum: %v. arg: %v.", kv.gid, kv.me, kv.curConfig, args.ConfigNum, args)
		DPrintf("Gourp: %v. StateMachine: %v. PutAppend(). ErrWrongGroup. Shard: %v. ConfigNum: %v. Status: %v.", kv.gid, kv.me, n, kv.shards[n].ConfigNum, kv.shards[n].Status)
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
	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf("Gourp: %v. StateMachine: %v. Get(). index: %v. arg: %v.", kv.gid, kv.me, index, args)
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
			DPrintf("Gourp: %v. StateMachine: %v. Get(). Command doesn't match. Send Start. arg: %v. res: %v. reply %v.", kv.gid, kv.me, args, res, reply)
			ch <- res // send msg to channel, because there is another thread matched this log is still waitting. It is safe for buffer is 1.
			DPrintf("Gourp: %v. StateMachine: %v. Get(). Command doesn't match. Send End. arg: %v. res: %v. reply %v.", kv.gid, kv.me, args, res, reply)
			return
		}
		reply.Err = res.Err
		reply.Value = res.Value
		DPrintf("Gourp: %v. StateMachine: %v. Get(). arg: %v. res: %v. reply %v.", kv.gid, kv.me, args, res, reply)
	case <-time.After(ApplyTimeout):
		reply.Err = ErrTimeout
		DPrintf("Gourp: %v. StateMachine: %v. Get(). ErrTimeout. arg: %v.", kv.gid, kv.me, args)
	}

	kv.mu.Lock()
	kv.removeNotifyChannel(index)
	kv.mu.Unlock()
}

// PutAppend is equivalent to Get.
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	DPrintf("Gourp: %v. StateMachine: %v. PutAppend(). arg: %v.", kv.gid, kv.me, args)
	n := key2shard(args.Key)
	request, ok := kv.shards[n].Cache[args.ClientId]
	if !ok {
		request.SequenceNum = 0
	}
	if request.SequenceNum >= args.SequenceNum {
		reply.Err = request.Err
		DPrintf("Gourp: %v. StateMachine: %v. PutAppend(). request %v >= args.SequenceNum %v. arg: %v. reply %v.", kv.gid, kv.me, request, args.SequenceNum, args, reply)
		kv.mu.Unlock()
		return
	}
	if args.ConfigNum != kv.shards[n].ConfigNum || kv.shards[n].Status != ShardStatusWorking {
		reply.Err = ErrWrongGroup
		DPrintf("Gourp: %v. StateMachine: %v. PutAppend(). ErrWrongGroup. kv.curConfig: %v. args.configNum: %v. arg: %v.", kv.gid, kv.me, kv.curConfig, args.ConfigNum, args)
		DPrintf("Gourp: %v. StateMachine: %v. PutAppend(). ErrWrongGroup. Shard: %v. ConfigNum: %v. Status: %v.", kv.gid, kv.me, n, kv.shards[n].ConfigNum, kv.shards[n].Status)
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
			DPrintf("Gourp: %v. StateMachine: %v. PutAppend(). Command doesn't match. Send Start. arg: %v. res: %v. reply %v.", kv.gid, kv.me, args, res, reply)
			ch <- res
			DPrintf("Gourp: %v. StateMachine: %v. PutAppend(). Command doesn't match. Send End. arg: %v. res: %v. reply %v.", kv.gid, kv.me, args, res, reply)
			return
		}
		reply.Err = res.Err
		DPrintf("Gourp: %v. StateMachine: %v. PutAppend(). res: %v. arg: %v.", kv.gid, kv.me, res, args)
	case <-time.After(ApplyTimeout):
		DPrintf("Gourp: %v. StateMachine: %v. PutAppend(). ErrTimeout. arg: %v. index: %v.", kv.gid, kv.me, args, index)
		reply.Err = ErrTimeout
	}

	kv.mu.Lock()
	kv.removeNotifyChannel(index)
	kv.mu.Unlock()
}

// recieve Push request from other Raft clusters
func (kv *ShardKV) PushShardData(args *PushShardDataArg, reply *PushShardDataRply) {
	kv.mu.Lock()
	DPrintf("Gourp: %v. StateMachine: %v. PushShardData(). args: %v.", kv.gid, kv.me, args)
	// first. Check for duplicate
	n := args.Shard
	request, ok := kv.shards[n].Cache[args.ClientId]
	if !ok {
		request.SequenceNum = 0
	}
	// a command whose serial number has already been executed
	if request.SequenceNum >= args.SequenceNum {
		reply.Err = request.Err
		DPrintf("Gourp: %v. StateMachine: %v. PushShardData(). request %v >= args.SequenceNum %v. arg: %v. reply %v.", kv.gid, kv.me, request, args.SequenceNum, args, reply)
		kv.mu.Unlock()
		return
	}
	if kv.shards[args.Shard].ConfigNum >= args.ConfigNum {
		reply.Err = OK
		DPrintf("Gourp: %v. StateMachine: %v. PushShardData(). Shard: %v. ConfigNum: %v. >= args.ConfigNum. arg: %v.", kv.gid, kv.me, args.Shard, kv.shards[args.Shard].ConfigNum, args)
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	command := Command{
		Op:          OpPushShardData,
		Shard:       args.Shard,
		ConfigNum:   args.ConfigNum,
		Data:        args.Data,
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
			DPrintf("Gourp: %v. StateMachine: %v. PushShardData(). Command doesn't match. Send Start. arg: %v. res: %v. reply %v.", kv.gid, kv.me, args, res, reply)
			ch <- res
			DPrintf("Gourp: %v. StateMachine: %v. PushShardData(). Command doesn't match. Send End. arg: %v. res: %v. reply %v.", kv.gid, kv.me, args, res, reply)
			return
		}
		reply.Err = res.Err
		DPrintf("Gourp: %v. StateMachine: %v. PushShardData(). res: %v. arg: %v.", kv.gid, kv.me, res, args)
	case <-time.After(ApplyTimeout):
		DPrintf("Gourp: %v. StateMachine: %v. PushShardData(). ErrTimeout. arg: %v. index: %v.", kv.gid, kv.me, args, index)
		reply.Err = ErrTimeout
	}

	kv.mu.Lock()
	kv.removeNotifyChannel(index)
	kv.mu.Unlock()
}

// recieve msg from Raft and apply the log to state machine
func (kv *ShardKV) apply() {
	for !kv.killed() {
		msg := <-kv.applyCh
		// DPrintf("Gourp: %v. StateMachine: %v. apply(). get msg. msg.CommandValid: %v. msg.SnapshotValid: %v. index: %v. msg: %v.", kv.gid, kv.me, msg.CommandValid, msg.SnapshotValid, msg.CommandIndex, msg)
		if msg.CommandValid {
			kv.mu.Lock()
			// to check duplicate log
			if kv.lastApplied >= msg.CommandIndex {
				DPrintf("Gourp: %v. StateMachine: %v. apply(). Command. Command has execute. msg: %v.", kv.gid, kv.me, msg)
				kv.mu.Unlock()
				continue
			}
			DPrintf("Gourp: %v. StateMachine: %v. apply(). msg.CommandIndex: %v. Command: %v. ", kv.gid, kv.me, msg.CommandIndex, msg.Command)
			notify := kv.dealWithCommandMsg(msg)
			kv.lastApplied = msg.CommandIndex // update lastApplied
			kv.createSnapshot()               // go kv.createSnapshot() ?
			_, isLeader := kv.rf.GetState()   // check state
			if !isLeader {
				kv.mu.Unlock()
				continue
			}
			// state is leader. to nofity the sever to reply to request
			// DPrintf("Gourp: %v. StateMachine: %v. apply(). Command. msg.CommandIndex: %v. Notify: %v.", kv.gid, kv.me, msg.CommandIndex, notify)
			ch := kv.getNotifyChannel(msg.CommandIndex)
			// DPrintf("Gourp: %v. StateMachine: %v. apply(). Command. Send nofityChan Start. msg.CommandIndex: %v. Notify: %v.", kv.gid, kv.me, msg.CommandIndex, notify)
			ch <- notify // hold on the lock
			DPrintf("Gourp: %v. StateMachine: %v. apply(). Command. Send nofityChan End. msg.CommandIndex: %v. Notify: %v. msg: %v.", kv.gid, kv.me, msg.CommandIndex, notify, msg)
			kv.mu.Unlock()
		} else if msg.SnapshotValid {
			kv.mu.Lock()
			term, _ := kv.rf.GetState()
			DPrintf("Gourp: %v. StateMachine: %v. apply(). Snapshot. Term: %v. msg.SnapshotTerm: %v.", kv.gid, kv.me, term, msg.SnapshotTerm)
			approve := kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot)
			if !approve {
				kv.mu.Unlock()
				continue
			}

			kv.readSnapShot(msg.Snapshot)
			kv.lastApplied = msg.SnapshotIndex
			DPrintf("Gourp: %v. StateMachine: %v. apply(). Snapshot. End. kv.lastApplied: %v. msg.SnapshotTerm: %v.", kv.gid, kv.me, kv.lastApplied, msg.SnapshotTerm)
			kv.mu.Unlock()
		}
	}
	DPrintf("Gourp: %v. StateMachine: %v. apply(). Killed", kv.gid, kv.me)
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
			ClinetId:    command.ClinetId,
			SequenceNum: command.SequenceNum,
		}
		switch {
		case command.Op == OpGet:
			n := key2shard(command.Key)
			// to check for duplicate
			if kv.shards[n].Cache[command.ClinetId].SequenceNum >= command.SequenceNum {
				// read cache
				DPrintf("Gourp: %v. StateMachine: %v. dealWithCommandMsg(). Op: %v. msg: %v. Shard: %v. check duplicate. Cache: %v.", kv.gid, kv.me, command.Op, msg, n, kv.shards[n].Cache[command.ClinetId])
				return kv.shards[n].Cache[command.ClinetId]
			}
			// ownership and config match
			if command.ConfigNum != kv.shards[n].ConfigNum || kv.shards[n].Status != ShardStatusWorking {
				notify.Err = ErrWrongGroup
				return notify // don't save cache and request
			}
			_, ok := kv.shards[n].Kvtable[command.Key]
			if !ok {
				notify.Err = ErrNoKey
				DPrintf("Gourp: %v. StateMachine: %v. dealWithCommandMsg(). OpGet. msg: %v. ErrNoKey. shard: %v data: %v.", kv.gid, kv.me, msg, n, kv.shards[n])
			} else {
				notify.Value = kv.shards[n].Kvtable[command.Key]
			}
			// save this request
			notify.Key = command.Key
			kv.shards[n].Cache[command.ClinetId] = notify
		case command.Op == OpPut:
			n := key2shard(command.Key)
			if kv.shards[n].Cache[command.ClinetId].SequenceNum >= command.SequenceNum {
				DPrintf("Gourp: %v. StateMachine: %v. dealWithCommandMsg(). Op: %v. msg: %v. Shard: %v. check duplicate. Cache: %v.", kv.gid, kv.me, command.Op, msg, n, kv.shards[n].Cache[command.ClinetId])
				return kv.shards[n].Cache[command.ClinetId]
			}
			if command.ConfigNum != kv.shards[n].ConfigNum || kv.shards[n].Status != ShardStatusWorking {
				notify.Err = ErrWrongGroup
				return notify
			}
			kv.shards[n].Kvtable[command.Key] = command.Value
			notify.Key = command.Key
			kv.shards[n].Cache[command.ClinetId] = notify
		case command.Op == OpAppend:
			n := key2shard(command.Key)
			if kv.shards[n].Cache[command.ClinetId].SequenceNum >= command.SequenceNum {
				DPrintf("Gourp: %v. StateMachine: %v. dealWithCommandMsg(). Op: %v. msg: %v. Shard: %v. check duplicate. Cache: %v.", kv.gid, kv.me, command.Op, msg, n, kv.shards[n].Cache[command.ClinetId])
				return kv.shards[n].Cache[command.ClinetId]
			}
			if command.ConfigNum != kv.shards[n].ConfigNum || kv.shards[n].Status != ShardStatusWorking {
				notify.Err = ErrWrongGroup
				return notify
			}
			kv.shards[n].Kvtable[command.Key] += command.Value
			notify.Key = command.Key
			kv.shards[n].Cache[command.ClinetId] = notify
		case command.Op == OpPushShardData:
			if kv.shards[command.Shard].Cache[command.ClinetId].SequenceNum >= command.SequenceNum {
				DPrintf("Gourp: %v. StateMachine: %v. dealWithCommandMsg(). Op: %v. msg: %v. Shard: %v. check duplicate. Cache: %v.", kv.gid, kv.me, command.Op, msg, command.Shard, kv.shards[command.Shard].Cache[command.ClinetId])
				return kv.shards[command.Shard].Cache[command.ClinetId]
			}
			kv.dealWithOpPushShardData(command)
			notify.Err = OK
			kv.shards[command.Shard].Cache[command.ClinetId] = notify
		case command.Op == OpConfigChange:
			// OpConfigChange needn't check for duplicate
			kv.dealWithOpConfigChange(command)
		default:
			notify.Err = ErrType
		}
		return notify
	}
	return NotifyMsg{
		Value:       value,
		Err:         OK,
		ClinetId:    -1,
		SequenceNum: -1,
	}
}

func (kv *ShardKV) dealWithOpConfigChange(command Command) {
	switch command.Change.Op {
	case ShardOpJoin:
		if kv.shards[command.Shard].ConfigNum >= command.ConfigNum {
			DPrintf("Gourp: %v. StateMachine: %v. OpConfigChange(). Join Quit. Shard: %v. curNum: %v. commadConfig: %v.", kv.gid, kv.me, command.Shard, kv.shards[command.Shard].ConfigNum, command.ConfigNum)
			return
		}
		// if former own is nobody, StateMachine will join this shard soon.
		if command.Change.FormerGid == 0 {
			kv.shards[command.Shard].ConfigNum = command.ConfigNum
			break
		}
		kv.shards[command.Shard].Status = ShardStatusWaiting // wait data from pushing
		DPrintf("Gourp: %v. StateMachine: %v. OpConfigChange(). Join. Shard: %v. FGid: %v. CGid: %v. newConfigNum: %v.", kv.gid, kv.me, command.Shard, command.Change.FormerGid, command.Change.CurrentGid, command.ConfigNum)
	case ShardOpLeave:
		if kv.shards[command.Shard].ConfigNum >= command.ConfigNum || kv.shards[command.Shard].Status != ShardStatusWorking {
			DPrintf("Gourp: %v. StateMachine: %v. OpConfigChange(). Leave Quit. Shard: %v. Status: %v. curNum: %v. commadConfig: %v.", kv.gid, kv.me, command.Shard, kv.shards[command.Shard].Status, kv.shards[command.Shard].ConfigNum, command.ConfigNum)
			return
		}
		// start to push data to new own
		kv.shards[command.Shard].Status = ShardStatusPushing
		go kv.sendRequestPushShardData(kv.make_end, command.Change.NewConfig.Groups[command.Change.CurrentGid], command.Shard, command.ConfigNum, kv.encodeKVShard(command.Shard))
		DPrintf("Gourp: %v. StateMachine: %v. OpConfigChange(). Leave. End. Shard: %v. newConfig: %v.", kv.gid, kv.me, command.Shard, kv.shards[command.Shard])
		DPrintf("Gourp: %v. StateMachine: %v. OpConfigChange(). Leave. End. Shard: %v. Data: %v.", kv.gid, kv.me, command.Shard, kv.shards[command.Shard])
	case ConfigOpUpdate:
		if kv.curConfig.Num >= command.ConfigNum {
			DPrintf("Gourp: %v. StateMachine: %v. OpConfigChange(). Update Quit. curNum: %v. commadConfig: %v.", kv.gid, kv.me, kv.curConfig, command.ConfigNum)
			return
		}
		kv.curConfig = command.Change.NewConfig
		DPrintf("Gourp: %v. StateMachine: %v. OpConfigChange(). Update. newConfig: %v.", kv.gid, kv.me, kv.curConfig)
	default:
		if kv.shards[command.Shard].ConfigNum >= command.ConfigNum {
			DPrintf("Gourp: %v. StateMachine: %v. OpConfigChange(). Quit. Shard: %v. curNum: %v. commadConfig: %v.", kv.gid, kv.me, command.Shard, kv.shards[command.Shard].ConfigNum, command.ConfigNum)
			return
		}
		kv.shards[command.Shard].ConfigNum = command.ConfigNum
		DPrintf("Gourp: %v. StateMachine: %v. OpConfigChange(). Null. Shard: %v. newConfig: %v.", kv.gid, kv.me, command.Shard, kv.shards[command.Shard])
	}
}

func (kv *ShardKV) dealWithOpPushShardData(command Command) {
	DPrintf("Gourp: %v. StateMachine: %v. OpPushShardData(). Shard: %v. From: %v. ConfigNum: %v. cmdConfigNum: %v.", kv.gid, kv.me, command.Shard, command.ClinetId, kv.shards[command.Shard].ConfigNum, command.ConfigNum)
	if kv.shards[command.Shard].ConfigNum >= command.ConfigNum {
		return
	}
	kv.decodeKVShard(command.Data, command.Shard)
	kv.shards[command.Shard].ConfigNum = command.ConfigNum
	kv.shards[command.Shard].Status = ShardStatusWorking
	DPrintf("Gourp: %v. StateMachine: %v. OpPushShardData(). Shard: %v. Data: %v.", kv.gid, kv.me, command.Shard, kv.shards[command.Shard])
}

func (kv *ShardKV) readSnapShot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var curConfig shardctrler.Config
	var shards []Shard
	if d.Decode(&curConfig) == nil && d.Decode(&shards) == nil {
		kv.curConfig = curConfig
		kv.shards = shards
	}
	DPrintf("Gourp: %v. StateMachine: %v. readSnapShot(). Config: %v. shards: %v. ", kv.gid, kv.me, curConfig, shards)
	DPrintf("Gourp: %v. StateMachine: %v. readSnapShot(). kv.curConfig: %v. kv.shards: %v. ", kv.gid, kv.me, kv.curConfig, kv.shards)
}

func (kv *ShardKV) createSnapshot() {
	if kv.maxraftstate < 0 {
		return
	}
	/* kv.mu.Lock()
	defer kv.mu.Unlock() */
	size := kv.rf.GetRaftStateSize()
	if size < kv.maxraftstate {
		return
	}
	DPrintf("Gourp: %v. StateMachine: %v. createSnapshot(). RaftStateSize: %v. maxraftstate: %v.", kv.gid, kv.me, size, kv.maxraftstate)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.curConfig)
	e.Encode(kv.shards)
	data := w.Bytes()
	DPrintf("Gourp: %v. StateMachine: %v. createSnapshot(). Start snapshot. kv.lastApplied: %v. kv.curConfig: %v. kv.shards: %v.", kv.gid, kv.me, kv.lastApplied, kv.curConfig, kv.shards)
	kv.rf.Snapshot(kv.lastApplied, data)
	DPrintf("Gourp: %v. StateMachine: %v. createSnapshot(). End snapshot. kv.lastApplied: %v. kv.curConfig: %v. kv.shards: %v.", kv.gid, kv.me, kv.lastApplied, kv.curConfig, kv.shards)
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
	_, ok := kv.notifyChannel[commandIndex]
	if !ok {
		return
	}
	close(kv.notifyChannel[commandIndex])
	delete(kv.notifyChannel, commandIndex)
}

func (kv *ShardKV) queryLastestConfig() {
	// for !kv.killed() { can't pass TestUnreliable3?
	for !kv.killed() {
		time.Sleep(QueryConfigTime)
		_, isLeader := kv.rf.GetState()
		// DPrintf("Gourp: %v. queryLastestConfig(). StateMachine: %v.", kv.gid, kv.me)
		if isLeader {
			kv.mu.Lock()
			conifg := kv.clerk.Query(kv.curConfig.Num + 1)
			ready := true
			for i, shard := range kv.shards {
				if shard.Status != ShardStatusWorking {
					ready = false
					DPrintf("Gourp: %v. StateMachine: %v. queryLastestConfig(). Shard: %v. shard.ConfigNum: %v. Status: %v.", kv.gid, kv.me, i, shard.ConfigNum, shard.Status)
				}
			}
			if !ready {
				DPrintf("Gourp: %v. StateMachine: %v. queryLastestConfig(). Not ready.", kv.gid, kv.me)
				kv.mu.Unlock()
				continue
			}
			opNum := 0
			if kv.curConfig.Num >= conifg.Num {
				kv.mu.Unlock()
				continue
			}
			for i, shard := range kv.shards {
				DPrintf("Gourp: %v. StateMachine: %v. queryLastestConfig(). Shard: %v. shard.ConfigNum: %v. conifg: %v.", kv.gid, kv.me, i, shard.ConfigNum, conifg)
				if conifg.Num <= shard.ConfigNum {
					continue
				}
				opNum++
				var change ConfigChange
				if kv.curConfig.Shards[i] == kv.gid && conifg.Shards[i] != kv.gid {
					// Shard OP Leave
					change = ConfigChange{
						Shard:      i,
						Op:         ShardOpLeave,
						FormerGid:  kv.gid,
						CurrentGid: conifg.Shards[i],
						NewConfig:  conifg,
					}
				} else if kv.curConfig.Shards[i] != kv.gid && conifg.Shards[i] == kv.gid {
					// Shard OP Join
					change = ConfigChange{
						Shard:      i,
						Op:         ShardOpJoin,
						FormerGid:  kv.curConfig.Shards[i],
						CurrentGid: kv.gid,
						NewConfig:  conifg,
					}
				} else {
					change = ConfigChange{
						Shard:     i,
						NewConfig: conifg,
					}
				}
				go kv.addChangeConfig(kv.make_end, change, kv.curConfig, conifg)
			}
			if opNum == 0 {
				DPrintf("Gourp: %v. StateMachine: %v. queryLastestConfig(). Update Config. newConifg: %v. ", kv.gid, kv.me, conifg)
				cmd := Command{
					ConfigNum: conifg.Num,
					Change: ConfigChange{
						Op:        ConfigOpUpdate,
						NewConfig: conifg,
					},
					Op: OpConfigChange,
				}

				kv.rf.Start(cmd)
			}
			kv.mu.Unlock()
		}
	}
	// DPrintf("Gourp: %v. queryLastestConfig(). StateMachine: %v. Killed", kv.gid, kv.me)
}

func (kv *ShardKV) addChangeConfig(make_end func(string) *labrpc.ClientEnd, change ConfigChange, oldConfig shardctrler.Config, newConfig shardctrler.Config) {
	var index int
	var isLeader bool
	cmd := Command{
		ConfigNum: newConfig.Num,
		Shard:     change.Shard,
		Change:    change,
		Op:        OpConfigChange,
	}

	index, _, isLeader = kv.rf.Start(cmd)
	if isLeader {
		DPrintf("Gourp: %v. StateMachine: %v. addChangeConfig(). Shard: %v. Log index: %v. Op: %v. newConfig: %v.", kv.gid, kv.me, cmd.Shard, index, change.Op, change.NewConfig)
	}
}

type PushShardDataArg struct {
	Shard       int
	ConfigNum   int
	Data        []byte
	ClientId    int64
	SequenceNum int
}

type PushShardDataRply struct {
	Err Err
}

func (kv *ShardKV) sendRequestPushShardData(make_end func(string) *labrpc.ClientEnd, servers []string, shard int, configNum int, data []byte) {
	id := nrand()
	seq := 1
	i := 0
	DPrintf("Gourp: %v. StateMachine: %v. sendRequestPushShardData(). id: %v. Push Shard %v to %v. newConfigNum: %v.", kv.gid, kv.me, id, shard, servers, configNum)
	for !kv.killed() {
		args := PushShardDataArg{
			Shard:       shard,
			ConfigNum:   configNum,
			Data:        data,
			ClientId:    id,
			SequenceNum: seq,
		}
		reply := PushShardDataRply{}
		DPrintf("Gourp: %v. StateMachine: %v. sendRequestPushShardData(). id: %v. to %v.", kv.gid, kv.me, id, servers[i])

		ok := make_end(servers[i]).Call("ShardKV.PushShardData", &args, &reply)

		if !ok {
			i = (i + 1) % len(servers)
			time.Sleep(100 * time.Millisecond)
			DPrintf("Gourp: %v. StateMachine: %v. sendRequestPushShardData(). !ok. id: %v. Push Shard %v to %v. newConfigNum: %v.", kv.gid, kv.me, id, shard, servers, configNum)
			continue
		}

		if reply.Err == OK {
			DPrintf("Gourp: %v. StateMachine: %v. sendRequestPushShardData(). OK. id: %v. Push Shard %v to %v. newConfigNum: %v.", kv.gid, kv.me, id, shard, servers, configNum)
			kv.mu.Lock()
			DPrintf("Gourp: %v. StateMachine: %v. sendRequestPushShardData(). OK. id: %v. Push Shard %v to %v. PushingConfigNum: %v. CurConfig: %v. CurStatus: %v. kv.cur: %v.", kv.gid, kv.me, id, shard, servers, configNum, kv.shards[shard].ConfigNum, kv.shards[shard].Status, kv.curConfig)
			if kv.shards[shard].ConfigNum == configNum-1 {
				kv.shards[shard].Status = ShardStatusWorking
				kv.shards[shard].ConfigNum = configNum
				kv.shards[shard].Kvtable = make(KVData)
				kv.shards[shard].Cache = make(map[int64]NotifyMsg)
				kv.createSnapshot()
			}
			kv.mu.Unlock()
			return
		}

		if reply.Err == ErrWrongGroup {
			i = (i + 1) % len(servers)
			time.Sleep(100 * time.Millisecond)
			DPrintf("Gourp: %v. StateMachine: %v. sendRequestPushShardData(). ErrWrongGroup. id: %v. Pusg Shard %v to %v. newConfigNum: %v.", kv.gid, kv.me, id, shard, servers, configNum)
			continue
		}

		if reply.Err == ErrTimeout {
			i = (i + 1) % len(servers)
			time.Sleep(100 * time.Millisecond)
			DPrintf("Gourp: %v. StateMachine: %v. sendRequestPushShardData(). ErrTimeout. id: %v. Pusg Shard %v to %v. newConfigNum: %v.", kv.gid, kv.me, id, shard, servers, configNum)
			continue
		}

		if reply.Err == ErrWrongLeader {
			i = (i + 1) % len(servers)
			time.Sleep(100 * time.Millisecond)
			DPrintf("Gourp: %v. StateMachine: %v. sendRequestPushShardData(). ErrWrongLeader. id: %v. Pusg Shard %v to %v. newConfigNum: %v.", kv.gid, kv.me, id, shard, servers, configNum)
			continue
		}
	}
}

func (kv *ShardKV) encodeKVShard(shard int) []byte {
	shardsMap := make(KVData)
	cacheReplica := make(map[int64]NotifyMsg)
	for key, value := range kv.shards[shard].Kvtable {
		// DPrintf("Gourp: %v. StateMachine: %v. dealWithOpGetShardData(). shard: %v match key: %v. v: %v.", kv.gid, kv.me, command.Shard, key, value)
		shardsMap[key] = value
	}
	for k, v := range kv.shards[shard].Cache {
		cacheReplica[k] = v
	}
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(shardsMap)
	e.Encode(cacheReplica)
	data := w.Bytes()
	return data
}

func (kv *ShardKV) decodeKVShard(data []byte, shard int) {
	if len(data) < 1 || data == nil {
		// DPrintf("Gourp: %v. StateMachine: %v. dealWithOpConfigChange(). Join. NoData End. newConfig: %v.", kv.gid, kv.me, kv.shards[shard].ConfigNum)
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var table KVData
	var caches map[int64]NotifyMsg
	d.Decode(&table)
	d.Decode(&caches)
	kv.shards[shard].Kvtable = make(KVData)
	kv.shards[shard].Cache = make(map[int64]NotifyMsg)
	for key, value := range table {
		kv.shards[shard].Kvtable[key] = value
		// DPrintf("Gourp: %v. StateMachine: %v. dealWithOpConfigChange(). Join. key: %v. v: %v.", kv.gid, kv.me, key, value)
	}
	for key, value := range caches {
		kv.shards[shard].Cache[key] = value
	}
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
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
	kv.notifyChannel = make(map[int]chan NotifyMsg)
	kv.clerk = shardctrler.MakeClerk(kv.ctrlers)
	kv.shards = make([]Shard, shardctrler.NShards)
	kv.curConfig = shardctrler.Config{}
	for i := 0; i < len(kv.shards); i++ {
		kv.shards[i].Kvtable = make(KVData)
		kv.shards[i].Cache = make(map[int64]NotifyMsg)
	}
	kv.readSnapShot(kv.rf.ReadSnapshot())
	for i := 0; i < len(kv.shards); i++ {
		kv.shards[i].Status = ShardStatusWorking
	}
	go kv.apply()
	go kv.queryLastestConfig()
	return kv
}
