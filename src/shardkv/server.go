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

const Debug = false
const ApplyTimeout = time.Millisecond * 300
const QueryConfigTime = time.Millisecond * 100

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
	Changes   []ConfigChange
	OldConfig shardctrler.Config
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
	id             int64
	sequenceNum    int
	curConfig      shardctrler.Config
	runningState   RuningState
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

func (kv *ShardKV) GetShardData(args *GetShardDataArgs, reply *GetShardDatareply) {
	kv.mu.Lock()
	request, ok := kv.lastRequestNum[args.ClientId]
	if !ok {
		request = 0
	}

	// a command whose serial number has already been executed
	if request >= args.SequenceNum {
		reply.Err = kv.rpcGetCache[args.ClientId].Err
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
		DPrintf("Gourp: %v. StateMachine: %v. GetShardData(). Not leader. arg: %v.", kv.gid, kv.me, args)
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
		reply.Err = res.Err
		reply.ConfigNum = res.ConfigNum
		reply.Data = res.Data
		DPrintf("Gourp: %v. StateMachine: %v. RunState: %v. GetShardData(). arg: %v.", kv.gid, kv.me, kv.runningState, args)
	case <-time.After(ApplyTimeout * 5):
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
		if kv.lastRequestNum[command.ClinetId] >= command.SequenceNum {
			if command.Op != OpGet && command.Op != OpGetShardData {
				return notify
			}
			// read cache
			DPrintf("Gourp: %v. StateMachine: %v. RunState: %v. dealWithCommandMsg(). Op: %v. msg: %v. check duplicate. kv.rpcGetCache[command.ClinetId]: %v.", kv.gid, kv.me, kv.runningState, command.Op, msg, kv.rpcGetCache[command.ClinetId])
			return kv.rpcGetCache[command.ClinetId]
		}
		switch {
		case command.Op == OpPut:
			kv.kvTable[command.Key] = command.Value
			DPrintf("Gourp: %v. StateMachine: %v. RunState: %v. dealWithCommandMsg(). OpPut. msg: %v. kv.kvTable[command.Key]: %v.", kv.gid, kv.me, kv.runningState, msg, kv.kvTable[command.Key])
		case command.Op == OpAppend:
			kv.kvTable[command.Key] += command.Value
			DPrintf("Gourp: %v. StateMachine: %v. RunState: %v. dealWithCommandMsg(). OpAppend. msg: %v. kv.kvTable[command.Key]: %v.", kv.gid, kv.me, kv.runningState, msg, kv.kvTable[command.Key])
		case command.Op == OpGet:
			_, ok := kv.kvTable[command.Key]
			if !ok {
				notify.Err = ErrNoKey
				DPrintf("Gourp: %v. StateMachine: %v. RunState: %v. dealWithCommandMsg(). OpGet. msg: %v. ErrNoKey", kv.gid, kv.me, kv.runningState, msg)
			} else {
				notify.Value = kv.kvTable[command.Key]
			}
			kv.rpcGetCache[command.ClinetId] = notify // ceche get
		case command.Op == OpGetShardData:
			/* DPrintf("Gourp: %v. StateMachine: %v. RunState: %v. dealWithCommandMsg(). OpGetShardData. Config: %v. Shard: %v.", kv.gid, kv.me, kv.runningState, command.ConfigNum, command.Shard)
			if command.ConfigNum != kv.curConfig.Num {
				notify.Err = "" // TODO
				kv.rpcGetCache[command.ClinetId] = notify
				return notify
			} */
			shardsMap := make(KVData, 0)
			for key, value := range kv.kvTable {
				if command.Shard == key2shard(key) {
					shardsMap[key] = value
				}
			}
			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)
			e.Encode(shardsMap)
			data := w.Bytes()
			notify.Data = data
			kv.rpcGetCache[command.ClinetId] = notify
			DPrintf("Gourp: %v. StateMachine: %v. RunState: %v. dealWithCommandMsg(). OpGetShardData. End. Config: %v. Shard: %v. notify.Err: %v.", kv.gid, kv.me, kv.runningState, command.ConfigNum, command.Shard, notify.Err)
		case command.Op == OpConfigChange:
			if kv.curConfig.Num >= command.ChangesLog.NewConfig.Num {
				break
			}
			kv.runningState = ReConfiging
			changesLog := command.ChangesLog.Changes
			oldConfig := command.ChangesLog.OldConfig

			for i := 0; i < len(changesLog); i++ {
				switch changesLog[i].Op {
				case ShardOpJoin:
					DPrintf("Gourp: %v. StateMachine: %v. RunState: %v. dealWithCommandMsg(). Join. log: %v.", kv.gid, kv.me, kv.runningState, changesLog[i])
					if changesLog[i].FormerGid != 0 {
						kv.sequenceNum++
						isSucceed := false
						for {
							if isSucceed {
								break
							}
							cond := sync.NewCond(&kv.mu)
							finish := 0
							for j := 0; j < len(oldConfig.Groups[changesLog[i].FormerGid]); j++ {
								args := GetShardDataArgs{
									ConfigNum:   oldConfig.Num,
									Shard:       changesLog[i].Shard,
									ClientId:    kv.id,
									SequenceNum: kv.sequenceNum,
								}
								reply := GetShardDatareply{}
								DPrintf("Gourp: %v. StateMachine: %v. RunState: %v. dealWithCommandMsg(). Join. PRC to %v. arg: %v.", kv.gid, kv.me, kv.runningState, oldConfig.Groups[changesLog[i].FormerGid][j], args)
								go kv.sendRequestGetShardData(oldConfig.Groups[changesLog[i].FormerGid][j], &args, &reply, cond, &finish, &isSucceed)
							}
							for finish < len(oldConfig.Groups[changesLog[i].FormerGid]) && !isSucceed {
								DPrintf("Gourp: %v. StateMachine: %v. RunState: %v. dealWithCommandMsg(). Join Wait.", kv.gid, kv.me, kv.runningState)
								cond.Wait()
							}
						}
					}
				case ShardOpLeave:
					// do nothing
				}
			}
			kv.curConfig = command.ChangesLog.NewConfig
			kv.runningState = Working
		default:
			notify.Err = ErrType
			// DPrintf("Gourp: %v. StateMachine: %v. RunState: %v. Apply(). Msg type error. msg: %v.", kv.gid, kv.me, kv.runningState, msg)
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

func (kv *ShardKV) readSnapShot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var lastRequestNum map[int64]int
	var rpcGetCache map[int64]NotifyMsg
	var kvTable KVData
	if d.Decode(&lastRequestNum) == nil && d.Decode(&rpcGetCache) == nil && d.Decode(&kvTable) == nil {
		kv.lastRequestNum = lastRequestNum
		kv.rpcGetCache = rpcGetCache
		kv.kvTable = kvTable
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

func (kv *ShardKV) queryLastestConfig() {
	for {
		_, isLeader := kv.rf.GetState()
		if isLeader && kv.runningState == Working {
			// DPrintf("Gourp: %v. StateMachine: %v. RunState: %v. queryLastestConfig().", kv.gid, kv.me, kv.runningState)
			conifg := kv.clerk.Query(-1)
			kv.mu.Lock()
			if conifg.Num > kv.curConfig.Num {
				kv.runningState = ReConfiging
				go kv.addChangeConfig(conifg)
			}
			kv.mu.Unlock()
		}
		time.Sleep(QueryConfigTime)
	}
}

func (kv *ShardKV) addChangeConfig(config shardctrler.Config) {
	kv.mu.Lock()
	oldConfig := kv.curConfig
	kv.mu.Unlock()
	DPrintf("Gourp: %v. StateMachine: %v. RunState: %v. addChangeConfig(). oldConfig: %v. newConfig: %v. gid: %v.", kv.gid, kv.me, kv.runningState, oldConfig, config, kv.gid)
	changesLog := make([]ConfigChange, 0)
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
		}
	}
	kv.sequenceNum++
	command := Command{
		ChangesLog: ConfigChangesLog{
			Changes:   changesLog,
			OldConfig: oldConfig,
			NewConfig: config,
		},
		Op:          OpConfigChange,
		ClinetId:    kv.id,
		SequenceNum: kv.sequenceNum,
	}
	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		return
	}
	DPrintf("Gourp: %v. StateMachine: %v. RunState: %v. addChangeConfig(). changesLog: %v.", kv.gid, kv.me, kv.runningState, changesLog)

	kv.mu.Lock()
	ch := kv.getNotifyChannel(index)
	kv.mu.Unlock()

	select {
	case <-ch:
		DPrintf("Gourp: %v. StateMachine: %v. RunState: %v. addChangeConfig(). Success.", kv.gid, kv.me, kv.runningState)
	}
	kv.mu.Lock()
	kv.removeNotifyChannel(index)
	kv.mu.Unlock()
}

type GetShardDataArgs struct {
	ConfigNum   int
	Shard       int
	ClientId    int64
	SequenceNum int
}

type GetShardDatareply struct {
	Err       Err
	ConfigNum int
	Data      []byte
}

func (kv *ShardKV) sendRequestGetShardData(server string, args *GetShardDataArgs, reply *GetShardDatareply, cond *sync.Cond, finish *int, isSucceed *bool) {
	if *isSucceed {
		*finish = *finish + 1
		return
	}

	ok := kv.make_end(server).Call("ShardKV.GetShardData", args, reply)

	kv.mu.Lock()
	defer kv.mu.Unlock()
	defer cond.Broadcast()
	if !ok {
		*finish = *finish + 1
		DPrintf("Gourp: %v. StateMachine: %v. RunState: %v. sendRequestGetShardData(). RPC to %v fail.", kv.gid, kv.me, kv.runningState, server)
		return
	}

	DPrintf("Gourp: %v. StateMachine: %v. RunState: %v. sendRequestGetShardData(). RPC to %v Success. args: %v. Err: %v.", kv.gid, kv.me, kv.runningState, server, args, reply.Err)
	if reply.Err != OK {
		*finish = *finish + 1
		return
	}

	/* if kv.curConfig.Num >= reply.ConfigNum {
		*finish = *finish + 1
		return
	} */

	if reply.Data == nil || len(reply.Data) < 1 {
		*finish = *finish + 1
		*isSucceed = true
		return
	}

	r := bytes.NewBuffer(reply.Data)
	d := labgob.NewDecoder(r)
	var table KVData
	d.Decode(&table)
	for key, value := range table {
		kv.kvTable[key] = value
	}
	*isSucceed = true
	*finish = *finish + 1
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
	kv.id = kv.clerk.GetId()
	kv.sequenceNum = 0
	kv.curConfig = shardctrler.Config{}
	kv.runningState = Working
	kv.readSnapShot(kv.rf.ReadSnapshot())
	go kv.apply()
	go kv.queryLastestConfig()
	return kv
}
