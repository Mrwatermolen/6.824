package shardctrler

import (
	"log"
	"sort"
	"sync"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false
const ApplyTimeout = time.Millisecond * 300

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs        []Config // indexed by config num. decides which replica group should serve each shard.
	configNum      int      // nonessential. == len(configs) - 1
	notifyChannel  map[int]chan NotifyMsg
	lastApplied    int
	lastRequestNum map[int64]int
	rpcQueryCache  map[int64]NotifyMsg
}

type Op struct {
	// Your data here.
}

type Command struct {
	Servers     map[int][]string
	Gids        []int
	Gid         int
	Shard       int
	ConfigNum   int
	Op          string
	ClinetId    int64
	SequenceNum int
}

type NotifyMsg struct {
	Config Config
	Err    Err
}

type ReplicaGroud struct {
	gid    int
	nShard int
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	sc.mu.Lock()
	request, ok := sc.lastRequestNum[args.ClientId]
	if !ok {
		request = 0
	}
	if request >= args.SequenceNum {
		reply.Err = OK
		_, isLeader := sc.rf.GetState()
		reply.WrongLeader = !isLeader
		if !isLeader {
			reply.Err = ErrWrongLeader
		}
		DPrintf("ShardCtrler: %v. Join(). request %v >= args.SequenceNum %v. arg: %v. reply %v.", sc.me, request, args.SequenceNum, args, reply)
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()
	command := Command{
		Servers:     args.Servers,
		Op:          OpSCJoin,
		ClinetId:    args.ClientId,
		SequenceNum: args.SequenceNum,
	}
	index, _, isLeader := sc.rf.Start(command)
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		return
	}

	reply.WrongLeader = false
	sc.mu.Lock()
	DPrintf("ShardCtrler: %v. Join(). args: %v.", sc.me, args)
	ch := sc.getNotifyChannel(index)
	sc.mu.Unlock()

	select {
	case res := <-ch:
		reply.Err = res.Err
		DPrintf("ShardCtrler: %v. Join(). arg: %v. res: %v. reply %v.", sc.me, args, res, reply)
	case <-time.After(ApplyTimeout):
		reply.Err = ErrTimeout
		DPrintf("ShardCtrler: %v. Join(). ErrTimeout. arg: %v.", sc.me, args)
	}

	sc.mu.Lock()
	sc.removeNotifyChannel(index)
	sc.mu.Unlock()
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	sc.mu.Lock()
	request, ok := sc.lastRequestNum[args.ClientId]
	if !ok {
		request = 0
	}
	if request >= args.SequenceNum {
		reply.Err = OK
		_, isLeader := sc.rf.GetState()
		reply.WrongLeader = !isLeader
		if !isLeader {
			reply.Err = ErrWrongLeader
		}
		DPrintf("ShardCtrler: %v. Leave(). request %v >= args.SequenceNum %v. arg: %v. reply %v.", sc.me, request, args.SequenceNum, args, reply)
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()
	command := Command{
		Gids:        args.GIDs,
		Op:          OpSCLeave,
		ClinetId:    args.ClientId,
		SequenceNum: args.SequenceNum,
	}
	index, _, isLeader := sc.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}

	reply.WrongLeader = false
	sc.mu.Lock()
	DPrintf("ShardCtrler: %v. Leave(). args: %v.", sc.me, args)
	ch := sc.getNotifyChannel(index)
	sc.mu.Unlock()
	select {
	case res := <-ch:
		reply.Err = res.Err
		DPrintf("ShardCtrler: %v. Leave(). arg: %v. res: %v. reply %v.", sc.me, args, res, reply)
	case <-time.After(ApplyTimeout):
		reply.Err = ErrTimeout
		DPrintf("ShardCtrler: %v. Leave(). ErrTimeout. arg: %v.", sc.me, args)
	}

	sc.mu.Lock()
	sc.removeNotifyChannel(index)
	sc.mu.Unlock()
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	sc.mu.Lock()
	request, ok := sc.lastRequestNum[args.ClientId]
	if !ok {
		request = 0
	}
	if request >= args.SequenceNum {
		reply.Err = OK
		_, isLeader := sc.rf.GetState()
		reply.WrongLeader = !isLeader
		if !isLeader {
			reply.Err = ErrWrongLeader
		}
		DPrintf("ShardCtrler: %v. Move(). request %v >= args.SequenceNum %v. arg: %v. reply %v.", sc.me, request, args.SequenceNum, args, reply)
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()
	command := Command{
		Gid:         args.GID,
		Shard:       args.Shard,
		Op:          OpSCMove,
		ClinetId:    args.ClientId,
		SequenceNum: args.SequenceNum,
	}
	index, _, isLeader := sc.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}

	reply.WrongLeader = false
	sc.mu.Lock()
	DPrintf("ShardCtrler: %v. Leave(). args: %v.", sc.me, args)
	ch := sc.getNotifyChannel(index)
	sc.mu.Unlock()
	select {
	case res := <-ch:
		reply.Err = res.Err
		DPrintf("ShardCtrler: %v. Move(). arg: %v. res: %v. reply %v.", sc.me, args, res, reply)
	case <-time.After(ApplyTimeout):
		reply.Err = ErrTimeout
		DPrintf("ShardCtrler: %v. Move(). ErrTimeout. arg: %v.", sc.me, args)
	}

	sc.mu.Lock()
	sc.removeNotifyChannel(index)
	sc.mu.Unlock()
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	sc.mu.Lock()
	request, ok := sc.lastRequestNum[args.ClientId]
	if !ok {
		request = 0
	}
	if request >= args.SequenceNum {
		reply.Err = ErrWrongLeader
		_, isLeader := sc.rf.GetState()
		reply.WrongLeader = !isLeader
		if isLeader {
			reply.Config = sc.rpcQueryCache[args.ClientId].Config
			reply.Err = sc.rpcQueryCache[args.ClientId].Err
		}
		DPrintf("ShardCtrler: %v. Query(). request %v >= args.SequenceNum %v. arg: %v. reply %v.", sc.me, request, args.SequenceNum, args, reply)
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()

	command := Command{
		ConfigNum:   args.Num,
		Op:          OpSCQuery,
		ClinetId:    args.ClientId,
		SequenceNum: args.SequenceNum,
	}
	index, _, isLeader := sc.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}

	reply.WrongLeader = false
	sc.mu.Lock()
	DPrintf("ShardCtrler: %v. Query(). args: %v. configs: %v.", sc.me, args, sc.configs)
	ch := sc.getNotifyChannel(index)
	sc.mu.Unlock()

	select {
	case res := <-ch:
		reply.Err = res.Err
		reply.Config = res.Config
		DPrintf("ShardCtrler: %v. Query(). arg: %v. res: %v. reply %v.", sc.me, args, res, reply)
	case <-time.After(ApplyTimeout):
		reply.Err = ErrTimeout
		DPrintf("ShardCtrler: %v. Query(). ErrTimeout. arg: %v.", sc.me, args)
	}

	sc.mu.Lock()
	sc.removeNotifyChannel(index)
	sc.mu.Unlock()
}

func (sc *ShardCtrler) apply() {
	for {
		msg := <-sc.applyCh
		//DPrintf("ShardCtrler: %v. apply(). get msg. msg: %v.", sc.me, msg)
		if msg.CommandValid {
			sc.mu.Lock()
			if sc.lastApplied >= msg.CommandIndex {
				DPrintf("ShardCtrler: %v. apply(). Command. Command has execute. msg: %v.", sc.me, msg)
				sc.mu.Unlock()
				continue
			}
			sc.lastApplied = msg.CommandIndex
			notify := sc.dealWithCommandMsg(msg)
			_, isLeader := sc.rf.GetState() // check state
			if !isLeader {
				sc.mu.Unlock()
				continue
			}
			ch := sc.getNotifyChannel(msg.CommandIndex)
			ch <- notify
			// state is leader. to nofity the sever to reply to request
			DPrintf("ShardCtrler: %v. apply(). Laeder. msg: %v. notify: %v.", sc.me, msg, notify)
			sc.mu.Unlock()
		}
	}
}

func (sc *ShardCtrler) dealWithCommandMsg(msg raft.ApplyMsg) NotifyMsg {
	switch msg.Command.(type) {
	case Command:
		command := msg.Command.(Command)
		notify := NotifyMsg{
			Err: OK, //OK is default
		}
		if sc.lastRequestNum[command.ClinetId] >= command.SequenceNum {
			if command.Op != OpSCQuery {
				return notify
			}
			return sc.rpcQueryCache[command.ClinetId]
		}
		switch {
		case command.Op == OpSCJoin:
			DPrintf("ShardCtrler: %v. dealWithCommandMsg(). OpSCJoin. msg: %v.", sc.me, msg)
			sc.configs = append(sc.configs, sc.generateNewCopiedConfig()) // creating a new configuration and add to configs
			for gid, servers := range command.Servers {
				_, ok := sc.configs[sc.configNum].Groups[gid]
				if !ok {
					sc.configs[sc.configNum].Groups[gid] = servers // add new replica groups
				}
			}
			sc.rebalance()
			DPrintf("ShardCtrler: %v. dealWithCommandMsg(). OpSCJoin. sc.configs: %v.", sc.me, sc.configs)
		case command.Op == OpSCLeave:
			DPrintf("ShardCtrler: %v. dealWithCommandMsg(). OpSCLeave. msg: %v.", sc.me, msg)
			sc.configs = append(sc.configs, sc.generateNewCopiedConfig()) // creating a new configuration and add to configs
			// remove these command.Gids from new config
			for i := 0; i < len(command.Gids); i++ {
				_, ok := sc.configs[sc.configNum].Groups[command.Gids[i]]
				if !ok {
					continue
				}
				delete(sc.configs[sc.configNum].Groups, command.Gids[i]) // delete gid
				for j := 0; j < NShards; j++ {
					if sc.configs[sc.configNum].Shards[j] == command.Gids[i] {
						sc.configs[sc.configNum].Shards[j] = 0 // recycle the shard served by command.Gids[i].
					}
				}
			}
			// Select an existing GID and temporarily assign all free shards to it.
			existedGid := 0
			for i := 0; i < NShards; i++ {
				if sc.configs[sc.configNum].Shards[i] != 0 {
					existedGid = sc.configs[sc.configNum].Shards[i]
				}
			}
			if existedGid == 0 {
				// there in no gid.
				break
			}
			for i := 0; i < NShards; i++ {
				if sc.configs[sc.configNum].Shards[i] == 0 {
					sc.configs[sc.configNum].Shards[i] = existedGid
				}
			}
			sc.rebalance()
			DPrintf("ShardCtrler: %v. dealWithCommandMsg(). OpSCLeave. sc.configs: %v.", sc.me, sc.configs)
		case command.Op == OpSCMove:
			DPrintf("ShardCtrler: %v. dealWithCommandMsg(). OpSCMove. msg: %v.", sc.me, msg)
			sc.configs = append(sc.configs, sc.generateNewCopiedConfig())
			sc.configs[sc.configNum].Shards[command.Shard] = command.Gid
			DPrintf("ShardCtrler: %v. dealWithCommandMsg(). OpSCMove. sc.configs: %v.", sc.me, sc.configs)
		case command.Op == OpSCQuery:
			DPrintf("ShardCtrler: %v. dealWithCommandMsg(). OpSCQuery. msg: %v.", sc.me, msg)
			if command.ConfigNum < 0 || command.ConfigNum > sc.configNum {
				notify.Config = sc.configs[sc.configNum]
				sc.rpcQueryCache[command.ClinetId] = notify
				break
			}
			notify.Config = sc.configs[command.ConfigNum]
			sc.rpcQueryCache[command.ClinetId] = notify
		default:
			notify.Err = "ErrType"
		}
		sc.lastRequestNum[command.ClinetId] = command.SequenceNum // update lastRequestNum
		DPrintf("ShardCtrler: %v. dealWithCommandMsg(). msg: %v. config: %v.", sc.me, msg, sc.configs)
		return notify
	}
	return NotifyMsg{
		Err: OK,
	}
}

// generate a copy of the latest config and increase sc.configNum
func (sc *ShardCtrler) generateNewCopiedConfig() Config {
	newConfig := Config{
		Num: sc.configNum + 1,
	}
	newGroup := map[int][]string{}
	for i := 0; i < NShards; i++ {
		newConfig.Shards[i] = sc.configs[sc.configNum].Shards[i]
	}
	for gid, servers := range sc.configs[sc.configNum].Groups {
		newGroup[gid] = servers
	}
	newConfig.Groups = newGroup
	sc.configNum++
	return newConfig
}

// balance the load
func (sc *ShardCtrler) rebalance() {
	rg := sc.getReplicaGroup()
	DPrintf("ShardCtrler: %v. rebalance(). rg: %v.", sc.me, rg)
	for {
		maxNum := rg[len(rg)-1].nShard
		minNum := rg[0].nShard
		if maxNum-minNum <= 1 {
			if maxNum == 0 {
				// It means there is no shard that is assigned to any gid, so we need to initialization.
				n := (NShards + len(rg) - 1) / len(rg)
				partion := 0
				for i := 0; i < len(rg); i++ {
					for j := partion * n; j < (partion+1)*n && j < NShards; j++ {
						sc.configs[sc.configNum].Shards[j] = rg[i].gid
					}
					partion++
				}
			}
			DPrintf("ShardCtrler: %v. rebalance(). End! maxNum: %v. minNum: %v. rg: %v. newConfig: %v.", sc.me, maxNum, minNum, rg, sc.configs[sc.configNum])
			break
		}
		// assignd a shard which is served by gid holding the most shards to gid holding the lest shards
		for i := 0; i < NShards; i++ {
			if sc.configs[sc.configNum].Shards[i] == rg[len(rg)-1].gid {
				sc.configs[sc.configNum].Shards[i] = rg[0].gid
				break
			}
		}
		rg[0].nShard++
		rg[len(rg)-1].nShard--
		sort.SliceStable(rg, func(i, j int) bool {
			return rg[i].nShard < rg[j].nShard
		})
	}
}

// return the lastest config show gids have how many shards.
// Guaranteed determination
func (sc *ShardCtrler) getReplicaGroup() []ReplicaGroud {
	DPrintf("ShardCtrler: %v. getReplicaGroup().", sc.me)
	replicGroud := make(map[int]int, 0)
	// count gid
	for i := 0; i < NShards; i++ {
		curGid := sc.configs[sc.configNum].Shards[i]
		replicGroud[curGid]++
	}
	// count the gid has how many shards.
	newRG := make([]ReplicaGroud, 0)
	for key := range sc.configs[sc.configNum].Groups {
		newRG = append(newRG, ReplicaGroud{
			gid:    key,
			nShard: replicGroud[key],
		})
	}
	/* Considering this case: Groups = [{1: xxx} {2: yyy}] and replicGroud[key] = [{1: 0} {2: 0}].
	In Go, map iteration order is not deterministic.
	We could get newRG = [{1: 0} {2: 0}] or newRG = [{2: 0} {1: 0}].
	The shard rebalancing needs to be deterministic, so it is necessary to sort by key in first.
	Then using stable sort to sort by nShard*/
	sort.Slice(newRG, func(i, j int) bool {
		return newRG[i].gid < newRG[j].gid
	})
	sort.SliceStable(newRG, func(i, j int) bool {
		return newRG[i].nShard < newRG[j].nShard
	})
	DPrintf("ShardCtrler: %v. getReplicaGroup(). newRG: %v.", sc.me, newRG)
	return newRG
}

// return a channel for notify server that log has been applied to state machine.
// One log is bound to one channel,
// but one channel may be bound to many request from different clinet.
func (sc *ShardCtrler) getNotifyChannel(commandIndex int) chan NotifyMsg {
	_, ok := sc.notifyChannel[commandIndex]
	if !ok {
		sc.notifyChannel[commandIndex] = make(chan NotifyMsg, 1) // set channel's buffer to be 1.
	}
	return sc.notifyChannel[commandIndex]
}

// close NotifyChannel
func (sc *ShardCtrler) removeNotifyChannel(commandIndex int) {
	DPrintf("ShardCtrler: %v. removeNotifyChannel(). commandIndex: %v.", sc.me, commandIndex)
	_, ok := sc.notifyChannel[commandIndex]
	if !ok {
		DPrintf("ShardCtrler: %v. removeNotifyChannel(). commandIndex: %v. Not OK.", sc.me, commandIndex)
		return
	}
	close(sc.notifyChannel[commandIndex])
	delete(sc.notifyChannel, commandIndex)
	DPrintf("ShardCtrler: %v. removeNotifyChannel(). commandIndex: %v. Delete.", sc.me, commandIndex)
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardsc tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	labgob.Register(Command{})
	sc.configNum = 0
	sc.notifyChannel = make(map[int]chan NotifyMsg)
	sc.lastRequestNum = make(map[int64]int)
	sc.rpcQueryCache = make(map[int64]NotifyMsg)
	DPrintf("ShardCtrler: %v initialization. config: %v.", sc.me, sc.configs[0])
	go sc.apply()
	return sc
}
