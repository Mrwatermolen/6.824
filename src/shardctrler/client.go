package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	id           int64
	sequenceNum  int
	serverLeader int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.id = nrand()
	ck.serverLeader = 0
	ck.sequenceNum = 0
	DPrintf("Clinet %v. ck.servers: %v.", ck.id, ck.servers)
	return ck
}

func (ck *Clerk) Query(num int) Config {
	/* args := &QueryArgs{}
	// Your code here.
	args.Num = num
	ck.sequenceNum++
	args.ClientId = ck.id
	args.SequenceNum = ck.sequenceNum
	DPrintf("Clinet %v. Query", ck.id)
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardCtrler.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	} */

	ck.sequenceNum++
	DPrintf("Clinet %v. Query", ck.id)
	for {
		args := QueryArgs{
			Num:         num,
			ClientId:    ck.id,
			SequenceNum: ck.sequenceNum,
		}
		reply := QueryReply{}

		ok := ck.servers[ck.serverLeader].Call("ShardCtrler.Query", &args, &reply)

		if !ok {
			ck.serverLeader = (ck.serverLeader + 1) % len(ck.servers)
			continue
		}

		if reply.Err == OK {
			return reply.Config
		}

		if reply.Err == ErrTimeout {
			ck.serverLeader = (ck.serverLeader + 1) % len(ck.servers)
			continue
		}

		if reply.Err == ErrWrongLeader {
			ck.serverLeader = (ck.serverLeader + 1) % len(ck.servers)
			continue
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	/* 	args := &JoinArgs{}
	   	// Your code here.
	   	args.Servers = servers
	   	ck.sequenceNum++
	   	args.ClientId = ck.id
	   	args.SequenceNum = ck.sequenceNum
	   	DPrintf("Clinet %v. Join", ck.id)
	   	for {
	   		// try each known server.
	   		for _, srv := range ck.servers {
	   			var reply JoinReply
	   			ok := srv.Call("ShardCtrler.Join", args, &reply)
	   			if ok && reply.WrongLeader == false {
	   				return
	   			}
	   		}
	   		time.Sleep(100 * time.Millisecond)
	   	} */

	ck.sequenceNum++
	DPrintf("Clinet %v. Join", ck.id)
	for {
		args := JoinArgs{
			Servers:     servers,
			ClientId:    ck.id,
			SequenceNum: ck.sequenceNum,
		}
		reply := JoinReply{}

		ok := ck.servers[ck.serverLeader].Call("ShardCtrler.Join", &args, &reply)

		if !ok {
			ck.serverLeader = (ck.serverLeader + 1) % len(ck.servers)
			continue
		}

		if reply.Err == OK {
			return
		}

		if reply.Err == ErrTimeout {
			ck.serverLeader = (ck.serverLeader + 1) % len(ck.servers)
			continue
		}

		if reply.Err == ErrWrongLeader {
			ck.serverLeader = (ck.serverLeader + 1) % len(ck.servers)
			continue
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	/* args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids
	ck.sequenceNum++
	args.ClientId = ck.id
	args.SequenceNum = ck.sequenceNum
	DPrintf("Clinet %v. Leave", ck.id)
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardCtrler.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	} */

	ck.sequenceNum++
	DPrintf("Clinet %v. Join", ck.id)
	for {
		args := LeaveArgs{
			GIDs:        gids,
			ClientId:    ck.id,
			SequenceNum: ck.sequenceNum,
		}
		reply := LeaveReply{}

		ok := ck.servers[ck.serverLeader].Call("ShardCtrler.Leave", &args, &reply)

		if !ok {
			ck.serverLeader = (ck.serverLeader + 1) % len(ck.servers)
			continue
		}

		if reply.Err == OK {
			return
		}

		if reply.Err == ErrTimeout {
			ck.serverLeader = (ck.serverLeader + 1) % len(ck.servers)
			continue
		}

		if reply.Err == ErrWrongLeader {
			ck.serverLeader = (ck.serverLeader + 1) % len(ck.servers)
			continue
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	/* args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid
	DPrintf("Clinet %v. Move", ck.id)
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardCtrler.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	} */

	ck.sequenceNum++
	DPrintf("Clinet %v. Join", ck.id)
	for {
		args := MoveArgs{
			Shard:       shard,
			GID:         gid,
			ClientId:    ck.id,
			SequenceNum: ck.sequenceNum,
		}
		reply := MoveReply{}

		ok := ck.servers[ck.serverLeader].Call("ShardCtrler.Move", &args, &reply)

		if !ok {
			ck.serverLeader = (ck.serverLeader + 1) % len(ck.servers)
			continue
		}

		if reply.Err == OK {
			return
		}

		if reply.Err == ErrTimeout {
			ck.serverLeader = (ck.serverLeader + 1) % len(ck.servers)
			continue
		}

		if reply.Err == ErrWrongLeader {
			ck.serverLeader = (ck.serverLeader + 1) % len(ck.servers)
			continue
		}
		time.Sleep(100 * time.Millisecond)
	}
}
