package kvraft

import (
	"crypto/rand"
	"math/big"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	id           int64
	sequenceNum  int
	serverLeader int // cluster leader
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
	// You'll have to add code here.
	ck.id = nrand()
	ck.sequenceNum = 0
	ck.serverLeader = 0
	DPrintf("Clinet %v.", ck.id)
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	ck.sequenceNum++
	value := ""
	for {
		args := GetArgs{
			Key:         key,
			Op:          OpGet,
			ClientId:    ck.id,
			SequenceNum: ck.sequenceNum,
		}
		reply := GetReply{}
		DPrintf("Clinet %v. Get(). key: %v. sequenceNum: %v. RPC to %v ", ck.id, key, ck.sequenceNum, ck.serverLeader)
		ok := ck.servers[ck.serverLeader].Call("KVServer.Get", &args, &reply)

		if !ok {
			DPrintf("Clinet %v. Get(). key: %v. sequenceNum: %v. RPC to %v fail.", ck.id, key, ck.sequenceNum, ck.serverLeader)
			ck.serverLeader = (ck.serverLeader + 1) % len(ck.servers)
			continue
		}

		if reply.Err == OK {
			DPrintf("Clinet %v. Get(). key: %v. sequenceNum: %v. RPC to %v OK. reply: %v.", ck.id, key, ck.sequenceNum, ck.serverLeader, reply)
			value = reply.Value
			break
		}

		if reply.Err == ErrNoKey {
			break
		}

		if reply.Err == ErrTimeout {
			ck.serverLeader = (ck.serverLeader + 1) % len(ck.servers)
			continue
		}

		if reply.Err == ErrWrongLeader {
			ck.serverLeader = (ck.serverLeader + 1) % len(ck.servers)
			continue
		}
	}

	return value
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.sequenceNum++
	for {
		args := PutAppendArgs{
			Key:         key,
			Value:       value,
			Op:          op,
			ClientId:    ck.id,
			SequenceNum: ck.sequenceNum,
		}
		reply := PutAppendReply{}
		DPrintf("Clinet %v. PutAppend(). key: %v. value: %v. op: %v. sequenceNum: %v. RPC to %v.", ck.id, key, value, op, ck.sequenceNum, ck.serverLeader)
		ok := ck.servers[ck.serverLeader].Call("KVServer.PutAppend", &args, &reply)

		if !ok {
			DPrintf("Clinet %v. PutAppend(). key: %v. value: %v. op: %v. sequenceNum: %v. RPC to %v fail.", ck.id, key, value, op, ck.sequenceNum, ck.serverLeader)
			ck.serverLeader = (ck.serverLeader + 1) % len(ck.servers)
			continue
		}

		if reply.Err == OK {
			DPrintf("Clinet %v. PutAppend(). key: %v. value: %v. op: %v. sequenceNum: %v. RPC to %v OK. reply: %v.", ck.id, key, value, op, ck.sequenceNum, ck.serverLeader, reply)
			break
		}

		if reply.Err == ErrNoKey {
			break
		}

		if reply.Err == ErrTimeout {
			ck.serverLeader = (ck.serverLeader + 1) % len(ck.servers)
			continue
		}

		if reply.Err == ErrWrongLeader {
			ck.serverLeader = (ck.serverLeader + 1) % len(ck.servers)
			continue
		}
	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
