package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"
import "sync"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	lastLeader int
	identity   int64
	seq        int
	mu         sync.Mutex
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
	ck.lastLeader = 0
	ck.identity = nrand()
	ck.seq = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	/*args := GetArgs{Key: key, Client: ck.identity}
	ck.mu.Lock()
	args.Seq = ck.seq
	ck.seq++
	ck.mu.Unlock()*/
	args := GetArgs{Key: key}
	for i, n := ck.lastLeader, len(ck.servers); ; i = (i + 1) % n {
		var reply GetReply
		ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
		if ok && !reply.WrongLeader {
			if reply.Err == ErrNoKey {
				return ""
			} else {
				return reply.Value
			}
			ck.lastLeader = i
		}
	}

}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{Key: key, Value: value, Op: op, Client: ck.identity}
	ck.mu.Lock()
	args.Seq = ck.seq
	ck.seq++
	ck.mu.Unlock()
	for i, n := ck.lastLeader, len(ck.servers); ; i = (i + 1) % n {
		var reply PutAppendReply
		ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
		if ok && !reply.WrongLeader {
			ck.lastLeader = i
			return
		}
	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
