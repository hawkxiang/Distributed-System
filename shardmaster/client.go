package shardmaster

//
// Shardmaster clerk.
//

import "labrpc"
import "time"
import "crypto/rand"
import "math/big"
import "sync"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	lastLeader	int
	identity	int64
	seq			int
	mu 			sync.Mutex
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
	ck.lastLeader = 0
	ck.identity = nrand()
	ck.seq = 0
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	args.Num = num
	args.Client = ck.identity
	ck.mu.Lock()
	args.Seq = ck.seq
	ck.seq++
	ck.mu.Unlock()
	for {
		// try each known server.
		for i, n := ck.lastLeader, len(ck.servers); ; i = (i + 1) % n {
			var reply QueryReply
			ok := ck.servers[i].Call("ShardMaster.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.lastLeader = i
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	args.Servers = servers
	args.Client = ck.identity
	ck.mu.Lock()
	args.Seq = ck.seq
	ck.seq++
	ck.mu.Unlock()
	for {
		// try each known server.
		/*for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardMaster.Join", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}*/
		for i, n := ck.lastLeader, len(ck.servers); ; i = (i + 1) % n {
			var reply JoinReply
			ok := ck.servers[i].Call("ShardMaster.Join", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.lastLeader = i
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids
	args.Client = ck.identity
	ck.mu.Lock()
	args.Seq = ck.seq
	ck.seq++
	ck.mu.Unlock()
	for {
		// try each known server.
		for i, n := ck.lastLeader, len(ck.servers); ; i = (i + 1) % n {
			var reply LeaveReply
			ok := ck.servers[i].Call("ShardMaster.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.lastLeader = i
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid
	args.Client = ck.identity
	ck.mu.Lock()
	args.Seq = ck.seq
	ck.seq++
	ck.mu.Unlock()

	for {
		// try each known server.
		for i, n := ck.lastLeader, len(ck.servers); ; i = (i + 1) % n {
			var reply MoveReply
			ok := ck.servers[i].Call("ShardMaster.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.lastLeader = i
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
