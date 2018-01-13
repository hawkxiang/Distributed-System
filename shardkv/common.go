package shardkv
import "shardmaster"
//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK            = "OK"
	ErrNoKey      = "ErrNoKey"
	ErrStaleKey      = "ErrStaleKey"
	ErrWrongGroup = "ErrWrongGroup"
	ErrNotReady	  = "ErrNotReady"
	ErrBadConfig	= "ErrBadConfig"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Client	int64
	Seq 	int
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Client	int64
	Seq 	int
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}

type GetShardArgs struct {
	Shards	[]int
	CfgNum	int
}

type GetShardReply struct {
	WrongLeader bool
	Err 	Err
	Content	[shardmaster.NShards]map[string]string
	TaskSeq map[int64]int
}

type ReconfigureArgs struct {
	Cfg	shardmaster.Config
	Content [shardmaster.NShards]map[string]string
	TaskSeq map[int64]int
}

type ReconfigureReply struct {
	Err	Err
}
