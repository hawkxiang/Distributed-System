package shardkv


// import "shardmaster"
import "labrpc"
import "raft"
import "sync"
import "encoding/gob"
import "time"
import "bytes"
import (
	"shardmaster"
)



type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Meth	string
	Client 	int64
	Seq 	int
	Key 	string
	Value 	string
	Shard 	int
	Config shardmaster.Config
	Reconfig	GetShardReply
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	sm      *shardmaster.Clerk
	config  shardmaster.Config
	db 		map[string]string
	taskSeq map[int64]int
	shards 	map[int]string
	dbmu	sync.Mutex
	reflect map[int]chan Err
	reflect_content map[int] map[string]string
	reflect_task map[int] map[int64]int

}

func (kv *ShardKV) StaleTask(client int64, seq int) bool {
	if lastseq, ok := kv.taskSeq[client]; ok {
		return lastseq >= seq
	}
	return false
}

func (kv *ShardKV) DuplicateLog(entry *Op) (Err, int) {
	idx, _, isLeader := kv.rf.Start(*entry)
	if !isLeader {
		return "", -1
	}

	kv.mu.Lock()
	ch, ok := kv.reflect[idx]
	if !ok {
		ch = make(chan Err, 1)
		kv.reflect[idx] = ch
	}
	kv.mu.Unlock()

	select {
	case ret := <-ch:
			return ret, idx
	case <-time.After(600 * time.Millisecond):
		return TimeOut, 0
	}
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	command := Op{Meth:"Get", Key:args.Key, Shard:key2shard(args.Key)}

	err, idx := kv.DuplicateLog(&command)
	reply.WrongLeader = false
	reply.Err = err
	if -1 == idx {
		reply.WrongLeader = true
	} else if OK == err {
		kv.dbmu.Lock()
		if v, ok := kv.db[args.Key]; !ok {
			reply.Err = ErrNoKey
		} else {
			reply.Value = v
		}
		kv.dbmu.Unlock()
	}
}

func (kv *ShardKV) GetShard(args *GetShardArgs, reply *GetShardReply) {

	shard := args.Shard
	reply.Content = make(map[string]string)
	reply.TaskSeq = make(map[int64]int)
	kv.dbmu.Lock()
	defer kv.dbmu.Unlock()
	if kv.config.Num < args.CfgNum {
		reply.Err = ErrNotReady
		return
	}
	for key := range kv.db {
		if key2shard(key) == shard {
			reply.Content[key] = kv.db[key]
			//删除本地状态，不在这里实现，可能迁移消息丢失，确保后删除冗余信息
			//delete(kv.db, key)
		}
	}

	for key := range kv.taskSeq {
		reply.TaskSeq[key] = kv.taskSeq[key]
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	command := Op{Meth:args.Op, Key:args.Key, Value:args.Value, Client:args.Client, Seq:args.Seq, Shard:key2shard(args.Key)}
	err, idx := kv.DuplicateLog(&command)
	reply.WrongLeader = false
	reply.Err = err

	if -1 == idx {
		reply.WrongLeader = true
	}
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
// the k/v server should store snapshots with
// persister.SaveSnapshot(), and Raft should save its state (including
// log) with persister.SaveRaftState().
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.

	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.db = make(map[string]string)
	kv.reflect = make(map[int]chan Err)
	kv.taskSeq = make(map[int64]int)
	kv.sm = shardmaster.MakeClerk(masters)
	kv.reflect_content = make(map[int]map[string]string)
	kv.reflect_task = make(map[int] map[int64]int)
	kv.config = kv.sm.Query(-1)

	//go kv.checkLoop(masters)
	go kv.loop(maxraftstate, persister)
	go kv.checkLoop(servers)
	return kv
}

func (kv *ShardKV) loop(maxraftstate int, persister *raft.Persister)  {
	for entry := range kv.applyCh {
		if entry.UseSnapshot {
			kv.readSnatshot(entry.Snapshot)
		} else {
			command := entry.Command.(Op)
			var ret Err = OK
			var shard int
			shard = command.Shard
			gid := kv.config.Shards[shard]
			if kv.gid != gid {
				ret = ErrWrongGroup
			}
			kv.dbmu.Lock()
			switch command.Meth {
			case "Get":
				break
			case "Reconfig":
				for key := range command.Reconfig.Content {
					kv.db[key] = command.Reconfig.Content[key]
				}

				for cli := range command.Reconfig.TaskSeq {
					seq, exist := kv.taskSeq[cli]
					if !exist || seq < command.Reconfig.TaskSeq[cli] {
						kv.taskSeq[cli] = seq
					}
				}
				kv.config = command.Config
			default :
				if OK != ret || kv.StaleTask(command.Client, command.Seq) {
					break
				}

				if command.Meth == "Put" {
					kv.db[command.Key] = command.Value
				}else {
					kv.db[command.Key] += command.Value
				}
				kv.taskSeq[command.Client] = command.Seq
			}
			kv.dbmu.Unlock()

			kv.mu.Lock()
			ch, ok := kv.reflect[entry.Index]
			if ok {
				select {
				case <-kv.reflect[entry.Index]:
				default:
				}
				ch <- ret
			}
			kv.mu.Unlock()

			if maxraftstate != -1 && persister.RaftStateSize() > maxraftstate {
				recover := maxraftstate
				maxraftstate = -1
				//snapshot
				w := new(bytes.Buffer)
				e := gob.NewEncoder(w)
				//state
				e.Encode(kv.db)
				e.Encode(kv.taskSeq)
				data := w.Bytes()
				go func(snapstate []byte, preindex int, maxraftstate *int, recover int){
					kv.rf.TakeSnatshot(snapstate, preindex)
					*maxraftstate = recover
				}(data, entry.Index, &maxraftstate, recover)
			}
		}
	}
}

func (kv *ShardKV) readSnatshot(data []byte) {
	var lastIncludeIndex, lastIncludeTerm int

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&lastIncludeIndex)
	d.Decode(&lastIncludeTerm)
	kv.dbmu.Lock()
	d.Decode(&kv.db)
	d.Decode(&kv.taskSeq)
	kv.dbmu.Unlock()
}

func (lhs *GetShardReply) Merge(rhs *GetShardReply) {

	for key := range rhs.Content {
		lhs.Content[key] = rhs.Content[key]
	}

	for cli := range rhs.TaskSeq {
		seq, exist := lhs.TaskSeq[cli]
		if !exist || seq < rhs.TaskSeq[cli] {
			lhs.TaskSeq[cli] = seq
		}
	}
}

func (kv *ShardKV) Reconfigure(newCfg shardmaster.Config) bool {
	oldcfg := &kv.config
	allChange := GetShardReply{false,OK, map[string]string{}, map[int64]int{}};
	for i := 0; i < shardmaster.NShards; i++ {
		gid := oldcfg.Shards[i]
		//新配置文件中shard已经迁移到本组，但是旧配置文件中shard不属于该组，需要迁移
		if newCfg.Shards[i] == kv.gid && gid != kv.gid {
			args := &GetShardArgs{i, newCfg.Num}
			var reply GetShardReply
			//是否需要获得所有组内成员的状态？
			for _, srv := range oldcfg.Groups[gid] {
				ok := kv.sendGetShard(srv, args, &reply)
				if ok && reply.Err == OK {
					break
				} else if reply.Err == ErrNotReady{
					return false
				}
			}
			allChange.Merge(&reply)
		}
	}
	//apply change to state machine
	command := Op{Meth:"Reconfig", Config: newCfg, Reconfig: allChange}
	kv.DuplicateLog(&command)
	return true
}

func (kv *ShardKV) sendGetShard(server string, args *GetShardArgs, reply *GetShardReply) bool {
	ok := kv.make_end(server).Call("ShardKV.GetShard", args, reply)
	return ok
}

func (kv *ShardKV) checkLoop(masters []*labrpc.ClientEnd) {
	var timeoutChan <-chan time.Time
	for {
		if _, isLeader := kv.rf.GetState(); isLeader {
			newCfg := kv.sm.Query(-1)
			for i := kv.config.Num + 1; i <= newCfg.Num; i++ {
				cfg := kv.sm.Query(i)
				if !kv.Reconfigure(cfg) {
					break
				}
			}
		}
		timeoutChan = time.After(90 * time.Millisecond)
		<-timeoutChan
	}
}