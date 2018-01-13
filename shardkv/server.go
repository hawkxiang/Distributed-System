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
	Meth string
	GetArgc  GetArgs
	PutArgc PutAppendArgs
	ReCfg ReconfigureArgs
}

type Result struct {
	args  interface{}
	reply interface{}
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
	muMsg	sync.Mutex
	sm      *shardmaster.Clerk
	config  shardmaster.Config
	db      [shardmaster.NShards]map[string]string
	taskSeq map[int64]int
	messages map[int]chan Result
}

func (kv *ShardKV) StaleTask(client int64, seq int) bool {
	if lastseq, ok := kv.taskSeq[client]; ok && lastseq >= seq {
		return true
	}
	kv.taskSeq[client] = seq
	return false
}

func (kv *ShardKV) CheckValidKey(key string) bool {
	shardID := key2shard(key)
	if kv.gid != kv.config.Shards[shardID] {
		return false
	}
	return true
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	idx, _, isLeader := kv.rf.Start(Op{Meth: "Get", GetArgc:*args})
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	kv.muMsg.Lock()
	ch, ok := kv.messages[idx]
	if !ok {
		ch = make(chan Result, 1)
		kv.messages[idx] = ch
	}
	kv.muMsg.Unlock()

	select {
	case msg := <- ch:
		if ret, ok := msg.args.(GetArgs); !ok {
			reply.WrongLeader = true
		} else {
			if args.Client != ret.Client || args.Seq != ret.Seq {
				reply.WrongLeader = true
			} else {
				*reply = msg.reply.(GetReply)
				reply.WrongLeader = false
			}
		}
	case <- time.After(400 * time.Millisecond):
		reply.WrongLeader = true
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	idx, _, isLeader := kv.rf.Start(Op{Meth: "PutAppend", PutArgc:*args})
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	kv.muMsg.Lock()
	ch, ok := kv.messages[idx]
	if !ok {
		ch = make(chan Result, 1)
		kv.messages[idx] = ch
	}
	kv.muMsg.Unlock()

	select {
	case msg := <- ch:
		if ret, ok := msg.args.(PutAppendArgs); !ok {
			reply.WrongLeader = true
		} else {
			if args.Client != ret.Client || args.Seq != ret.Seq {
				reply.WrongLeader = true
			} else {
				reply.Err = msg.reply.(PutAppendReply).Err
				reply.WrongLeader = false
			}
		}
	case <- time.After(400 * time.Millisecond):
		reply.WrongLeader = true
	}
}

func (kv *ShardKV) SyncConfigure(args ReconfigureArgs) bool {
	for i := 0; i < 3; i++ {
		idx, _, isLeader := kv.rf.Start(Op{Meth:"Reconfigure", ReCfg:args})
		if !isLeader {
			return false
		}

		kv.muMsg.Lock()
		ch, ok := kv.messages[idx]
		if !ok {
			ch = make(chan Result, 1)
			kv.messages[idx] = ch
		}
		kv.muMsg.Unlock()

		select {
		case msg := <- ch:
			if ret, ok := msg.args.(ReconfigureArgs); !ok {
				return ret.Cfg.Num == args.Cfg.Num
			}
		case <- time.After(150 * time.Millisecond):
			continue
		}
	}
	return false
}

func (kv *ShardKV) GetShard(args *GetShardArgs, reply *GetShardReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.config.Num < args.CfgNum {
		reply.Err = ErrNotReady
		return
	}

	reply.Err = OK
	reply.TaskSeq = make(map[int64]int)
	for i := 0; i < shardmaster.NShards; i++ {
		reply.Content[i] = make(map[string]string)
	}

	for _, shardIdx := range args.Shards {
		for k, v := range kv.db[shardIdx] {
			reply.Content[shardIdx][k] = v
		}
	}

	for cli := range kv.taskSeq {
		reply.TaskSeq[cli] = kv.taskSeq[cli]
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
	kv.applyCh = make(chan raft.ApplyMsg, 32)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.messages = make(map[int]chan Result)
	kv.taskSeq = make(map[int64]int)
	kv.sm = shardmaster.MakeClerk(masters)
	//kv.config = kv.sm.Query(-1)

	for i := 0; i < shardmaster.NShards; i++ {
		kv.db[i] = make(map[string]string)
	}

	//go kv.checkLoop(masters)
	go kv.loop(maxraftstate, persister)
	go kv.pollConfig(servers)
	return kv
}

func (kv *ShardKV) loop(maxraftstate int, persister *raft.Persister) {
	for entry := range kv.applyCh {
		if entry.UseSnapshot {
			kv.readSnatshot(entry.Snapshot)
		} else {
			request := entry.Command.(Op)
			var result Result
			switch request.Meth {
			case "Get":
				result.args = request.GetArgc
				result.reply = kv.ApplyGet(request.GetArgc)
			case "PutAppend":
				result.args = request.PutArgc
				result.reply = kv.ApplyPutAppend(request.PutArgc)
			case "Reconfigure":
				result.args = request.ReCfg
				result.reply = kv.ApplyReconfigure(request.ReCfg)
			}

			kv.sendMessage(entry.Index, result)
			kv.checkSnapshot(entry.Index, maxraftstate, persister)
		}
	}
}

func (kv *ShardKV) sendMessage(index int, result Result) {
	kv.muMsg.Lock()
	defer kv.muMsg.Unlock()
	
	if _, ok := kv.messages[index]; !ok {
		kv.messages[index] = make(chan Result, 1)
	} else {
		select {
		case <-kv.messages[index]:
		default:
		}
	}
	kv.messages[index] <- result
}

func (kv *ShardKV) checkSnapshot(idx int, maxraftstate int, persister *raft.Persister) {
	if maxraftstate != -1 && persister.RaftStateSize() > maxraftstate {
		recover := maxraftstate
		maxraftstate = -1
		//snapshot
		w := new(bytes.Buffer)
		e := gob.NewEncoder(w)
		//state
		e.Encode(kv.db)
		e.Encode(kv.taskSeq)
		e.Encode(kv.config)
		data := w.Bytes()
		go func(snapstate []byte, preindex int, maxraftstate *int, recover int) {
			kv.rf.TakeSnatshot(snapstate, preindex)
			*maxraftstate = recover
		}(data, idx, &maxraftstate, recover)
	}
}

func (kv *ShardKV) readSnatshot(data []byte) {
	var lastIncludeIndex, lastIncludeTerm int

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&lastIncludeIndex)
	d.Decode(&lastIncludeTerm)
	kv.mu.Lock()
	d.Decode(&kv.db)
	d.Decode(&kv.taskSeq)
	d.Decode(&kv.config)
	kv.mu.Unlock()
}

func (kv *ShardKV) ApplyGet(args GetArgs) GetReply {
	var reply GetReply
	if !kv.CheckValidKey(args.Key) {
		reply.Err = ErrWrongGroup
		return reply
	}
	kv.mu.Lock()
	if value, ok := kv.db[key2shard(args.Key)][args.Key]; ok {
		reply.Err = OK
		reply.Value = value
	} else {
		reply.Err = ErrNoKey
	}
	kv.mu.Unlock()
	return reply
}

func (kv *ShardKV) ApplyPutAppend(args PutAppendArgs) PutAppendReply {
	var reply PutAppendReply
	if !kv.CheckValidKey(args.Key) {
		reply.Err = ErrWrongGroup
		return reply
	}

	kv.mu.Lock()
	if !kv.StaleTask(args.Client, args.Seq) {
		if args.Op == "Put" {
			kv.db[key2shard(args.Key)][args.Key] = args.Value
		} else {
			kv.db[key2shard(args.Key)][args.Key] += args.Value
		}
	}
	reply.Err = OK
	kv.mu.Unlock()
	return reply
}

func (kv *ShardKV) ApplyReconfigure(args ReconfigureArgs) ReconfigureReply {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	var reply ReconfigureReply
	if args.Cfg.Num > kv.config.Num {
		for shardIdx, data := range args.Content {
			for k, v := range data {
				kv.db[shardIdx][k] = v
			}
		}

		for cli := range args.TaskSeq {
			if seq, exist := kv.taskSeq[cli]; !exist || seq < args.TaskSeq[cli] {
				kv.taskSeq[cli] = args.TaskSeq[cli]
			}
		}
		kv.config = args.Cfg
		reply.Err = OK
	}
	return reply
}

func (kv *ShardKV) pollConfig(masters []*labrpc.ClientEnd) {
	var timeoutChan <-chan time.Time
	for true {
		if _, isLeader := kv.rf.GetState(); isLeader {
			newCfg := kv.sm.Query(-1)
			for i := kv.config.Num + 1; i <= newCfg.Num; i++ {
				if !kv.Reconfigure(kv.sm.Query(i)) {
					break
				}
			}
		}
		timeoutChan = time.After(100 * time.Millisecond)
		<-timeoutChan
	}
}

func (kv *ShardKV) pullShard(gid int, args *GetShardArgs, reply *GetShardReply) bool {
	for _, server := range kv.config.Groups[gid] {
		srv := kv.make_end(server)
		ok := srv.Call("ShardKV.GetShard", args, reply)
		if ok {
			if reply.Err == OK {
				return true
			} else if reply.Err == ErrNotReady {
				return false
			}
		}
	}
	return false
}

func (kv *ShardKV) Reconfigure(newCfg shardmaster.Config) bool {
	ret := ReconfigureArgs{Cfg:newCfg}
	ret.TaskSeq = make(map[int64]int)
	for i := 0; i < shardmaster.NShards; i++ {
		ret.Content[i] = make(map[string]string)
	}
	isOK := true
	
	mergeShards := make(map[int][]int)
	for i := 0; i < shardmaster.NShards; i++ {
		if newCfg.Shards[i] == kv.gid && kv.config.Shards[i] != kv.gid {
			gid := kv.config.Shards[i]
			if gid != 0 {
				if _, ok := mergeShards[gid]; !ok {
					mergeShards[gid] = []int{i}
				} else {
					mergeShards[gid] = append(mergeShards[gid], i)
				}
			}
		}
	}
	
	var retMu sync.Mutex
	var wait sync.WaitGroup
	for gid, value := range mergeShards {
		wait.Add(1)
		go func(gid int, value []int) {
			defer wait.Done()
			var reply GetShardReply

			if kv.pullShard(gid, &GetShardArgs{CfgNum: newCfg.Num, Shards:value}, &reply) {
				retMu.Lock()
				for shardIdx, data := range reply.Content {
					for k, v := range data {
						ret.Content[shardIdx][k] = v
					}
				}
				
				for cli := range reply.TaskSeq {
					if seq, exist := ret.TaskSeq[cli]; !exist || seq < reply.TaskSeq[cli] {
						ret.TaskSeq[cli] = reply.TaskSeq[cli]
					}
				}
				retMu.Unlock()
			} else {
				isOK = false
			}
		} (gid, value)
	}
	wait.Wait()
	return isOK && kv.SyncConfigure(ret)
}
