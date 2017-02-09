package raftkv

import (
	"bytes"
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Meth   string
	Key    string
	Value  string
	Client int64
	Seq    int
}

type RaftKV struct {
	mu      sync.Mutex
	rwmu    sync.RWMutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	db      map[string]string
	reflect map[int]chan Op
	chk     map[int64]int
}

func (kv *RaftKV) StaleTask(client int64, seq int) bool {
	if lastseq, ok := kv.chk[client]; ok {
		return lastseq >= seq
	}
	return false
}

func (kv *RaftKV) DuplicateLog(entry Op) bool {
	idx, _, isLeader := kv.rf.Start(entry)
	if !isLeader {
		return false
	}

	kv.mu.Lock()
	//double check
	ch, ok := kv.reflect[idx]
	if !ok {
		ch = make(chan Op, 1)
		kv.reflect[idx] = ch
	}
	kv.mu.Unlock()

	//wait to commit
	select {
	case op := <-ch:
		return op == entry
	case <-time.After(600 * time.Millisecond):
		return false
	}
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	command := Op{Meth: "Get", Key: args.Key}
	//duplicate command to raft log
	reply.Err = OK
	if ok := kv.DuplicateLog(command); !ok {
		reply.WrongLeader = true
	} else {
		reply.WrongLeader = false
		//get state
		kv.rwmu.RLock()
		if v, ok := kv.db[args.Key]; !ok {
			reply.Err = ErrNoKey
		} else {
			reply.Value = v
		}
		kv.rwmu.RUnlock()
	}
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	command := Op{Meth: args.Op, Key: args.Key, Value: args.Value, Client: args.Client, Seq: args.Seq}
	if ok := kv.DuplicateLog(command); !ok {
		reply.WrongLeader = true
	} else {
		reply.WrongLeader = false
		reply.Err = OK
		//move apply change state to commit apply
	}
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// Your initialization code here.
	kv.db = make(map[string]string)
	kv.reflect = make(map[int]chan Op)
	kv.applyCh = make(chan raft.ApplyMsg, 64)
	kv.chk = make(map[int64]int)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.loop(maxraftstate, persister)
	return kv
}

//read commite channel, and apple state change to machine
func (kv *RaftKV) loop(maxraftstate int, persister *raft.Persister) {
	for entry := range kv.applyCh {
		if entry.UseSnapshot {
			kv.readSnatshot(entry.Snapshot)
		} else {
			command := entry.Command.(Op)
			kv.rwmu.Lock()
			//apply change task to state machine
			if command.Meth != "Get" && !kv.StaleTask(command.Client, command.Seq) {
				switch command.Meth {
				case "Put":
					kv.db[command.Key] = command.Value
				case "Append":
					kv.db[command.Key] += command.Value
				}
				kv.chk[command.Client] = command.Seq
			}
			kv.rwmu.Unlock()

			kv.mu.Lock()
			//double check
			if _, ok := kv.reflect[entry.Index]; !ok {
				kv.reflect[entry.Index] = make(chan Op, 1)
			}
			kv.mu.Unlock()
			kv.reflect[entry.Index] <- command

			//check snapshot
			if maxraftstate != -1 && persister.RaftStateSize() > maxraftstate {
				go func() {
					if !kv.rf.Snapshoting() {
						kv.rf.SetSnapshot(true)
						//snapshot
						w := new(bytes.Buffer)
						e := gob.NewEncoder(w)
						//state
						kv.rwmu.RLock()
						e.Encode(kv.db)
						e.Encode(kv.chk)
						kv.rwmu.RUnlock()
						data := w.Bytes()
						kv.rf.TakeSnatshot(data, entry.Index)
						kv.rf.SetSnapshot(false)
					}
				}()
			}
		}
	}
}

//snatshot
func (kv *RaftKV) readSnatshot(data []byte) {
	var lastIncludeIndex, lastIncludeTerm int

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&lastIncludeIndex)
	d.Decode(&lastIncludeTerm)
	kv.rwmu.Lock()
	d.Decode(&kv.db)
	d.Decode(&kv.chk)
	kv.rwmu.Unlock()
}
