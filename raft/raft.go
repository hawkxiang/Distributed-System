package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "fmt"
import "sync"
import "labrpc"
import "bytes"
import "encoding/gob"
import "time"
import "errors"

const (
	Stopped = iota
	Initialized
	Follower
	Candidate
	Leader
	Snapshotting
)
const EmptyVote = -10

const (
	HeartbeatInterval = 38 * time.Millisecond
	ElectionTimeout   = 150 * time.Millisecond
)

var NotLeaderError = errors.New("raft.Server: Not current leader")
var StopError = errors.New("raft: Has been stopped")

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type Entry struct {
	Index   int
	Term    int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	logmu     sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]
	state     int

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	//persistent state
	CurrentTerm int
	VotedFor    int
	Log         []Entry

	//volatile state
	commitIndex int
	lastApplied int

	//volatile state on leaders
	nextIndex  []int
	matchIndex []int

	//sync chan
	stopped     chan bool
	blockqueue  chan *message
	applyNotice chan bool
	applyCh     chan ApplyMsg

	//wait works goroutine end
	wg sync.WaitGroup
}

//give message to event loop, event loop use handle to do.
type message struct {
	args  interface{}
	reply interface{}
	err   chan error
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here.
	isleader = false
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.CurrentTerm
	if rf.state == Leader {
		isleader = true
	}
	return term, isleader
}

func (rf *Raft) ChangeState(newstate int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.state = newstate
	if newstate == Leader {
		num := len(rf.peers)
		rf.nextIndex = make([]int, num)
		rf.matchIndex = make([]int, num)
		
		for i, lastidx:= 0, rf.LastIndex(); i < num; i++ {
			//TODO:
			rf.matchIndex[i] = 0
			rf.nextIndex[i] = lastidx + 1
		}
	}

}

func (rf *Raft) State() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state
}


func (rf *Raft) updateCurrentTerm(newterm int, newleader int) {
	if rf.state != Follower {
		rf.ChangeState(Follower)
	}

	rf.mu.Lock()
	rf.CurrentTerm = newterm
	rf.mu.Unlock()
	rf.VotedFor = newleader
	rf.persist()
}

func (rf *Raft) Running() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return (rf.state != Initialized && rf.state != Stopped)
}

func (rf *Raft) Vote(peer int) {
	rf.VotedFor = peer
	rf.persist()
}

func (rf *Raft) QuorumSize() int {
	return (len(rf.peers) / 2) + 1
}

func (rf *Raft) BaseIndex() int {
	return rf.Log[0].Index
}

func (rf *Raft) LastIndex() int {
	return rf.Log[len(rf.Log)-1].Index
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}
//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.CurrentTerm)
	d.Decode(&rf.VotedFor)
	d.Decode(&rf.Log)
}

func (rf *Raft) readMeta(data []byte) (int, int) {
	if len(data) == 0 {
		return 0, 0
	}

	var lastIncludeIndex, lastIncludeTerm int
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&lastIncludeIndex)
	d.Decode(&lastIncludeTerm)

	return lastIncludeIndex, lastIncludeTerm
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//code to send a AppendEntries RPC to a server.
// for heartbeats
func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args SnatshotArgs, reply *SnatshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false
	if rf.State() == Leader {
		reply := new(AgreementRely)
		//this handler need not block, when the event loop receive the task, first return 'index' and 'term', then start agreement with other peers.
		ok := rf.deliver(&AgreementArgs{command}, reply)
		if ok == nil {
			isLeader = true
			index = reply.Index
			term = reply.Term
		}
	}
	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	if rf.State() == Stopped {
		return
	}

	close(rf.stopped)
	//to pass all test, need to not wait all gotine over.
	//rf.wg.Wait()

	rf.ChangeState(Stopped)
}

func makeServer(peers []*labrpc.ClientEnd, me int, persister *Persister) *Raft {
	rf := &Raft{
		peers:     peers,
		persister: persister,
		me:        me,
		state:     Stopped,

		commitIndex: 0,
		lastApplied: 0,
		nextIndex:   nil,
		matchIndex:  nil,

		blockqueue:  make(chan *message, 64),
		applyNotice: make(chan bool, 32),
	}
	return rf
}

func (rf *Raft) Init() error {
	if rf.Running() {
		return fmt.Errorf("raft.Server: Server already running[%v]", rf.state)
	}

	if rf.state == Initialized {
		rf.state = Initialized
		return nil
	}

	// initialize from state persisted before a crash
	rf.readPersist(rf.persister.ReadRaftState())
	//TODO:read snatshot to change commitIndex and lastApplied if snatshot is true
	snapdata := rf.persister.ReadSnapshot()
	rf.commitIndex, _ = rf.readMeta(snapdata)
	//apply snapshot to state machine in init state
	message := ApplyMsg{UseSnapshot: true, Snapshot: snapdata}
	rf.wg.Add(1)
	go func() {
		defer rf.wg.Done()
		rf.applyCh <- message
	}()
	rf.lastApplied = rf.commitIndex

	//To eliminate valid Log index == 0, insert a pad entry int index 0.
	if rf.commitIndex == 0 && len(rf.Log) == 0 {
		//index 0 is guard
		rf.Log = append(rf.Log, Entry{0, 0, nil})
		rf.VotedFor = EmptyVote
		rf.CurrentTerm = 0
	}

	rf.state = Initialized
	return nil
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := makeServer(peers, me, persister)
	if err := rf.Init(); err != nil {
		return nil
	}

	rf.stopped = make(chan bool)
	rf.ChangeState(Follower)
	rf.applyCh = applyCh

	//enter event loop
	rf.wg.Add(1)
	go func() {
		defer rf.wg.Done()
		rf.loop()
	}()

	//apply event to state machine
	rf.wg.Add(1)
	go func() {
		defer rf.wg.Done()
		rf.applyLoop()
	}()

	return rf
}

func (rf *Raft) deliver(value interface{}, r interface{}) error {
	if !rf.Running() {
		return StopError
	}

	task := &message{args: value, reply: r, err: make(chan error, 1)}

	//deliver the task to event loop
	select {
	case rf.blockqueue <- task:
	case <-rf.stopped:
		return StopError
	}
	// wait for the task been handle over, and return
	select {
	case <-rf.stopped:
		return StopError
	case suc := <-task.err:
		return suc
	}
}

//loop event
func (rf *Raft) loop() {
	for rf.state != Stopped {
		switch rf.state {
		case Follower:
			rf.followerLoop()
		case Candidate:
			rf.candidateLoop()
		case Leader:
			rf.leaderLoop()
		}
	}
}

func (rf *Raft) applyLoop() {
	for rf.state != Stopped {
		select {
		case <-rf.stopped:
			rf.ChangeState(Stopped)
			return

		case <-rf.applyNotice:
			if rf.lastApplied >= rf.BaseIndex(){
				for rf.commitIndex > rf.lastApplied {
					rf.lastApplied++
					rf.applyCh <- ApplyMsg{rf.lastApplied, rf.Log[rf.lastApplied-rf.BaseIndex()].Command, false, nil}
				}
			}
		}
	}
}
