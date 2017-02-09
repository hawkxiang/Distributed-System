package raft

import "bytes"
import "encoding/gob"

//import "fmt"

func (rf *Raft) TakeSnatshot(snapstate []byte, preindex int, maxraftstate int) {
	rf.logmu.Lock()
	defer rf.logmu.Unlock()
	defer rf.persist()
	if preindex <= rf.BaseIndex() || preindex > rf.lastApplied || rf.persister.RaftStateSize() < maxraftstate{
		return
	}

	//snapshot
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	//meta
	e.Encode(preindex)
	e.Encode(rf.Log[preindex-rf.BaseIndex()].Term)
	data := w.Bytes()
	data = append(data, snapstate...)
	rf.persister.SaveSnapshot(data)

	//compaction, drop rf.Log through preindex, garbage collection
	//index 0 is guard, eliminate slice index out range
	rf.Log = rf.Log[preindex-rf.BaseIndex():]
	
}

type SnatshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type SnatshotReply struct {
	Term        int
	PeerId      int
	LastInclude int
}

func (rf *Raft) InstallSnapshot(args SnatshotArgs, reply *SnatshotReply) {
	ok := rf.deliver(&args, reply)
	if ok != nil {
		reply = nil
	}
}

func (rf *Raft) handleInstallSnapshot(args *SnatshotArgs) (SnatshotReply, bool) {
	if args.Term < rf.CurrentTerm {
		return SnatshotReply{Term: rf.CurrentTerm, PeerId: rf.me, LastInclude: 0}, false
	}
	if args.Term > rf.CurrentTerm {
		rf.updateCurrentTerm(args.Term, args.LeaderId)
	} else {
		rf.VotedFor = args.LeaderId
	}
	defer rf.persist()
	//snapshot
	rf.persister.SaveSnapshot(args.Data)
	//compaction, drop rf.Log through preindex, garbage collection
	rf.logmu.Lock()
	var newLog []Entry
	//rf.Log always has a guard
	newLog = append(newLog, Entry{args.LastIncludedIndex, args.LastIncludedTerm, nil})
	for i := len(rf.Log)-1; i >= 0; i-- {
		if rf.Log[i].Index == args.LastIncludedIndex && rf.Log[i].Term == args.LastIncludedTerm {
			newLog = append(newLog, rf.Log[i+1:]...)
			break
		}
	}
	rf.Log = newLog
	reply := SnatshotReply{Term: rf.CurrentTerm, PeerId: rf.me, LastInclude: rf.LastIndex()}
	rf.logmu.Unlock()

	rf.commitIndex = args.LastIncludedIndex
	rf.lastApplied = args.LastIncludedIndex

	rf.applyCh <- ApplyMsg{UseSnapshot: true, Snapshot: args.Data}
	return reply, true
}


func (rf *Raft) handleSnapshotResponse(reply *SnatshotReply) bool {
	if reply.Term > rf.CurrentTerm {
		rf.updateCurrentTerm(reply.Term, EmptyVote)
		return false
	}
	if reply.LastInclude > 0 {
		rf.matchIndex[reply.PeerId] = reply.LastInclude
		rf.nextIndex[reply.PeerId] = reply.LastInclude + 1
	}
	return true
}
