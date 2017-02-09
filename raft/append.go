package raft

//import "fmt"

//AppendEntries RPC arguments structure.
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

//AppendEntries RPC reply structure.
type AppendEntriesReply struct {
	Term    int
	Success bool
	PeerId  int
	//return remote follower log match index
	LastMatch int
}

type AgreementArgs struct {
	Command interface{}
}

type AgreementRely struct {
	Term  int
	Index int
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	ok := rf.deliver(&args, reply)
	if ok != nil {
		reply = nil
	}
}

func (rf *Raft) handleAppendEntries(args *AppendEntriesArgs) (AppendEntriesReply, bool) {
	
	if args.Term < rf.CurrentTerm {
		//change the remote server which the AppendEntries come from to be a follower.
		//heartbeats false, append false
		//return AppendEntriesReply{rf.CurrentTerm, false, rf.me, len(rf.Log) - 1}, false
		return AppendEntriesReply{rf.CurrentTerm, false, EmptyVote, -1}, false
	}

	if args.Term > rf.CurrentTerm {
		rf.updateCurrentTerm(args.Term, args.LeaderId)
	} else {
		// the local server can not leader, maybe candidate or Follower
		if rf.state == Candidate {
			rf.mu.Lock()
			rf.state = Follower
			rf.mu.Unlock()
		}
		
		rf.VotedFor = args.LeaderId
		
	}
	defer rf.persist()

	if args.PrevLogIndex < rf.commitIndex {
		return AppendEntriesReply{rf.CurrentTerm, false, rf.me, rf.commitIndex}, true 
	}

	rf.logmu.Lock()
	defer rf.logmu.Unlock()
	lastidx := rf.LastIndex()
	baseidx := rf.BaseIndex()
	if args.PrevLogIndex > lastidx {
		return AppendEntriesReply{rf.CurrentTerm, false, rf.me, lastidx}, true
	}

	if args.PrevLogIndex > baseidx {
		term := rf.Log[args.PrevLogIndex-baseidx].Term
		if args.PrevLogTerm != term {
			reply := AppendEntriesReply{rf.CurrentTerm, false, rf.me, baseidx}
			for i := args.PrevLogIndex - 1; i > baseidx; i-- {

				if rf.Log[i - baseidx].Term != term {
					reply.LastMatch = max(i,rf.commitIndex)
					break
				}
			}
			return reply, true
		}
	}

	if args.PrevLogIndex < baseidx {
		return AppendEntriesReply{rf.CurrentTerm, false, EmptyVote, -1}, false
	} else {
		rf.Log = rf.Log[:args.PrevLogIndex + 1 - baseidx]
		rf.Log = append(rf.Log, args.Entries...)
	}

	if args.LeaderCommit > rf.commitIndex {
		//apply this commit to state machine
		idx := min(args.LeaderCommit, lastidx)
		//only commit current term log, befor logs will be commit
		if rf.CurrentTerm == rf.Log[idx-baseidx].Term {
			rf.commitIndex = idx
			rf.applyNotice <- true
		}
	}
	return AppendEntriesReply{rf.CurrentTerm, true, rf.me, lastidx}, true
}

func (rf *Raft) handleHeartResponse(reply *AppendEntriesReply) bool {
	if !reply.Success {
		if reply.Term > rf.CurrentTerm {
			rf.updateCurrentTerm(reply.Term, EmptyVote)
		}
	} else {
		rf.matchIndex[reply.PeerId] = reply.LastMatch
		rf.nextIndex[reply.PeerId] = reply.LastMatch + 1
	}
	return reply.Success
}

func (rf *Raft) handleResponseAppend(reply *AppendEntriesReply, respChan chan *AppendEntriesReply, snapChan chan *SnatshotReply) bool {
	if !reply.Success {
		if reply.Term > rf.CurrentTerm {
			rf.updateCurrentTerm(reply.Term, EmptyVote)
		} else if reply.PeerId != EmptyVote {
			//decrement nextIndex, resend append request to remote(reply.PeedId).
			/*rf.nextIndex[reply.PeerId]--*/
			rf.nextIndex[reply.PeerId] = reply.LastMatch + 1
			rf.logmu.RLock()
			rf.boatcastAppend(reply.PeerId, respChan, snapChan)
			rf.logmu.RUnlock()
		}
		
	} else {
		rf.matchIndex[reply.PeerId] = reply.LastMatch
		rf.nextIndex[reply.PeerId] = reply.LastMatch + 1
	}
	return reply.Success
}

func (rf *Raft) handleRequestAgreement(req *AgreementArgs, respChan chan *AppendEntriesReply, snapChan chan *SnatshotReply) AgreementRely {
	//append this entry to local machine.
	entry := Entry{rf.nextIndex[rf.me], rf.CurrentTerm, req.Command}
	rf.logmu.RLock()
	rf.Log = append(rf.Log, entry)
	//to agreement with followers
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			rf.boatcastAppend(i, respChan, snapChan)
		}
	}
	rf.logmu.RUnlock()
	rf.persist()
	rf.matchIndex[rf.me] = rf.nextIndex[rf.me]
	rf.nextIndex[rf.me]++
	return AgreementRely{rf.CurrentTerm, rf.matchIndex[rf.me]}
}

func (rf *Raft) boatcastAppend(server int, respChan chan *AppendEntriesReply, snapChan chan *SnatshotReply) {
	if rf.nextIndex[server] > rf.BaseIndex() {
		prevIndex := rf.nextIndex[server] - 1
		var entries []Entry
		var args AppendEntriesArgs
		if rf.nextIndex[server] <= rf.LastIndex() {
			entries = rf.Log[rf.nextIndex[server]-rf.BaseIndex():]
			args = AppendEntriesArgs{rf.CurrentTerm, rf.me, prevIndex, rf.Log[prevIndex-rf.BaseIndex()].Term, entries, rf.commitIndex}
		} else {
			args = AppendEntriesArgs{rf.CurrentTerm, rf.me, prevIndex, 0, nil, rf.commitIndex}
		}
		//rf.wg.Add(1)
		go func() {
			//defer rf.wg.Done()
			r := new(AppendEntriesReply)
			ok := rf.sendAppendEntries(server, args, r)
			if ok {
				respChan <- r
			}
		}()
	} else {
		var args SnatshotArgs
		args.Term = rf.CurrentTerm
		args.LeaderId = rf.me
		args.Data = rf.persister.ReadSnapshot()
		args.LastIncludedIndex, args.LastIncludedTerm = rf.readMeta(args.Data)
		//rf.wg.Add(1)
		go func() {
			//defer rf.wg.Done()
			r := new(SnatshotReply)
			ok := rf.sendInstallSnapshot(server, args, r)
			if ok {
				snapChan <- r
			}
		}()
	}
}
