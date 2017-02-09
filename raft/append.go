package raft

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
		return AppendEntriesReply{rf.CurrentTerm, false, EmptyVote, -1}, false
	}

	if args.Term > rf.CurrentTerm {
		rf.updateCurrentTerm(args.Term, args.LeaderId)
	} else {
		// the local server can not leader, maybe candidate or Follower
		if rf.state == Candidate {
			rf.state = Follower
		}
		
		rf.VotedFor = args.LeaderId
		
	} 
	
	if args.PrevLogIndex < rf.commitIndex {
		return AppendEntriesReply{rf.CurrentTerm, false, rf.me, rf.commitIndex}, true 
	}

	rf.logmu.Lock()
	
	if args.PrevLogIndex > rf.LastIndex() {
		rf.logmu.Unlock()
		return AppendEntriesReply{rf.CurrentTerm, false, rf.me, rf.LastIndex()}, true
	}

	if args.PrevLogIndex > rf.BaseIndex() {
		term := rf.Log[args.PrevLogIndex-rf.BaseIndex()].Term
		if args.PrevLogTerm != term {
			reply := AppendEntriesReply{rf.CurrentTerm, false, rf.me, rf.BaseIndex()}
			for i := args.PrevLogIndex - 1; i > rf.BaseIndex(); i-- {
				if rf.Log[i - rf.BaseIndex()].Term != term {
					reply.LastMatch = max(i,rf.commitIndex)
					break
				}
			}
			rf.logmu.Unlock()
			return reply, true
		}
	}
	rf.Log = rf.Log[:args.PrevLogIndex + 1 - rf.BaseIndex()]
	rf.Log = append(rf.Log, args.Entries...)

	if args.LeaderCommit > rf.commitIndex {
		//apply this commit to state machine
		idx := min(args.LeaderCommit, rf.LastIndex())
		//only commit current term log, befor logs will be commit
		if rf.CurrentTerm == rf.Log[idx-rf.BaseIndex()].Term {
			rf.commitIndex = idx
			rf.applyNotice <- true
		}
	}
	r := AppendEntriesReply{rf.CurrentTerm, true, rf.me, rf.LastIndex()}
	rf.logmu.Unlock()
	rf.persist()
	return r, true
}

func (rf *Raft) handleHeartResponse(reply *AppendEntriesReply) bool {
	if reply.Term > rf.CurrentTerm {
		rf.updateCurrentTerm(reply.Term, EmptyVote)
	}
	
	if reply.LastMatch > 0{
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

			rf.logmu.Lock()
			rf.boatcastAppend(reply.PeerId, respChan, snapChan)
			rf.logmu.Unlock()
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
	rf.logmu.Lock()
	rf.Log = append(rf.Log, entry)
	//to agreement with followers
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			rf.boatcastAppend(i, respChan, snapChan)
		}
	}
	rf.logmu.Unlock()
	rf.persist()
	rf.matchIndex[rf.me] = rf.nextIndex[rf.me]
	rf.nextIndex[rf.me]++
	return AgreementRely{rf.CurrentTerm, rf.matchIndex[rf.me]}
}

func (rf *Raft) boatcastAppend(server int, respChan chan *AppendEntriesReply, snapChan chan *SnatshotReply) {
	if rf.nextIndex[server] > rf.BaseIndex() {
		prevIndex := rf.nextIndex[server] - 1
		var entries []Entry
		preTerm := -1
		if prevIndex <= rf.LastIndex() {
			entries = rf.Log[rf.nextIndex[server]-rf.BaseIndex():]
			preTerm = rf.Log[prevIndex-rf.BaseIndex()].Term
		}
		args := AppendEntriesArgs{rf.CurrentTerm, rf.me, prevIndex, preTerm, entries, rf.commitIndex}
		go func() {
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
		go func() {
			r := new(SnatshotReply)
			ok := rf.sendInstallSnapshot(server, args, r)
			if ok {
				snapChan <- r
			}
		}()
	}
}
