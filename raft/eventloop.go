package raft

import (
	"sort"
	"time"
)

func (rf *Raft) followerLoop() {
	timeoutChan := random(ElectionTimeout, 2*ElectionTimeout)

	for rf.state == Follower {
		var err error
		update := false

		select {
		case <-rf.stopped:
			// race condition. so Stop and this all need change state
			rf.ChangeState(Stopped)
			return

		// get task in blockqueue.
		case e := <-rf.blockqueue:
			switch req := e.args.(type) {
			case *RequestVoteArgs:
				//get voteGranter from candidator, neead resert expired timer
				rp, _ := (e.reply).(*RequestVoteReply)
				*rp, update = rf.handleRequestVote(req)
			case *AppendEntriesArgs:
				//get heartbears from leader, need reset expired timer
				rp, _ := (e.reply).(*AppendEntriesReply)
				*rp, update = rf.handleAppendEntries(req)
				//snatshot
			case *SnatshotArgs:
				rp, _ := (e.reply).(*SnatshotReply)
				*rp, update = rf.handleInstallSnapshot(req)
			default:
				err = NotLeaderError
			}

			e.err <- err

		case <-timeoutChan:
			//become a candidator.
			rf.ChangeState(Candidate)
		}
		// continue as follower
		if update {
			timeoutChan = random(ElectionTimeout, 2*ElectionTimeout)
		}
	}
}

func (rf *Raft) candidateLoop() {
	rf.VotedFor = EmptyVote
	doVote := true
	votesGranted := 0
	var timeoutChan <-chan time.Time
	var respChan chan *RequestVoteReply

	for rf.state == Candidate {
		//send vote to all the nodes
		if doVote {
			//change local state
			rf.mu.Lock()
			rf.CurrentTerm++
			rf.VotedFor = rf.me
			rf.mu.Unlock()
			rf.persist()

			timeoutChan = random(ElectionTimeout, 2*ElectionTimeout)
			// Send RequestVote RPCs to all other servers
			respChan = make(chan *RequestVoteReply, len(rf.peers)-1)
			rf.logmu.RLock()
			lastLogIndex := rf.LastIndex()
			lastLogTerm := rf.Log[lastLogIndex-rf.BaseIndex()].Term
			rf.logmu.RUnlock()
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me {
					//rf.wg.Add(1)
					go func(server int) {
						//defer rf.wg.Done()
						r := new(RequestVoteReply)
						ok := rf.sendRequestVote(server, RequestVoteArgs{rf.CurrentTerm, rf.me, lastLogIndex, lastLogTerm}, r)
						if ok {
							respChan <- r
						}
					}(i)
				}
			}

			votesGranted = 1
			doVote = false
		}

		if votesGranted == rf.QuorumSize() {
			rf.ChangeState(Leader)
			return
		}

		select {
		case <-rf.stopped:
			rf.ChangeState(Stopped)
			return

		//get the vote reply info, and use handler to add votesGranted or change State
		case reply := <-respChan:
			if success := rf.handleResponseVote(reply); success {
				votesGranted++
			}
		// get task in blockqueue.
		case e := <-rf.blockqueue:
			var err error
			switch req := e.args.(type) {
			//if handle return true, server become a follower, exit the candidator loop
			case *RequestVoteArgs:
				//*(e.reply), _ = rf.handleRequestVote(req)
				rp, _ := (e.reply).(*RequestVoteReply)
				*rp, _ = rf.handleRequestVote(req)
			//if handle return true, return true server become follower, exit the Leader loop
			case *AppendEntriesArgs:
				rp, _ := (e.reply).(*AppendEntriesReply)
				*rp, _ = rf.handleAppendEntries(req)
			case *AgreementArgs:
				err = NotLeaderError
			}

			e.err <- err

		case <-timeoutChan:
			doVote = true
		}
	}
}

func (rf *Raft) leaderLoop() {
	//first, insert a blank no-op enter,instead first heartbeats Charter 8
	heartbeats := true
	//commit Set.
	commitSync := make(map[int]bool)
	var timeoutChan <-chan time.Time
	
	respChan := make(chan *AppendEntriesReply, len(rf.peers)*3)
	//snapshot reply
	snapChan := make(chan *SnatshotReply, len(rf.peers)-1)

	for rf.state == Leader {
		if heartbeats {
			rf.logmu.RLock()
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me {
					rf.boatcastAppend(i, respChan, snapChan)
				}
			}
			rf.logmu.RUnlock()
			timeoutChan = random(HeartbeatInterval, HeartbeatInterval)
			heartbeats = false
		}

		select {
		case <-rf.stopped:
			rf.ChangeState(Stopped)
			return

		case snap := <-snapChan:
			if success := rf.handleSnapshotResponse(snap); success {
				rf.leaderCommit(commitSync, snap.PeerId)
			}

		case reply := <-respChan:
			if success := rf.handleResponseAppend(reply, respChan, snapChan); success {
				rf.leaderCommit(commitSync, reply.PeerId)
			}

		case e := <-rf.blockqueue:
			var err error
			switch req := e.args.(type) {
			//Start func deliver a new agreement task.
			case *AgreementArgs:
				//clien need task, append this command to local Log, and send append request to other peers
				rp, _ := (e.reply).(*AgreementRely)
				*rp = rf.handleRequestAgreement(req, respChan, snapChan)
				//local machine has duplicate to Log
				commitSync[rf.me] = true
				timeoutChan = random(HeartbeatInterval, HeartbeatInterval)
				heartbeats = false
			//other peers vote, return true server become follower, exit the Leader loop
			case *RequestVoteArgs:
				rp, _ := (e.reply).(*RequestVoteReply)
				*rp, _ = rf.handleRequestVote(req)
			//other peers request append, return true server become follower, exit the Leader loop
			case *AppendEntriesArgs:
				rp, _ := (e.reply).(*AppendEntriesReply)
				*rp, _ = rf.handleAppendEntries(req)
			}

			e.err <- err

		case <-timeoutChan:
			heartbeats = true
		}
	}
}

func (rf *Raft) leaderCommit(commitSync map[int]bool, PeerId int) {
	commitSync[PeerId] = true
	//set Leader commitIndex
	if len(commitSync) >= rf.QuorumSize() {
		//If there exists an N such that N > commitIndex, a majority
		//of matchIndex[i] ≥ N, and log[N].term == currentTerm:
		//set commitIndex = N
		var matchLog []int
		for key, _ := range commitSync {
			matchLog = append(matchLog, rf.matchIndex[key])
		}
		sort.Sort(sort.Reverse(sort.IntSlice(matchLog)))
		//select this QuorumSize()-1 index as this commit point
		candIdx := matchLog[rf.QuorumSize()-1]
		//fmt.Printf("candId: %d\n", candId)
		if candIdx > rf.commitIndex && rf.Log[candIdx-rf.BaseIndex()].Term == rf.CurrentTerm {
			rf.commitIndex = candIdx
			//apply this commit to state machine
			rf.applyNotice <- true
		}
	}
}
