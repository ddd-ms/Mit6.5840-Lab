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

import (
	//	"bytes"

	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state       NodeState
	currentTerm int // Term stands for election term
	votedFor    int
	logs        []Entry

	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int

	electionTimer  *time.Timer
	heartbeatTimer *time.Timer
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:          peers, // all known cluster nodes
		persister:      persister,
		me:             me,
		dead:           0,
		state:          StateFollower,
		currentTerm:    0,
		votedFor:       -1,
		logs:           make([]Entry, 1),
		commitIndex:    -1,
		lastApplied:    -1,
		nextIndex:      make([]int, len(peers)),
		matchIndex:     make([]int, len(peers)),
		heartbeatTimer: time.NewTimer(StableHeartbeatTimeout()),
		electionTimer:  time.NewTimer(RandomizedElectionTimeout()),
	}

	// Your initialization code here (3A, 3B, 3C).
	// start ticker goroutine to start elections
	go rf.ticker()
	return rf
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	return rf.currentTerm, rf.state == StateLeader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC handler.

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) getLastLog() Entry {
	return rf.logs[len(rf.logs)-1]
}
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != StateLeader { // only Leader are allowed to spread logs
		return -1, -1, false
	}
	// append new log to Leader rf.logs
	lastLog := rf.getLastLog()
	newLog := Entry{
		lastLog.Index + 1,
		rf.currentTerm,
		command,
	}
	rf.logs = append(rf.logs, newLog)
	// update relevent states
	// rf.matchIndex[rf.me] = newLog.Index
	// rf.nextIndex[rf.me] = newLog.Index + 1
	DPrintf("{Node %v} receives a new command[%v] to replicate in term %v", rf.me, newLog, rf.currentTerm)

	// broadcast AppendEntries RPC call to all followers
	req := new(AppendEntriesRequest)
	req.Term = rf.currentTerm
	req.LeaderId = rf.me
	req.PrevLogIndex = lastLog.Index
	req.PrevLogTerm = lastLog.Term
	req.LeaderCommit = rf.commitIndex
	req.Entries = []Entry{newLog} // TODO:: send one entry once? -> send more maybe
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		// send AppendEntries call to all followers and wait for reply
		// need a timeout mechanism, when triggered, send again until receive all nodes reply

		// mutex to lock respCounter
		var respCounterMu sync.Mutex
		respCounter := 0
		go func(peer int) {
			resp := new(AppendEntriesResponse)
			rf.peers[peer].Call("Raft.AppendEntries", req, resp)
			rpcTimer := time.NewTimer(time.Duration(RPCTimeout) * time.Millisecond)
			for {
				select {
				case <-resp.ok:
					// received from RPC call
					respCounterMu.Lock()
					respCounter += 1
					respCounterMu.Unlock()
					rpcTimer.Stop()
					return
				case <-rpcTimer.C:
					rf.peers[peer].Call("Raft.AppendEntries", req, resp)
					rpcTimer.Reset(time.Duration(RPCTimeout) * time.Millisecond)
				}
			}
		}(peer)
		// think about situation:
		// COMMAND 1 -> leader, leader boardcast AppendEntries RPC call
		// received majority resp, commited COMMAND 1.
		// Node x still respond, COMMAND 2 -> leader at that time.
		// Leader would have to notify node X with req{COMMAND 1;COMMAND 2}

	}
	return newLog.Index, newLog.Term, true
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// this go routine starts a new election
// if no heartbeat received recently.
func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here (3A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		// ms := 50 + (rand.Int63() % 300)
		// time.Sleep(time.Duration(ms) * time.Millisecond)
		select {
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			rf.ChangeState(StateCandidate)
			rf.currentTerm += 1
			rf.StartElection() // handout requestVotes calls and gather responses
			rf.electionTimer.Reset(RandomizedElectionTimeout())
			rf.mu.Unlock()
		case <-rf.heartbeatTimer.C:
			rf.mu.Lock()
			if rf.state == StateLeader {
				// send heartbeats to all other peers
				rf.BroadcastHeartbeat()
				rf.heartbeatTimer.Reset(StableHeartbeatTimeout())
			}
			rf.mu.Unlock()
		}
	}
}

// Assert rf struct already holding rf.mu,
// make sure call rf.mu.Lock() before calling this function
func (rf *Raft) ChangeState(state NodeState) {
	if state == rf.state {
		return
	}
	rf.state = state
	switch state {
	case StateFollower:
		rf.electionTimer.Reset(RandomizedElectionTimeout())
		rf.heartbeatTimer.Stop()
	case StateCandidate:
	case StateLeader:
		rf.electionTimer.Stop()
		rf.heartbeatTimer.Reset(StableHeartbeatTimeout())
	}
}

func (rf *Raft) BroadcastHeartbeat() {
	// send empty appendEntries to all servers except rf.me
	for idx := range rf.peers {
		if idx == rf.me {
			continue
		}
		req := new(AppendEntriesRequest)
		req.Term = rf.currentTerm
		req.LeaderId = rf.me
		resp := new(AppendEntriesResponse)
		go rf.peers[idx].Call("Raft.AppendEntries", req, resp)
	}
}

func (rf *Raft) AppendEntries(req *AppendEntriesRequest, resp *AppendEntriesResponse) {
	DPrintf("Node %d received AppendEntries request %v", rf.me, req)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.ChangeState(StateFollower)
	rf.electionTimer.Reset(RandomizedElectionTimeout())
}

func (rf *Raft) StartElection() {
	req := new(RequestVoteRequest)
	req.Term = rf.currentTerm
	req.CandidateId = rf.me
	// serial procedure begins:
	rf.votedFor = rf.me
	cntVoteGranted := 1
	// end serial procedure
	DPrintf("{Node %v} starts election with RequestVoteRequest %v, peers:%v", rf.me, req, len(rf.peers))
	// send requestVotes
	for idx := range rf.peers {
		if idx == rf.me {
			continue
		}
		// concurrency procedure below, any R/W to rf should be protected
		go func(pIndex int) {
			resp := new(RequestVoteResponse)
			if rf.peers[pIndex].Call("Raft.RequestVote", req, resp) {
				// rpc call procedure returns True means resp has been writen
				DPrintf("{Node %v} received RequestVoteResponse %v from {Node%v}", rf.me, resp, pIndex)
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if rf.currentTerm == req.Term && rf.state == StateCandidate {
					// req.Term as constant, rf.currentTerm might INCR as rf obj receives requestVote call containing newer term
					// at that time, there's no need to consider rf's requestVote calls' response, since they are outdated
					if resp.VoteGranted {
						cntVoteGranted += 1
						if cntVoteGranted > len(rf.peers)/2 {
							// candidate -> leader
							DPrintf("{Node %v} becomes leader with term %v", rf.me, rf.currentTerm)
							rf.ChangeState(StateLeader)
							rf.BroadcastHeartbeat()
							// rf.heartbeatTimer.Reset(StableHeartbeatTimeout())
						}
					} else if resp.Term > rf.currentTerm {
						// out of date
						DPrintf("{Node %v} finds a new leader {Node %v} with term %v and steps down in term %v", rf.me, idx, resp.Term, rf.currentTerm)
						rf.ChangeState(StateFollower)
						rf.currentTerm = resp.Term
						rf.votedFor = -1
					}
				}
			}
		}(idx)
	}
}
func (rf *Raft) RequestVote(req *RequestVoteRequest, resp *RequestVoteResponse) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// defer DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} before processing requestVoteRequest %v and reply requestVoteResponse %v", rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog(), req, resp)

	if req.Term < rf.currentTerm ||
		(req.Term == rf.currentTerm &&
			rf.votedFor != -1 &&
			rf.votedFor != req.CandidateId) {
		// sender out of date or duplicate vote
		resp.VoteGranted = false
		resp.Term = rf.currentTerm
		return
	}
	if req.Term > rf.currentTerm {
		// sender is newer
		rf.ChangeState(StateFollower)
		rf.currentTerm, rf.votedFor = req.Term, -1
	}
	//TODO:: check log stuff
	if !rf.isLogUpToDate(req.LastLogTerm, req.LastLogIndex) {
		resp.Term, resp.VoteGranted = rf.currentTerm, false
		return
	}
	rf.votedFor = req.CandidateId
	rf.electionTimer.Reset(RandomizedElectionTimeout())
	resp.Term, resp.VoteGranted = rf.currentTerm, true
}
func (rf *Raft) isLogUpToDate(term, index int) bool {
	// TODO:: impl
	return true
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
