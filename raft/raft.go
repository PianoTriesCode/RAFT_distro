package raft

import (
	"DS_PA1/labrpc"
	"bytes"
	"encoding/gob"
	"math/rand"
	"sync"
	"time"
	//"//fmt"
)

//States
const FOLLOWER = 0
const LEADER = 1;
const CANDIDATE = 2;

type ApplyMsg struct { 
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for Assignment2; only used in Assignment3
	Snapshot    []byte // ignore for Assignment2; only used in Assignment3
}

type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        	int // index into peers[]
	state 	  	int
	timeout   	int
	heartTimeout int
	currentTerm int
	votedFor 	int
	resetCh chan int
	stopAppend chan int
	appendChannel chan int
	executionChannel chan ApplyMsg
	lastApplied int
	leaderCommit int
	nextIndex []int
	matchIndex []int
	logs []LogEntry

}

func (rf *Raft) GetState() (int, bool) {

	rf.mu.Lock()
    defer rf.mu.Unlock()
    return rf.currentTerm, rf.state == LEADER
}
type RequestVoteArgs struct {
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int

}

type RequestVoteReply struct {
	Term int
	VoteGranted bool
}
 type LogEntry struct{
	Term int
	Command interface{}
 }

type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogterm int
	Entries []LogEntry
	LeaderCommit int

}

type AppendEntriesReply struct {
	Term int
	Appended bool
}

func (rf *Raft) sendAppendEntries(value int, arg AppendEntriesArgs, reply *AppendEntriesReply) bool {
	rf.peers[value].Call("Raft.AppendEntries", arg, &reply)
	return true
}

func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.logs)
	e.Encode(rf.votedFor)

	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) readPersist(data []byte) {
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.logs)
	d.Decode(&rf.votedFor)
}

func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
    term := args.Term
    candidate := args.CandidateId
    lastindex := args.LastLogIndex
    lastTerm := args.LastLogTerm

    rf.mu.Lock()
    defer rf.mu.Unlock()

	if term > rf.currentTerm && len(rf.logs) == 0 {
		if rf.state == LEADER {
			rf.stopAppend <- 1
			go rf.timer()
		} else{
			select{
			case rf.resetCh<-1:
			default:
				go rf.timer()
			}
		}
		rf.state = FOLLOWER
		rf.currentTerm = term
		rf.votedFor = candidate
		reply.VoteGranted = true
		reply.Term = term
		return
	}	

	if (term > rf.currentTerm){
		if (rf.state != FOLLOWER){
			if rf.state == LEADER {
				rf.stopAppend <- 1
				go rf.timer()
			} else{
				select{
				case rf.resetCh<-1:
				default:
					go rf.timer()
					rf.resetCh <- 1
				}
			}
			rf.state = FOLLOWER
			rf.currentTerm = term
			rf.votedFor = -1
		} else{
			rf.currentTerm = term
			rf.votedFor = -1
			// rf.resetCh <- 1
		}
	}

    // Same term, check if this server has already voted in this term
    if term == rf.currentTerm  { //make sure this condition works
        if rf.votedFor == -1 && ((lastTerm > rf.logs[len(rf.logs)-1].Term) || 
		(lastTerm == rf.logs[len(rf.logs)-1].Term && lastindex >= len(rf.logs))) {
            reply.Term = term
            reply.VoteGranted = true
            rf.votedFor = candidate
			select{
			case rf.resetCh<-1:
			default:
			}
        } else {
            reply.VoteGranted = false
            reply.Term = rf.currentTerm
        }
        return
    }

    // Term is outdated, vote denied
    reply.VoteGranted = false
    reply.Term = rf.currentTerm

}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	term := args.Term
	prevLogIndex := args.PrevLogIndex
	// prevLogTerm := args.PrevLogterm -> jugaar for now

	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm


	if (term > rf.currentTerm){
		if (rf.state != FOLLOWER){
			if rf.state == LEADER {
				rf.stopAppend <- 1
				go rf.timer()
				rf.state = FOLLOWER
				rf.currentTerm = term
			} else{
				rf.state = FOLLOWER
				rf.currentTerm = term
				select{
				case rf.resetCh<-1:
				default:
					go rf.timer()
					rf.resetCh <- 1
				}
				rf.votedFor = -1
			}
		}
	}

	if term == rf.currentTerm {
		if rf.state != FOLLOWER {
			if rf.state == LEADER {
				rf.stopAppend <- 1
				go rf.timer()
			} else{
				select{
				case rf.resetCh<-1:
				default:
					go rf.timer()
					rf.resetCh <- 1
				}
			}
			rf.state = FOLLOWER
			rf.currentTerm = term
			rf.votedFor = -1
		}else{
			rf.resetCh <- 1
			rf.currentTerm = term
			rf.votedFor = -1
		}
		if (len(rf.logs) == 0){
			rf.logs = args.Entries
			reply.Appended = true
		} else if prevLogIndex == -1 || (args.Entries[len(args.Entries) - 1].Term > rf.logs[len(rf.logs) - 1].Term) || 
		((args.Entries[len(args.Entries) - 1].Term == rf.logs[len(rf.logs) - 1].Term) && (len(args.Entries) >= len(rf.logs))) {
			rf.logs = args.Entries
			reply.Appended = true
		} else{
			reply.Appended = false
		}
		if (rf.leaderCommit < args.LeaderCommit) && reply.Appended{
			rf.leaderCommit = args.LeaderCommit
			rf.appendChannel <- 1;
			rf.persist()
		}else{
		
		}
	} 
}


func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, &reply)
	return ok
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := -1
	Term := -1
	isLeader := rf.state == LEADER

	// If this server is the leader, add the command to its log
	if isLeader {
		log := LogEntry{
			Term:    rf.currentTerm,
			Command: command,
		}
		if len(rf.logs) == 0 || rf.logs[len(rf.logs) - 1] != log{
			rf.logs = append(rf.logs, log)
		}
		index = len(rf.logs)   // Update index to the new log entry’s index
		Term = rf.currentTerm  // Set term to the leader's current term
	} else {
		//fmt.Printf("Server %d is not the leader, command not appended\n", rf.me)
	}
	return index, Term, isLeader
}

func (rf *Raft) Kill() {
	
}

func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	rf := &Raft{
		peers:        peers,
		persister:    persister,
		me:           me,
		currentTerm:  0,
		state:        FOLLOWER,
		timeout:      r.Intn(400) + 400,
		heartTimeout: 100,
		votedFor:     -1,
		resetCh:      make(chan int),
		stopAppend:   make(chan int),
		appendChannel: make (chan int),
		executionChannel : applyCh,
		nextIndex: make([]int,len(peers)),
		matchIndex: make([]int,len(peers)),
		logs: []LogEntry{},
		leaderCommit: 0,
		lastApplied: 0,
	}

	rf.readPersist(persister.ReadRaftState())



	go rf.timer()
	go rf.appendstuff()

	return rf
}

func (rf *Raft) timer() {
	for {
		select {
		case <-rf.resetCh:
			
			continue
		case <-time.After(time.Millisecond * time.Duration(rf.timeout)):
			
			
			rf.handleVote()
			return
		}
	}
}

func (rf *Raft) handleVote() {
	votes := 1
	broke := false
	done := make(chan int)

	rf.mu.Lock()
	if (rf.state == LEADER || rf.state == CANDIDATE){
		rf.mu.Unlock()
		return
	}
	rf.currentTerm++
	rf.state = CANDIDATE
	rf.votedFor = rf.me
	
	rf.mu.Unlock()

	rf.mu.Lock()
	args := RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
		LastLogIndex: len(rf.logs),
	}
	if len(rf.logs) > 0 {
		args.LastLogTerm = rf.logs[len(rf.logs)-1].Term
	}
	rf.mu.Unlock()
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int, broke *bool, votes *int) {
			reply := RequestVoteReply{}
			if rf.sendRequestVote(server, args, &reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if rf.state != CANDIDATE {
					return
				}
				if reply.Term > rf.currentTerm {
					
					rf.state = FOLLOWER
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					*broke = true
					done <- 1
					rf.persist()
					go rf.timer()
					return
				} else if reply.VoteGranted {
					*votes++
				
					if *votes > (len(rf.peers) / 2) && rf.state != LEADER {
						
						rf.state = LEADER
						go rf.leaderStatus()
						done <- 1
						rf.persist()
						return
					}
				} else {
					rf.currentTerm = reply.Term
					rf.persist()
				}
			} else {
				//fmt.Printf("Server %d did not receive response from Server %d\n", rf.me, server)
			}
		}(i, &broke, &votes)
	}


	select {
	case <-done:
		return
	case <-rf.resetCh:
		go rf.timer()
		return
	case <-time.After(time.Millisecond * time.Duration(rf.timeout)):
		rf.mu.Lock()
		if (rf.state == FOLLOWER || rf.state == LEADER){
			return
		}
		rf.state = FOLLOWER
		rf.mu.Unlock()
		rf.resetTimer()
		rf.handleVote()
		return
	}
}


func (rf *Raft) sendHeartbeat() {
	rf.mu.Lock()
	term := rf.currentTerm
	args := AppendEntriesArgs{
		Term:         term,
		LeaderId:     rf.me,
		PrevLogIndex: -1,
		PrevLogterm:  -1,
		Entries:      rf.logs,
		LeaderCommit: rf.leaderCommit,
	}
	votes := 1

	// Set previous log info if there are entries
	if len(rf.logs) > 1 {
		args.PrevLogIndex = len(rf.logs) 
		args.PrevLogterm = rf.logs[args.PrevLogIndex - 1].Term 

	} 
	broke := false
	rf.mu.Unlock()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int, broke *bool, votes *int) {
			reply := AppendEntriesReply{}
			if rf.sendAppendEntries(server, args, &reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if (len(args.Entries) != len(rf.logs)){
					return
				}				
				
				if reply.Term > rf.currentTerm && rf.state != FOLLOWER{
					rf.votedFor = -1
					rf.currentTerm = reply.Term
					*broke = true
					if (rf.state == LEADER){
						rf.stopAppend <- 1
						go rf.timer()
					} else{
						select{
						case rf.resetCh <- 1:
						default:
							go rf.timer()
							rf.resetCh <- 1
						}
					}
					rf.state = FOLLOWER
					return
				} else if reply.Appended && len(rf.logs) >= 1 && len(rf.logs) > rf.leaderCommit{
					*votes++
					
					if *votes > (len(rf.peers) / 2) && !*broke && len(args.Entries) == len(rf.logs){
						rf.leaderCommit = len(args.Entries)// assuming 1-indexed logs
						rf.appendChannel <- 1 // signal to apply the committed entry
						*broke = true
					}
					rf.persist()
					return
				} else {
					//fmt.Printf("No need for votes since length and commit align or maybe bool is wrong so no vote%d %d %t server: %d \n", len(rf.logs),rf.leaderCommit, reply.Appended, rf.me)
				}
			} else {
				//fmt.Printf("Server %d failed to receive response from server %d\n", rf.me, server)
			}
		}(i, &broke, &votes)
	}
}

func (rf *Raft) appendstuff() {

	for {
		
		select {
		case <-rf.appendChannel:
			rf.mu.Lock()
		for rf.lastApplied < rf.leaderCommit {
			if rf.lastApplied >= len(rf.logs) {
			
				break 
			}

			arg := ApplyMsg{
				Index:   rf.lastApplied + 1,
				Command: rf.logs[rf.lastApplied].Command,
			}


			
			rf.executionChannel <- arg
			rf.lastApplied++
		}
		rf.mu.Unlock()
			// Proceed with applying entries
		}
	}
}


func (rf *Raft) leaderStatus() {
	for {
		select {
		case <-rf.stopAppend:
			return
		case <-time.After(time.Millisecond * time.Duration(rf.heartTimeout)):
			
			rf.sendHeartbeat()
		}
	}
}

func (rf *Raft) resetTimer() {
	rf.mu.Lock()
	rf.timeout = rand.Intn(400) + 400
	rf.mu.Unlock()
}
