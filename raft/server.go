package raftkv

import (
	"encoding/gob"
	"DS_PA1/labrpc"
	"log"
	"DS_PA1/src/raft"
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
	Key string
	Value string
	Operation string
	ID int
	Seq int
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	kvstore (map[string]string)
	seenStore (map[int]int)
	channelReq map[int]chan Op //should I make by id or req?

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
}


func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	newOp := Op{
		Key:       args.Key,
		Operation: "Get",
		Seq:       args.Seq,
		ID:        args.ID,
	}
	kv.mu.Lock()
	ok, thing := kv.seenStore[args.ID] //check for existence and key
	kv.mu.Unlock()
	if thing && ok == args.Seq {
		kv.mu.Lock() //Bonus fr
		reply.Value = string(kv.kvstore[args.Key]) //was a byte map before im not changing this because chal raha hai
		reply.Err = OK
		reply.WrongLeader = false
		kv.mu.Unlock()
		return
	} else{
	}
	if (ok > newOp.Seq){
		reply.WrongLeader = false
		reply.Err = OK
		return
	}
	
	
	index, _, isLeader := kv.rf.Start(newOp) //since the index is going to be unique (since I appended it to the leader)c, I make a channel on this

	if !isLeader {
		reply.WrongLeader = true
		return
	}
	kv.mu.Lock()
	ackChan := make(chan Op, 1) //buffer to ensure at-most-once semantics.
	kv.channelReq[index] = ackChan //I want to make like, a consensus similar to what I did in PA1, where i would send a request to a request handler
	kv.mu.Unlock() //this time, i want each request to have its own channel so that it may communicate with the applyer per request
	time.Sleep(100 * time.Millisecond) //im waiting for raft to hopefully reach a consensus at this point

	select {
	case correct := <-ackChan:
		kv.mu.Lock()
		delete(kv.channelReq,index)
		kv.mu.Unlock()
		if args.Seq == correct.Seq && args.ID == correct.ID { // the arg I recieved should be the same one I sent to get applied.  code passes now
			reply.Value = string(kv.kvstore[args.Key])
			reply.Err = OK
			reply.WrongLeader = false //why am I stuck here?
			return
		} else {
			reply.WrongLeader = true
		}
	default:
		kv.mu.Lock()
		delete(kv.channelReq,index)
		kv.mu.Unlock()
		reply.WrongLeader = true //find somewhere else
		return
	}
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	newOp := Op{
		Key:       args.Key,
		Value:     args.Value,
		Operation: args.Op,
		ID:        args.ID,
		Seq:       args.Seq,
	}
	kv.mu.Lock()
	ok, wing := kv.seenStore[args.ID] //error to see if it exists.
	kv.mu.Unlock()
	if wing && ok == newOp.Seq {
		reply.Err = OK
		reply.WrongLeader = false
		return
	}
	if (ok > args.Seq){ //if the thing I got is bigger, toh back to netcen lol
		reply.WrongLeader = false
		reply.Err = OK
		return
	}
	index, _, isLeader := kv.rf.Start(newOp)
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	kv.mu.Lock()
	ackChan := make(chan Op, 1) 
	kv.channelReq[index] = ackChan //yay
	kv.mu.Unlock()
	
	time.Sleep(100 * time.Millisecond)

	select {
	case correct := <-ackChan:
		kv.mu.Lock()
		if args.Seq == correct.Seq && args.ID == correct.ID {
			reply.Err = OK
			reply.WrongLeader = false
			delete(kv.channelReq,index)
		} else {
			reply.WrongLeader = true
			delete(kv.channelReq,index)
		}
		kv.mu.Unlock()
	default:
		kv.mu.Lock()
		reply.WrongLeader = true
		reply.Err = Err("Timeout")
		delete(kv.channelReq,index)
		kv.mu.Unlock()
	}
}

func (kv *RaftKV) handleApply() {
	for {
		select {
		case msg := <-kv.applyCh:
			////DPrintf("ApplyMsg received: CommandIndex=%d", msg.Index) //UGHHHH
			operation := msg.Command.(Op)
			kv.mu.Lock()
			seqNo, bing := kv.seenStore[operation.ID]
			kv.mu.Unlock()
			if !(bing && seqNo == operation.Seq){ //check this to see if its not old at this point. 
				kv.mu.Lock()
			switch operation.Operation {
			case "Put":
				kv.kvstore[operation.Key] = operation.Value
				kv.seenStore[operation.ID] = operation.Seq
			case "Append":
				////DPrintf("Applying Append: Key=%s, Value=%s", operation.Key, operation.Value)
				kv.kvstore[operation.Key] += operation.Value
				kv.seenStore[operation.ID] = operation.Seq //updating
			case "Get":
				kv.seenStore[operation.ID] = operation.Seq //dont really need to do anything with this, just put it as last seen.
			}
				kv.mu.Unlock()
		}
			kv.mu.Lock()
			ch, ok := kv.channelReq[msg.Index] //confirm somebody made a channel on this index.
			kv.mu.Unlock()
			if ok {
				ch <- operation
			} else {
				////DPrintf("No client waiting for Index=%d", msg.Index)
			}
		}
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

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.kvstore = make(map[string]string)
	kv.seenStore = make(map[int]int)
	kv.channelReq = make(map[int]chan Op)
	go kv.handleApply()


	return kv
}
