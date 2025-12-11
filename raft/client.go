package raftkv

import (
	"DS_PA1/labrpc"
	"crypto/rand"
	"math/big"
)


type Clerk struct {
	servers []*labrpc.ClientEnd
	id int64
	Seqno int
	// You will have to modify this struct.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.id = nrand()
	ck.Seqno = 0
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string { //check this get and Put both.
	//DPrintf("Clerk Get called: Key=%s, ID=%d, Seqno=%d", key, ck.id, ck.Seqno)

	args := GetArgs{
		Key: key,
		ID:  int(ck.id),
		Seq: ck.Seqno,
		Op:  "Get",
	}
	ck.Seqno = ck.Seqno + 1

	reply := GetReply{}
	ind := 0
	for {
		ok := ck.servers[ind].Call("RaftKV.Get", &args, &reply)
		if !ok {
			//there were prints here
		} else {
		}
		if reply.Err == OK {
			return reply.Value
		}
		ind = (ind + 1) % len(ck.servers) // Rotate through servers

	}
}

func (ck *Clerk) PutAppend(key string, value string, op string) {

	args := PutAppendArgs{
		Key:   key,
		Value: value,
		ID:    int(ck.id),
		Seq:   ck.Seqno,
		Op:    op,
	}
	ck.Seqno++

	reply := PutAppendReply{}
	ind := 0
	for {
		ok := ck.servers[ind].Call("RaftKV.PutAppend", &args, &reply)
		if !ok{
			//prints here, checking if i even get it back.
		} else{

		}
		if reply.Err == OK {
			return
		}
		ind = (ind + 1) % len(ck.servers) 
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
