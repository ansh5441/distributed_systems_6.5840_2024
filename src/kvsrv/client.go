package kvsrv

import (
	"crypto/rand"
	"math/big"
	"sync"

	"6.5840/labrpc"
)

type Clerk struct {
	server                    *labrpc.ClientEnd
	clientId                  int64
	messageSequenceNumber     int64
	messageSequenceNumberLock sync.Mutex
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.server = server
	// You'll have to add code here.
	ck.clientId = nrand()
	ck.messageSequenceNumber = 0
	ck.messageSequenceNumberLock = sync.Mutex{}
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	var args GetArgs
	args.Key = key
	args.ClientId = ck.clientId
	args.MessageSequenceNumber = ck.getMessageSequenceNumber()

	var reply GetReply
	ok := ck.server.Call("KVServer.Get", &args, &reply)
	if !ok {
		// log.Printf("Get(%v) failed", key)
		return ""
	}
	// log.Printf("Get(%v) = %v", key, reply.Value)
	return reply.Value
}

func (ck *Clerk) getMessageSequenceNumber() int64 {
	ck.messageSequenceNumberLock.Lock()
	ck.messageSequenceNumber++
	messageSequenceNumner := ck.messageSequenceNumber
	ck.messageSequenceNumberLock.Unlock()
	return messageSequenceNumner

}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) string {
	// You will have to modify this function.
	var args PutAppendArgs
	args.Key = key
	args.Value = value
	args.ClientId = ck.clientId
	args.MessageSequenceNumber = ck.getMessageSequenceNumber()
	var reply PutAppendReply
	// log.Printf("Client: args = %s(%v,%v)", op, args.Key, args.Value)
	ok := ck.server.Call("KVServer."+op, &args, &reply)
	if !ok {
		// log.Printf("%s(%v,%v) failed", op, key, value)
		return ""
	}
	// log.Printf("Client: %s(%v,%v) = %v", op, key, value, reply.Value)
	return reply.Value
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}
