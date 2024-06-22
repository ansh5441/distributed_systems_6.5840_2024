package kvsrv

import (
	"math/rand"
	"sync"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	server                    *labrpc.ClientEnd
	clientId                  int64
	messageSequenceNumber     int64
	messageSequenceNumberLock sync.Mutex
}

func nrand() int64 {
	// generate a random number between 0 and 9999
	randomNumber := rand.Int63()
	return randomNumber % 10000
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

	// log.Printf("clientId: %v, messageSequenceNumber: %v, Get(%v)", args.ClientId, args.MessageSequenceNumber, key)
	var reply GetReply
	ok := ck.server.Call("KVServer.Get", &args, &reply)
	backoffMS := 10
	for !ok {
		// log.Printf("clientId: %v, messageSequenceNumber: %v failed", args.ClientId, args.MessageSequenceNumber)
		backoffMS *= 2
		time.Sleep(time.Duration(backoffMS) * time.Millisecond)
		ok = ck.server.Call("KVServer.Get", &args, &reply)
	}
	// log.Printf("Get(%v) = %v", key, reply.Value)
	return reply.Value
}

func (ck *Clerk) getMessageSequenceNumber() int64 {
	ck.messageSequenceNumberLock.Lock()
	ck.messageSequenceNumber++
	messageSequenceNumber := ck.messageSequenceNumber
	ck.messageSequenceNumberLock.Unlock()
	return messageSequenceNumber

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
	// log.Printf("clientId: %v, messageSequenceNumber: %v, %s(%v,%v)", args.ClientId, args.MessageSequenceNumber, op, key, value)
	ok := ck.server.Call("KVServer."+op, &args, &reply)
	backoffMS := 10
	for !ok {
		// log.Printf("clientID: %v, messageSequenceNumber: %v failed", args.ClientId, args.MessageSequenceNumber)
		backoffMS *= 2
		time.Sleep(time.Duration(backoffMS) * time.Millisecond)
		ok = ck.server.Call("KVServer."+op, &args, &reply)

	}
	// log.Printf("ClientID: %v, %s(%v,%v) = %v", args.ClientId, op, key, value, reply.Value)
	return reply.Value
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}
