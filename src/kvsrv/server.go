package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex
	// Your definitions here.
	// declare a map to store key-value pairs
	kv map[string]string
	// client vs last processed message map
	lastProcessed         map[int64]int64
	lastProcessedResponse map[int64]string
	lastProcessedLock     sync.Mutex
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	// log.Printf("Server: Get(%v)", args.Key)
	value := kv.get(args.Key)
	reply.Value = value
	kv.mu.Unlock()

}

func (kv *KVServer) get(key string) string {
	// get the value of the key
	value, ok := kv.kv[key]
	if ok {
		return value
	}
	return ""
}

func (kv *KVServer) CheckMessageValidity(clientId int64, messageSequenceNumber int64) bool {
	// kv.lastProcessedLock.Lock()
	value, ok := kv.lastProcessed[clientId]
	// messageSequenceNumber is the first message from the client
	if !ok {
		kv.lastProcessed[clientId] = messageSequenceNumber
		// kv.lastProcessedLock.Unlock()
		return true
	}
	if value < messageSequenceNumber {
		kv.lastProcessed[clientId] = messageSequenceNumber
		// kv.lastProcessedLock.Unlock()
		return true
	}
	// kv.lastProcessedLock.Unlock()
	return false
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	// log.Printf("Server: Put(%v, %v)", args.Key, args.Value)
	// Your code here.
	if !kv.CheckMessageValidity(args.ClientId, args.MessageSequenceNumber) {
		reply.Value = kv.get(args.Key)
		kv.mu.Unlock()
		return
	}

	kv.kv[args.Key] = args.Value

	// log.Printf("Server: Put(%v, %v) = %v", args.Key, args.Value, kv.kv[args.Key])
	kv.mu.Unlock()
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	// log.Printf("Server: Append %v", args.Key)
	if !kv.CheckMessageValidity(args.ClientId, args.MessageSequenceNumber) {
		// remove the current value and return the old value
		reply.Value = kv.lastProcessedResponse[args.ClientId]
		kv.mu.Unlock()
		return
	}
	value, ok := kv.kv[args.Key]

	if !ok {
		reply.Value = ""
		// kv.kv[args.Key] = args.Value
	} else {
		reply.Value = value
		kv.lastProcessedResponse[args.ClientId] = value
		kv.kv[args.Key] = value + args.Value
	}
	kv.mu.Unlock()
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	// You may need initialization code here.
	kv.kv = make(map[string]string)
	kv.mu = sync.Mutex{}
	kv.lastProcessed = make(map[int64]int64)
	kv.lastProcessedLock = sync.Mutex{}
	kv.lastProcessedResponse = make(map[int64]string)

	return kv
}
