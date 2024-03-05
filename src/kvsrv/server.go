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
	lastProcessed     map[int64]int64
	lastProcessedLock sync.Mutex
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	value := kv.get(args.Key)
	reply.Value = value

}

func (kv *KVServer) get(key string) string {
	kv.mu.Lock()
	// get the value of the key
	value, ok := kv.kv[key]
	if ok {
		kv.mu.Unlock()
		return value
	}
	kv.mu.Unlock()
	return ""
}

func (kv *KVServer) CheckMessageValidity(clientId int64, messageSequenceNumber int64) bool {
	kv.lastProcessedLock.Lock()
	value, ok := kv.lastProcessed[clientId]
	if !ok {
		kv.lastProcessed[clientId] = messageSequenceNumber
		kv.lastProcessedLock.Unlock()
		return true
	}
	if value < messageSequenceNumber {
		kv.lastProcessed[clientId] = messageSequenceNumber
		kv.lastProcessedLock.Unlock()
		return true
	}
	kv.lastProcessedLock.Unlock()
	return false
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if !kv.CheckMessageValidity(args.ClientId, args.MessageSequenceNumber) {
		reply.Value = kv.get(args.Key)
		return
	}
	kv.mu.Lock()
	// log.Printf("Server: Put(%v, %v)", args.Key, args.Value)
	kv.kv[args.Key] = args.Value
	// log.Printf("Server: Put(%v, %v) = %v", args.Key, args.Value, kv.kv[args.Key])
	kv.mu.Unlock()
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if !kv.CheckMessageValidity(args.ClientId, args.MessageSequenceNumber) {
		reply.Value = kv.get(args.Key)
		return
	}
	kv.mu.Lock()
	value, ok := kv.kv[args.Key]
	// log.Printf("Server: Append(%v, %v)", args.Key, args.Value)
	if !ok {
		reply.Value = ""
		kv.kv[args.Key] = args.Value
	} else {
		reply.Value = value
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

	return kv
}
