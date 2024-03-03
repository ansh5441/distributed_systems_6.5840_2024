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
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	// get the value of the key
	value, ok := kv.kv[args.Key]
	if ok {
		reply.Value = value
	} else {
		reply.Value = ""
	}
	kv.mu.Unlock()
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	kv.kv[args.Key] = args.Value
	kv.mu.Unlock()
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	value, ok := kv.kv[args.Key]
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

	return kv
}
