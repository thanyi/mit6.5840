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
	DataMap map[string]string // server中的数据
	KvCache map[int64]string  // server请求的cache

}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	key := args.Key
	value, ok := kv.DataMap[key]
	if ok {
		// 如果未执行过此操作，则返回数据
		reply.Value = value
	} else {
		reply.Value = ""
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.DataMap[args.Key] = args.Value

}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// 如果存在cache
	if cache, ok := kv.KvCache[args.RequestID]; ok {
		reply.Value = cache
		return
	}
	// 更新DataMap中的值，返回旧值
	old := kv.DataMap[args.Key]
	kv.DataMap[args.Key] += args.Value
	reply.Value = old
	// 更新cache
	kv.KvCache[args.RequestID] = old

	delete(kv.KvCache, args.RequestID-1)
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.DataMap = make(map[string]string)
	kv.KvCache = make(map[int64]string)

	return kv
}
