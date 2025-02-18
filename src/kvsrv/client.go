package kvsrv

import (
	"6.5840/labrpc"
	"time"
)
import "crypto/rand"
import "math/big"

type RequestTpye int

type Clerk struct {
	server *labrpc.ClientEnd
	// You will have to modify this struct.
	requestId int64
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
	ck.requestId = nrand()
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
	var args = GetArgs{
		Key: key,
	}
	var reply = GetReply{}
	for {
		ok := ck.server.Call("KVServer.Get", &args, &reply)
		if ok {
			break
		} else {
			DPrintf("[Client] Call ***(%v) failed, retrying...", args)
		}
	}
	return reply.Value
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
	args := PutAppendArgs{
		Key:       key,
		Value:     value,
		RequestID: ck.requestId, // 一次client只调用一次Clerk
	}
	reply := PutAppendReply{}
	// 一直循环请求
	for {
		ok := ck.server.Call("KVServer."+op, &args, &reply)
		if ok {
			ck.requestId++
			return reply.Value
		} else {
			DPrintf("[Client] Call ***(%v) failed, retrying...", args)
			time.Sleep(100 * time.Millisecond) // 添加100毫秒的休眠时间
		}

	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}
