package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type TaskType int
type WorkState int // worker的工作状态
type GlobalState int

const (
	MapWork GlobalState = iota
	MapDone
	ReduceWork
	ReduceDone
	AllDone
)

const (
	MapTask TaskType = iota
	ReduceTask
	WaitTask
	DoneTask
)

const (
	Free WorkState = iota
	Working
)

type MapRpcArgs struct{}

type MapRpcReply struct {
	FileName  string
	WorkerIdx int // Map函数或者reduce函数的序号
	NReduce   int // reduce worker的数量
}

type ReduceRpcArgs struct{}

type ReduceRpcReply struct {
	FileName  string
	WorkerIdx int // Map函数或者reduce函数的序号
	NReduce   int // reduce worker的数量
	NMap      int
}

type GetTaskArgs struct{}

type GetTaskReply struct {
	TaskType TaskType
}

type EndTaskArgs struct {
	WorkIdx int // Map函数或者reduce函数的序号
}

type EndTaskReply struct {
	TaskType TaskType
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
