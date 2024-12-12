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

type RpcArgs struct {
	WorkerIdx int  	// worker的编号
}

type RpcReply struct {
	TaskType string
	FileName string
	Idx	int			// Map函数或者reduce函数的序号
	NReduce int			// reduce worker的数量
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
