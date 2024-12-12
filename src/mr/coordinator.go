package mr

import (

"fmt"
"log"
"strconv"
"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"


type Coordinator struct {
	// Your definitions here.
	FileAllocatedMap map[string]bool   // 表示是否此文件语句被分配
	nReduce int				// 表明reduce worker的数目
	mutx    sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
// 进行任务分配，当Worker进行请求的时候，返回没有进行任务的File
func (c *Coordinator) AssignTask(args *RpcArgs, reply *RpcReply) error {

	// 查看是Map请求还是Reduce请求
	// 当请求表明想要进行Task的获取， 就找一个可以进行任务的file进行返回
	fmt.Println("start assign to worker "+ strconv.Itoa(args.WorkerIdx))
	c.mutx.Lock()				// 加锁进行对filemap的修改
	defer c.mutx.Unlock()
	i := 0 		// 给map的序号
	for filename, isused := range c.FileAllocatedMap{
		if !isused {
			fmt.Println("AssignTask:", filename)
			reply.TaskType = "map"
			reply.FileName = filename
			reply.NReduce = c.nReduce
			reply.Idx = i
			c.FileAllocatedMap[filename] = true
			break
		}
		i++		// map序号+1
	}

	return nil
}




//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here. TODO


	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	fmt.Println("Make coordinator...")
	c.nReduce = nReduce
	c.FileAllocatedMap = make(map[string]bool)
	// 初始化操作，每一个filename对应一个是否被分配任务的标志位
	for _, file := range files {
		c.FileAllocatedMap[file] = false
	}
	fmt.Println("Allocated FileAllocatedMap")
	fmt.Println("start server..")
	c.server()
	return &c
}
