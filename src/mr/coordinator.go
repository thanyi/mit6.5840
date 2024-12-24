package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type GlobalStatus int

type TaskStatus int // 描述任务状态

const (
	UnAssigned TaskStatus = iota
	Assigned
	Failed
	Completed
)

type TaskInfo struct {
	TaskStatus TaskStatus
	FileName   string
	TimeStamp  time.Time // 表示时间戳
}
type Coordinator struct {
	nReduce              int // 表明reduce worker的数目
	nMap                 int
	mutx                 sync.Mutex
	MapTaskInfos         []TaskInfo
	ReduceTaskInfos      []TaskInfo
	IsAllMapCompleted    bool
	IsAllReduceCompleted bool
	NMapAssigned         int
	NReduceAssigned      int
}

// 作为Coordinator的主要函数
func (c *Coordinator) RequestTask(args *MsgSend, reply *MsgReply) error {
	c.mutx.Lock()
	defer c.mutx.Unlock()

	/* 首先分析维护的Map任务列表是否还存在可分Map任务 */
	if !c.IsAllMapCompleted {
		NMapAssigned := 0 // 初始化，不然每次申请计数就变大
		// 若是还有任务，则进行划分
		for idx, taskInfo := range c.MapTaskInfos {
			if taskInfo.TaskStatus == UnAssigned || taskInfo.TaskStatus == Failed ||
				(taskInfo.TaskStatus == Assigned && time.Since(taskInfo.TimeStamp) >= 10*time.Second) {
				// Reply值的赋予
				reply.NReduce = c.nReduce
				reply.TaskId = idx // 将idx作为返回值中的TaskId
				reply.TaskType = MapTask
				reply.FileName = taskInfo.FileName
				reply.NMap = c.nMap

				c.MapTaskInfos[idx].TaskStatus = Assigned
				c.MapTaskInfos[idx].TimeStamp = time.Now()
				return nil
			} else if taskInfo.TaskStatus == Completed {
				NMapAssigned++
			}
		}

		if NMapAssigned == len(c.MapTaskInfos) {
			c.IsAllMapCompleted = true
		} else {
			reply.TaskType = WaitTask
			return nil
		}

	}

	if !c.IsAllReduceCompleted {
		NReduceAssigned := 0
		// 若是还有任务，则进行划分
		for idx, taskInfo := range c.ReduceTaskInfos {
			if taskInfo.TaskStatus == UnAssigned || taskInfo.TaskStatus == Failed ||
				(taskInfo.TaskStatus == Assigned && time.Since(taskInfo.TimeStamp) >= 10*time.Second) {
				// Reply值的赋予
				reply.NReduce = c.nReduce
				reply.TaskId = idx
				reply.TaskType = ReduceTask
				reply.FileName = taskInfo.FileName
				reply.NMap = c.nMap
				// Coordinator中的ReduceTaskInfos结构体数组的赋值
				c.ReduceTaskInfos[idx].TaskStatus = Assigned
				c.ReduceTaskInfos[idx].TimeStamp = time.Now()
				return nil
			} else if taskInfo.TaskStatus == Completed {
				NReduceAssigned++
			}
		}

		if NReduceAssigned == len(c.ReduceTaskInfos) {
			c.IsAllReduceCompleted = true
		} else {
			reply.TaskType = WaitTask
			return nil
		}

	}
	reply.TaskType = DoneTask
	return nil
}

func (c *Coordinator) WorkDoneTask(args *MsgSend, reply *MsgReply) error {
	c.mutx.Lock()
	defer c.mutx.Unlock()

	taskId := args.TaskId
	// 根据worker的返回更新结构体数组的状态
	switch args.WorkState {
	case MapCompleted:
		c.MapTaskInfos[taskId].TaskStatus = Completed
	case MapFailed:
		c.MapTaskInfos[taskId].TaskStatus = Failed
	case ReduceCompleted:
		c.ReduceTaskInfos[taskId].TaskStatus = Completed
	case ReduceFailed:
		c.ReduceTaskInfos[taskId].TaskStatus = Failed
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false
	if c.IsAllMapCompleted && c.IsAllReduceCompleted {
		ret = true
	}
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nReduce:              nReduce,
		nMap:                 len(files),
		MapTaskInfos:         make([]TaskInfo, len(files)),
		ReduceTaskInfos:      make([]TaskInfo, nReduce),
		IsAllMapCompleted:    false,
		IsAllReduceCompleted: false,
	}
	c.initTask(files) // 初始化任务列表
	c.server()
	return &c
}

func (c *Coordinator) initTask(files []string) {
	// 初始化MapTaskInfos
	for idx, file := range files {
		c.MapTaskInfos[idx] = TaskInfo{
			TaskStatus: UnAssigned,
			FileName:   file,
			TimeStamp:  time.Now(),
		}
	}

	// 初始化ReduceTaskInfos
	for idx, _ := range c.ReduceTaskInfos {
		c.ReduceTaskInfos[idx] = TaskInfo{
			TaskStatus: UnAssigned,
			TimeStamp:  time.Now(),
		}
	}

}

/* --------------------------------- 以下是废案 -------------------------------------*/

//type Coordinator struct {
//	// Your definitions here.
//	nReduce                 int 			// 表明reduce worker的数目
//	mutx                    sync.Mutex
//	workerStateList         []WorkState     // 保存每个worker的状态的列表
//	MapFileAllocated        map[string]bool // 表示是否此文件已经被分配
//	MapFileNames            []string        // 存储的文件名(用于迭代）
//	GlobalTaskState         GlobalState     // Coordinator这边的全局任务状态
//	InterFileStateAllocated []bool          // 中间文件状态list，是否被分配
//}
//
//// Your code here -- RPC handlers for the worker to call.
//// 进行任务分配，当Worker进行请求的时候，返回没有进行任务的File
//func (c *Coordinator) AssignMapTask(args *MsgArgs, reply *MsgReply) error {
//	c.mutx.Lock() // 加锁进行对filemap的修改
//	defer c.mutx.Unlock()
//
//	// 当coodinator状态式MapWork的时候，分发Map任务
//	// 当请求表明想要进行Task的获取， 找一个未进行任务的file进行返回
//	for i, filename := range c.MapFileNames { // map的迭代顺序不一致，所以只能用slice
//		// 先执行这一步，不然后续会多遍历一次结果
//		if i == len(c.MapFileNames)-1 {
//			c.GlobalTaskState = MapDone // 任务全被分发出去，标记位就是MapDone
//		}
//		if !c.MapFileAllocated[filename] {
//			reply.FileName = filename
//			reply.NReduce = c.nReduce
//			reply.WorkerIdx = i
//			reply.TimeStamp = time.Now()
//
//			c.MapFileAllocated[filename] = true
//			c.workerStateList[i] = Working // 将worker标上序号
//			break
//		}
//	}
//	return nil
//}
//
//func (c *Coordinator) AssignReduceTask(args *MsgArgs, reply *MsgReply) error {
//
//	c.mutx.Lock() // 加锁进行修改
//	defer c.mutx.Unlock()
//	for i := 0; i < c.nReduce; i++ {
//		if i == c.nReduce-1 {
//			c.GlobalTaskState = ReduceDone // 任务全被分发出去，标记位就是ReduceDone
//		}
//
//		if !c.InterFileStateAllocated[i] { // 如果没有被分配
//			var interFileName string = "mr"
//			reply.FileName = interFileName
//			reply.NReduce = c.nReduce
//			reply.WorkerIdx = i
//			c.workerStateList[i] = Working 		// 将worker标上序号
//			reply.NMap = len(c.MapFileNames)
//			c.InterFileStateAllocated[i] = true // 第i位标志位设置为true
//			break
//		}
//	}
//	return nil
//
//}
//
//func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
//	c.mutx.Lock()
//	defer c.mutx.Unlock()
//	c.GlobalTaskState = checkGlobalState(c) // 如果是MapDone状态，同时任务都完成，则变为ReduceWork
//	switch c.GlobalTaskState {
//
//	case MapWork:
//		reply.TaskType = MapTask
//	case MapDone:
//		reply.TaskType = WaitTask
//	case ReduceWork:
//		reply.TaskType = ReduceTask
//	case ReduceDone:
//		reply.TaskType = WaitTask
//	case AllDone:
//		reply.TaskType = DoneTask
//	}
//
//	return nil
//}
//
//func (c *Coordinator) EndTask(args *EndTaskArgs, reply *EndTaskReply) error {
//	c.mutx.Lock()
//	defer c.mutx.Unlock()
//
//	c.workerStateList[args.WorkIdx] = Free
//	return nil
//}
//
//// 对coordinator的全局状态进行识别，同时开启下一个阶段
//func checkGlobalState(c *Coordinator) GlobalState {
//	if c.GlobalTaskState == MapDone {
//		for _, state := range c.workerStateList {
//			if state != Free {
//				return MapDone
//			}
//		}
//		// 发现全局状态是Reduce,先扩充workerStateList数量
//		for i := 0; i < c.nReduce; i++ {
//			if i > len(c.workerStateList)-1 {
//				c.workerStateList = append(c.workerStateList, Free)
//			}
//		}
//		return ReduceWork
//	} else if c.GlobalTaskState == ReduceDone {
//		for _, state := range c.workerStateList {
//			if state != Free {
//				return ReduceDone
//			}
//		}
//
//		return AllDone
//	}
//
//	return c.GlobalTaskState
//}
//
//// start a thread that listens for RPCs from worker.go
//func (c *Coordinator) server() {
//	rpc.Register(c)
//	rpc.HandleHTTP()
//	//l, e := net.Listen("tcp", ":1234")
//	sockname := coordinatorSock()
//	os.Remove(sockname)
//	l, e := net.Listen("unix", sockname)
//	if e != nil {
//		log.Fatal("listen error:", e)
//	}
//	go http.Serve(l, nil)
//}
//
//// main/mrcoordinator.go calls Done() periodically to find out
//// if the entire job has finished.
//func (c *Coordinator) Done() bool {
//	ret := false
//
//	// Your code here.
//	if c.GlobalTaskState == AllDone {
//		for _, state := range c.workerStateList {
//			if state != Free {
//				return false
//			}
//		}
//		ret = true
//		//fmt.Println("server done!!")
//	}
//
//	return ret
//}
//
//// create a Coordinator.
//// main/mrcoordinator.go calls this function.
//// nReduce is the number of reduce tasks to use.
//func MakeCoordinator(files []string, nReduce int) *Coordinator {
//	c := Coordinator{}
//
//	// Your code here.
//	c.nReduce = nReduce
//	c.MapFileNames = files
//	c.MapFileAllocated = make(map[string]bool, len(files))
//	c.InterFileStateAllocated = make([]bool, nReduce)
//	c.workerStateList = make([]WorkState, len(files))
//	c.GlobalTaskState = MapWork
//
//	// 初始化操作，每一个filename对应一个是否被分配任务的标志位
//	for i, file := range files {
//		c.MapFileAllocated[file] = false
//		c.InterFileStateAllocated[i] = false // 中间文件事例："mr_0_9" i值表示其中'9'
//	}
//
//	//fmt.Println("start server..")
//	c.server()
//	return &c
//}
