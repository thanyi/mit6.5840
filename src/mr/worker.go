package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		args := new(MsgSend)
		reply := new(MsgReply)

		ok := call("Coordinator.RequestTask", &args, &reply)
		if !ok {
			fmt.Printf("RequestTask call failed!\n")
		}
		switch reply.TaskType {
		case MapTask:
			workMapTask(mapf, reply.FileName, reply.NReduce, reply.TaskId)
		case ReduceTask:
			workReduceTask(reducef, reply.FileName, reply.NMap, reply.TaskId)
		case WaitTask:
			time.Sleep(1 * time.Second)
		case DoneTask:
			os.Exit(0)
		}

		time.Sleep(1 * time.Second)
	}
}

// 向coordinator提交请求，coordinator返回filename
// worker将返回的key-value保存进tmp文件
func workMapTask(mapf func(string, string) []KeyValue, filename string,
	nReduce int, taskId int) {
	// 获取content
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := io.ReadAll(file) // 将文件内容传入content
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	// 调用map函数
	kva := mapf(filename, string(content)) // kva是[]mr.KeyValue{}，包含很多KeyValue{}结构

	intermediate := make([][]KeyValue, nReduce)
	for _, kv := range kva {
		r := ihash(kv.Key) % nReduce
		intermediate[r] = append(intermediate[r], kv)
	}

	// write the intermediate files
	for r, kva := range intermediate {
		oname := fmt.Sprintf("mr-%v-%v", taskId, r)
		ofile, err := os.CreateTemp(".", oname)
		if err != nil {
			log.Fatalf("cannot create tempfile %v", oname)
		}
		enc := json.NewEncoder(ofile)
		for _, kv := range kva {
			// write the key-value pairs to the intermediate file
			enc.Encode(kv)
		}
		ofile.Close()
		// Atomic file renaming：rename the tempfile to the final intermediate file
		os.Rename(ofile.Name(), oname)
	}

	endArgs := MsgSend{
		TaskId:    taskId,
		WorkState: MapCompleted,
	}
	endReply := MsgReply{}
	endOk := call("Coordinator.WorkDoneTask", &endArgs, &endReply) // 表明任务结束
	if !endOk {
		fmt.Printf("EndTask call failed!\n")
	}
}

func workReduceTask(reducef func(string, []string) string, filename string,
	nMap int, taskId int) {
	var interFileName string
	kva := []KeyValue{}
	// 将同一个reduceIdx下的所有文件读取
	for i := 0; i < nMap; i++ {
		interFileName = fmt.Sprintf("mr-%d-%d", i, taskId)
		// 追加模式打开文件
		interFile, err := os.Open(interFileName) // 使用只读模式

		if err != nil {
			log.Fatalf("cannot open %v", interFileName)
		}
		dec := json.NewDecoder(interFile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		interFile.Close()
	}
	sort.Sort(ByKey(kva)) // 根据key进行Sort

	oname := fmt.Sprintf("mr-out-%v", taskId)
	ofile, _ := os.Create(oname)
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value) // 将value全部放在一个values切片中
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	ofile.Close()

	endArgs := MsgSend{
		TaskId:    taskId,
		WorkState: ReduceCompleted,
	}
	endReply := MsgReply{}
	endOk := call("Coordinator.WorkDoneTask", &endArgs, &endReply) // 表明任务结束
	if !endOk {
		fmt.Printf("EndTask call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

/* --------------------------------- 以下是废案 -------------------------------------*/

// main/mrworker.go calls this function.
//func Worker(mapf func(string, string) []KeyValue,
//	reducef func(string, []string) string) {
//	// Your worker implementation here.
//	for {
//		taskType := GetTask()
//		if taskType == MapTask {
//			workMapTask(mapf)
//		} else if taskType == ReduceTask {
//			workReduceTask(reducef)
//		} else if taskType == DoneTask {
//			os.Exit(0)
//		} else if taskType == WaitTask {
//			time.Sleep(time.Second)
//		}
//		time.Sleep(time.Second)
//	}
//
//}
//
//func GetTask() TaskType {
//	args := new(GetTaskArgs)
//	reply := new(GetTaskReply)
//
//	ok := call("Coordinator.GetTask", &args, &reply)
//	if !ok {
//		fmt.Printf("GetTask call failed!\n")
//	}
//	return reply.TaskType
//}
//
//// 向coordinator提交请求，coordinator返回filename
//// worker将返回的key-value保存进tmp文件
//func workMapTask(mapf func(string, string) []KeyValue) {
//	args := new(MsgSend)
//	reply := new(MsgReply)
//	ok := call("Coordinator.AssignMapTask", &args, &reply)
//	if !ok {
//		fmt.Printf("AssignTask call failed!\n")
//	}
//	// 拿到了当前任务的相关参数
//	filename := reply.FileName
//	mapIdx := reply.WorkerIdx
//	nReduce := reply.NReduce
//	// 获取content
//	file, err := os.Open(filename)
//	if err != nil {
//		log.Fatalf("cannot open %v", filename)
//	}
//	content, err := ioutil.ReadAll(file) // 将文件内容传入content
//	if err != nil {
//		log.Fatalf("cannot read %v", filename)
//	}
//	file.Close()
//
//	// 调用map函数
//	kva := mapf(filename, string(content))      // kva是[]mr.KeyValue{}，包含很多KeyValue{}结构
//	intermediate := make([][]KeyValue, nReduce)             // 中间输出
//
//	for _, kv := range kva {
//		//使用hashkey对reduce进行分配
//		r := ihash(kv.Key) % nReduce // reduce的编号
//		intermediate[r] = append(intermediate[r], kv)
//	}
//
//	for r, v := range intermediate {
//		var intermediateFileName = fmt.Sprintf("mr-%v-%v", mapIdx, r)
//
//		intermediateFile, err := os.CreateTemp(".", intermediateFileName)
//		if err != nil {
//			log.Fatalf("cannot open %v", intermediateFile)
//		}
//		// 创建中间文件进行存储
//		enc := json.NewEncoder(intermediateFile) // file名是mr-X-Y结构
//		for _, kv := range v {   // 一次针对一个key
//			err := enc.Encode(&kv)
//			if err != nil {
//				log.Fatalf("cannot write intermediate files!")
//			}
//		}
//
//		intermediateFile.Close()
//		os.Rename(intermediateFile.Name(), intermediateFileName)
//	}
//
//
//	endArgs := new(EndTaskArgs)
//	endArgs.WorkIdx = mapIdx
//	endReply := new(EndTaskReply)
//	endOk := call("Coordinator.EndTask", &endArgs, &endReply) // 表明任务结束
//	if !endOk {
//		fmt.Printf("EndTask call failed!\n")
//	}
//}
//
//func workReduceTask(reducef func(string, []string) string) {
//
//	args := new(MsgArgs)
//	reply := new(MsgReply)
//	ok := call("Coordinator.AssignReduceTask", &args, &reply)
//	if !ok {
//		fmt.Printf("AssignReduceTask call failed!\n")
//	}
//	// 拿到了当前任务的相关参数
//	ReduceIdx := reply.WorkerIdx
//	nMap := reply.NMap
//
//	var interFileName string
//	kva := []KeyValue{}
//	// 将同一个reduceIdx下的所有文件读取
//	for i := 0; i < nMap; i++ {
//		interFileName = fmt.Sprintf("mr-%d-%d", i, ReduceIdx)
//		// 打开文件
//		interFile, err := os.Open(interFileName)
//		if err != nil {
//			log.Fatalf("cannot open %v", interFileName)
//		}
//		dec := json.NewDecoder(interFile)
//		for {
//			var kv KeyValue
//			if err := dec.Decode(&kv); err != nil {
//				break
//			}
//			kva = append(kva, kv)
//		}
//		interFile.Close()
//	}
//	sort.Sort(ByKey(kva)) // 根据key进行Sort
//
//	oname := fmt.Sprintf("mr-out-%d", ReduceIdx)
//	ofile, _ := os.Create(oname)
//	i := 0
//	for i < len(kva) {
//		j := i + 1
//		for j < len(kva) && kva[j].Key == kva[i].Key {
//			j++
//		}
//		values := []string{}
//		for k := i; k < j; k++ {
//			values = append(values, kva[k].Value) // 将value全部放在一个values切片中
//		}
//		output := reducef(kva[i].Key, values)
//
//		// this is the correct format for each line of Reduce output.
//		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
//		i = j
//	}
//
//	ofile.Close()
//
//	endArgs := new(EndTaskArgs)
//	endArgs.WorkIdx = ReduceIdx
//	endReply := new(EndTaskReply)
//	endOk := call("Coordinator.EndTask", &endArgs, &endReply) // 表明任务结束
//	if !endOk {
//		fmt.Printf("EndTask call failed!\n")
//	}
//}
//
//// example function to show how to make an RPC call to the coordinator.
////
//// the RPC argument and reply types are defined in rpc.go.
//func CallExample() {
//
//	// declare an argument structure.
//	args := ExampleArgs{}
//
//	// fill in the argument(s).
//	args.X = 99
//
//	// declare a reply structure.
//	reply := ExampleReply{}
//
//	// send the RPC request, wait for the reply.
//	// the "Coordinator.Example" tells the
//	// receiving server that we'd like to call
//	// the Example() method of struct Coordinator.
//	ok := call("Coordinator.Example", &args, &reply)
//	if ok {
//		// reply.Y should be 100.
//		fmt.Printf("reply.Y %v\n", reply.Y)
//	} else {
//		fmt.Printf("call failed!\n")
//	}
//}
//
//// send an RPC request to the coordinator, wait for the response.
//// usually returns true.
//// returns false if something goes wrong.
//func call(rpcname string, args interface{}, reply interface{}) bool {
//	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
//	sockname := coordinatorSock()
//	c, err := rpc.DialHTTP("unix", sockname)
//	if err != nil {
//		log.Fatal("dialing:", err)
//	}
//	defer c.Close()
//
//	err = c.Call(rpcname, args, reply)
//	if err == nil {
//		return true
//	}
//
//	fmt.Println(err)
//	return false
//}
