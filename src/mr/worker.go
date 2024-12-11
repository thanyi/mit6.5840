package mr

import (
"encoding/json"
"fmt"
"io/ioutil"
"os""sort"

)
import "log"
import "net/rpc"
import "hash/fnv"

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	intermediate := []KeyValue{}	// 中间输出
	args := new(RpcArgs)
	reply := new(RpcReply)

	filename, content := workformap(args, reply, mapf)

	// 在这里调用map函数
	// kva是[]mr.KeyValue{}，包含很多KeyValue{}结构
	kva := mapf(filename, content)
	// 将kva的每一项放入intermediate中
	intermediate = append(intermediate, kva...)
	sort.Sort(ByKey(intermediate)) // 根据key进行Sort
	// TODO: 使用hashkey对reduce进行分配
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}

		i = j
	}






	// 创建中间文件进行存储
	// TODO： Worker的序号怎么获取
	enc := json.NewEncoder(file)	// file名是mr-X-Y结构
	  for _, kv := range intermediate {
		err := enc.Encode(&kv)
		if err != nil {
			log.Fatalf("cannot write intermediate files!")
		}
	  }


	// uncomment to send the Example RPC to the coordinator.
	//CallExample()
}

// 向coordinator提交请求，coordinator返回filename
// return：filename以及content
func workformap(args *RpcArgs, reply *RpcReply,
				mapf func(string, string) []KeyValue) (string, string) {
	ok := call("Coordinator.AssignMap", &args, &reply)
	var filename string
	if ok {
		filename = reply.FileName		// 拿到了当前任务的file
	}else {
		fmt.Printf("call failed!\n")
	}

	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file) // 将文件内容传入content
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	return filename ,string(content)
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
