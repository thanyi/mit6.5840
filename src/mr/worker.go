package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"sync"
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
	var wg sync.WaitGroup
	/* 创建10个worker */
	for i := 0; i < 10; i++ {
		fmt.Println("start worker" + strconv.Itoa(i))
		wg.Add(1)
		// 开启一道线程
		go func(idx int) {
			defer wg.Done()
			args := new(RpcArgs)
			reply := new(RpcReply)

			ok := call("Coordinator.AssignTask", &args, &reply)
			if !ok {
				fmt.Printf("call failed!\n")
			}
			if reply.TaskType == "map" {
				// 拿到了当前任务的相关参数
				filename := reply.FileName
				mapIdx := reply.Idx
				nReduce := reply.NReduce

				// 获取content
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open %v", filename)
				}
				content, err := ioutil.ReadAll(file) // 将文件内容传入content
				if err != nil {
					log.Fatalf("cannot read %v", filename)
				}
				file.Close()

				workInMap(filename, string(content), mapIdx, nReduce, mapf)
			}
			fmt.Println("end worker"+ strconv.Itoa(reply.Idx))
		}(i)
	}
	wg.Wait()

	// uncomment to send the Example RPC to the coordinator.
	//CallExample()
}

// 向coordinator提交请求，coordinator返回filename
// worker将返回的key-value保存进tmp文件
// return：filename以及content
func workInMap(filename string, content string,
				mapIdx int, nReduce int,
				mapf func(string, string) []KeyValue) {


	// 调用map函数
	kva := mapf(filename, string(content))	// kva是[]mr.KeyValue{}，包含很多KeyValue{}结构
	intermediate := []KeyValue{}	// 中间输出
	intermediate = append(intermediate, kva...)	// 将kva的每一项放入intermediate中
	sort.Sort(ByKey(intermediate)) // 根据key进行Sort

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		//使用hashkey对reduce进行分配
		reduceNum := ihash(intermediate[i].Key)	% nReduce	// reduce的编号
		var intermediateFileName = "mr_" + strconv.Itoa(mapIdx) + "_" + strconv.Itoa(reduceNum)

		intermediateFile, err := os.OpenFile(intermediateFileName,
											 os.O_APPEND|os.O_CREATE|os.O_RDWR,
											0644)
		defer intermediateFile.Close()
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		// 创建中间文件进行存储
		enc := json.NewEncoder(intermediateFile)	// file名是mr-X-Y结构
		for _, kv := range intermediate[i:j] {		// 一次针对一个key
		err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("cannot write intermediate files!")
			}
		}
		i = j
	}


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
