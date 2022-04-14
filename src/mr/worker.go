package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path"
	"sort"
	"time"
)

const RequestTaskInterval = time.Second * 2

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// Worker Struct
//
type WorkerBody struct {
	WorkerID int                             // WorkerID
	Task     *Task                           // 收到的任务
	mapf     func(string, string) []KeyValue // map方法
	reducef  func(string, []string) string   // reduce方法
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
	w := WorkerBody{}
	w.mapf = mapf
	w.reducef = reducef
	// 首先向coordinator申请注册
	if w.CallRegisteWorker() {
		// 不断向coordinator请求任务 直到无法请求
		for {
			if !w.runWorker() {
				break
			}
			time.Sleep(RequestTaskInterval)
		}

	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func (w *WorkerBody) runWorker() bool {
	// 1. 请求任务
	// 如果请求任务失败 则返回false
	// 成功请求到任务后 无论任务是否执行失败 都返回true
	// 2. 执行任务
	// 3. 执行结束后 报告执行情况
	if w.RequestTask() {
		taskSuccess := true
		if !w.ProcessTask() {
			taskSuccess = false
		}
		w.ReportTasktoCoordinator(taskSuccess)
		return true
	}
	return false
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
		//fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		//fmt.Printf("call failed!\n")
	}
}

//
// 向coordinator请求注册 RPC: Coordinator.RegisteWorker
//
func (w *WorkerBody) CallRegisteWorker() bool {
	args := RequestArgs{}
	reply := RespondBody{}
	//fmt.Printf("CallRegisteWorker Start!\n")
	ok := call("Coordinator.RegisteWorker", &args, &reply)
	if ok {
		w.WorkerID = reply.WorkerID // 赋予WorkerID
		//fmt.Printf("CallRegisteWorker Succeeded! WorkerID %v\n", w.WorkerID)
	} else {
		//fmt.Printf("CallRegisteWorker failed!\n")
	}
	return ok
}

//
// 向coordinator请求任务 RPC: Coordinator.GetTask
//
func (w *WorkerBody) RequestTask() bool {
	args := RequestArgs{}
	reply := RespondBody{}
	//fmt.Printf("RequestTask Start!\n")
	args.WorkerID = w.WorkerID // 声明WorkerID
	ok := call("Coordinator.GetTask", &args, &reply)
	if ok {
		w.Task = reply.Task // 接收Task
		//fmt.Printf("RequestTask Succeeded! Task %v\n", w.Task)
	} else {
		//fmt.Printf("RequestTask failed!\n")
	}
	return ok
}

//
// 处理Task任务
//
func (w *WorkerBody) ProcessTask() bool {
	//fmt.Printf("ProcessTask Start!\n")
	if w.Task.Type == TaskTypeMap {
		// Map任务处理
		fileName := w.Task.FileName
		file, err := os.Open(fileName)
		if err != nil {
			//fmt.Printf("cannot open %v\n", fileName)
			return false
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			//fmt.Printf("cannot read %v\n", fileName)
			return false
		}
		file.Close()
		mapOupputTemp := make([]*os.File, w.Task.Reducen) // 临时文件
		josnEncs := make([]*json.Encoder, w.Task.Reducen) // 存储为JSON格式
		for i := 0; i < w.Task.Reducen; i++ {
			tmpFile, err := ioutil.TempFile("", "mr-intermediate-*.txt") // 创建临时文件
			if err != nil {
				//fmt.Printf("Cannot create temporary file %v\n", err)
				return false
			}
			mapOupputTemp[i] = tmpFile
			josnEncs[i] = json.NewEncoder(tmpFile)
			// mapOupputTemp[i], _ = os.Create(fmt.Sprintf("mr-mh-%d-%d", w.Task.TaskID, i))
		}
		kva := w.mapf(fileName, string(content)) // Map函数处理
		for _, item := range kva {
			//fmt.Fprintf(mapOupputTemp[ihash(item.Key)%w.Task.Reducen], "%v %v\n", item.Key, item.Value)
			err := josnEncs[ihash(item.Key)%w.Task.Reducen].Encode(&item) // ihash(item.Key)%w.Task.Reducen 为接下来指定Reduce任务准备
			if err != nil {
				//fmt.Printf("Cannot Write JSON data to file %v\n", err)
				return false
			}
		}
		for i := 0; i < w.Task.Reducen; i++ {
			newDir, _ := os.Getwd()
			newDir = path.Join(newDir, fmt.Sprintf("mr-mh-%d-%d", w.Task.TaskID, i)) // 中间文件命名格式 mr-X-Y X:第X个Map任务 Y:第Y个Reduce任务
			os.Rename(mapOupputTemp[i].Name(), newDir)                               // 确定处理完毕后 再把临时文件变为中间文件
			mapOupputTemp[i].Close()
			//fmt.Printf("TempFile %v To %v\n", mapOupputTemp[i].Name(), newDir)
		}
		//fmt.Printf("Process MapTask Succeeded! TaskID %v\n", w.Task.TaskID)
		return true
	} else if w.Task.Type == TaskTypeReduce {
		// Reduce任务处理
		intermediate := []KeyValue{} // 存储读取结果
		reduceId := w.Task.TaskID
		// 读取所有mr-*-reduceId文件
		for index := 0; index < w.Task.Mapn; index++ {
			fileName := fmt.Sprintf("mr-mh-%d-%d", index, reduceId)
			file, err := os.Open(fileName)
			if err != nil {
				//fmt.Printf("Cannot open file %v\n", err)
				return false
			}
			dec := json.NewDecoder(file)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				intermediate = append(intermediate, kv)
			}
			sort.Sort(ByKey(intermediate)) // 根据key排序中间键
		}
		tmpFile, err := ioutil.TempFile("", "mr-out-*.txt")
		if err != nil {
			//fmt.Printf("Cannot create temporary file %v\n", err)
			return false
		}
		i := 0
		for i < len(intermediate) {
			j := i + 1
			for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, intermediate[k].Value)
			} // 将所有key相同的中间键添加进values中
			output := w.reducef(intermediate[i].Key, values) // Reduce函数

			// this is the correct format for each line of Reduce output.
			fmt.Fprintf(tmpFile, "%v %v\n", intermediate[i].Key, output)

			i = j
		}
		newDir, _ := os.Getwd()
		newDir = path.Join(newDir, fmt.Sprintf("mr-out-%d", reduceId))
		os.Rename(tmpFile.Name(), newDir)
		//fmt.Printf("TempFile %v To %v\n", tmpFile.Name(), newDir)
		//fmt.Printf("Process ReduceTask Succeeded! TaskID %v\n", w.Task.TaskID)
		return true
	}
	//fmt.Printf("ProcessTask Failed! TaskID %v\n", w.Task.TaskID)
	return false
}

//
// 向coordinator报告任务 RPC: Coordinator.ReportTask
//
func (w *WorkerBody) ReportTasktoCoordinator(isSuccess bool) bool {
	args := ReportArgs{}
	reply := RespondBody{}
	//fmt.Printf("ReportTasktoCoordinator Start!\n")
	args.WorkerID = w.WorkerID
	args.Task = w.Task
	args.TaskSuccess = isSuccess

	ok := call("Coordinator.ReportTask", &args, &reply)
	if ok {
		w.Task = nil
		//fmt.Printf("ReportTasktoCoordinator Succeeded!\n")
	} else {
		//fmt.Printf("RequestTask failed!\n")
	}
	return ok
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
