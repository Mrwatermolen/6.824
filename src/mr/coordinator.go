package mr

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	// 定义任务状态
	TaskStatusUnassigned = iota // 0 任务未进入任务通道中
	TaskStatusQueue             // 处于任务队列通道中
	TaskStatusProcessing        // worker处理中
	TaskStatusFinished          // 任务结束
	TaskStatusFailed            // 4 任务失败
)

const (
	// 定义任务类型
	TaskTypeMap    = iota // 0 任务类型为Map
	TaskTypeReduce        // 1 任务类型为Reduce
)

const (
	// 定义时间
	MaxRunningTime     = time.Second * 10       // 最大运行时间
	UpdateIntervalTime = time.Millisecond * 200 // 更新时间
)

const (
	// 定义Worker状态
	WorkerStatusUnregisted = iota // 0 未注册
	WorkerStatusFree              // Worker空闲
	WorkerStatusRunning           // 处理Task中
	WorkerStatusFailed            // 3 Worker出现故障
)

type Coordinator struct {
	// Your definitions here.
	files        []string      // 接收Map的输入文件
	TaskMonitors []TaskMonitor // 任务状态
	nReduce      int           // Reduce任务个数
	nMap         int           // Map任务个数
	CurrentType  int           // 当前Task类型
	TaskSqNum    int           // chan里的任务数量
	WorkerNum    int           // 已经注册Worker的数量
	MapperRecord map[int]int   // 记录已经注册Worker的状态
	mu           sync.Mutex    // 互斥锁
	TaskCh       chan *Task    // 任务通道
	finished     bool          // 所有任务已经结束的标志
}

// 在RPC传递中首字母要大写
type Task struct {
	TaskID   int    // 任务ID
	FileName string // 处理文件名
	Type     int    // 任务类型
	Reducen  int    // Reduce任务个数 RPC中首字母大写 否则会重置
	Mapn     int    // Map任务个数 RPC中首字母大写 否则会重置
}

type TaskMonitor struct {
	WorkerID  int       // 当前正在处理该任务的WorkerID 0为未分配Worker
	Status    int       // 任务状态
	Task      Task      // 任务
	StartTime time.Time // 任务开始时间
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) Update() {
	c.mu.Lock()
	defer c.mu.Unlock()

	phaseFinish := true // Map or Reduce任务是否全部完成
	// 监控Task状态
	for index, task := range c.TaskMonitors {
		if task.Status == TaskStatusUnassigned {
			// 任务未被分配 产生一个全新的任务
			phaseFinish = false
			c.TaskMonitors[index].Status = TaskStatusQueue
			c.TaskMonitors[index].WorkerID = 0
			c.TaskMonitors[index].Task = c.GenerateTask(index)
			c.TaskCh <- &c.TaskMonitors[index].Task // 将任务加入任务队列中
			c.TaskSqNum += 1
			//fmt.Printf("TaskID: %v creates successfully and enters TaskCh \n", index)
		} else if task.Status == TaskStatusQueue {
			phaseFinish = false
			//fmt.Printf("TaskID: %v still is in TaskCh \n", index)
		} else if task.Status == TaskStatusProcessing {
			// 任务仍在Worker处理中 判断是否超时
			phaseFinish = false
			//fmt.Printf("TaskID: %v status is Processing in WorkerID %v. Check for timeout\n", index, task.WorkerID)
			runningTime := time.Since(c.TaskMonitors[index].StartTime)
			if runningTime > MaxRunningTime {
				// 超时处理
				//fmt.Printf("TaskID: %v status is Processing in WorkerID %v. Timeout!\n", index, task.WorkerID)
				c.MapperRecord[c.TaskMonitors[index].WorkerID] = WorkerStatusFailed // 设置对应Worker状态为Failed
				c.TaskMonitors[index].Status = TaskStatusQueue                      // 重新重置任务进入任务队列
				c.TaskCh <- &c.TaskMonitors[index].Task
				c.TaskSqNum += 1
			}
		} else if task.Status == TaskStatusFailed {
			// 任务处理失败 重新放入任务队列中
			phaseFinish = false
			c.TaskMonitors[index].Status = TaskStatusQueue
			c.TaskCh <- &c.TaskMonitors[index].Task
			c.TaskSqNum += 1
		}
	}
	// 判断当前阶段任务是否全部处理结束
	if phaseFinish {
		if c.CurrentType == TaskTypeMap {
			// 进入下一阶段 Reduce
			//fmt.Printf("TaskTypeMap is Finished. Start Reduce\n")
			c.InitRuduceTasks()
		} else if c.CurrentType == TaskTypeReduce {
			// 全部结束
			//fmt.Printf("All Done!\n")
			c.finished = true
		}
	}

}

//
// 产生一个任务
// 仅Update()能够调用
//
func (c *Coordinator) GenerateTask(taskID int) Task {
	newTask := Task{}
	newTask.TaskID = taskID
	newTask.Type = c.CurrentType
	newTask.Mapn = c.nMap
	newTask.Reducen = c.nReduce
	if c.CurrentType == TaskTypeMap {
		newTask.FileName = c.files[taskID]
	} else {
		newTask.FileName = ""
	}
	return newTask
}

//
// Worker RPC
// Worker注册
//
func (c *Coordinator) RegisteWorker(args *RequestArgs, reply *RespondBody) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.WorkerNum += 1 // 第一台WorkerID为1 依次递增
	reply.WorkerID = c.WorkerNum
	c.MapperRecord[c.WorkerNum] = WorkerStatusFree // 记录Worker
	return nil
}

//
// Worker RPC
// Worker请求分配Task
//
func (c *Coordinator) GetTask(args *RequestArgs, reply *RespondBody) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.TaskSqNum <= 0 {
		// 任务队列为空 Worker应该继续等待 不能读取chan 防止阻塞
		reply.WorkerID = -1 // 这里直接返回-1的WorkerID做了约定 返回-1 允许Worker继续工作
		// return errors.New("Worker ID: " + fmt.Sprintf("%d", args.WorkerID) + " GetTask. Chan is empty")
		return nil
	}
	checkStatus, ok := c.MapperRecord[args.WorkerID]
	if !ok {
		// Worker未注册
		return errors.New("Worker ID: " + fmt.Sprintf("%d", args.WorkerID) + " GetTask Worker Record Error")
	}
	if checkStatus != WorkerStatusFree {
		// 非空闲状态无法申请
		return errors.New("Worker ID: " + fmt.Sprintf("%d", args.WorkerID) + " GetTask Worker Status Error! Your status is " + fmt.Sprintf("%d", checkStatus))
	}
	reply.Task = <-c.TaskCh // 回复任务
	// 更新任务状态
	c.TaskMonitors[reply.Task.TaskID].WorkerID = args.WorkerID
	c.TaskMonitors[reply.Task.TaskID].Status = TaskStatusProcessing
	c.TaskMonitors[reply.Task.TaskID].StartTime = time.Now()
	// 更新Worker状态
	c.MapperRecord[args.WorkerID] = WorkerStatusRunning
	c.TaskSqNum -= 1
	//fmt.Printf("WorkerID %v gets TaskID %v.\n", args.WorkerID, reply.Task)
	return nil
}

//
// Worker RPC
// Worker完成Task后报告
//
func (c *Coordinator) ReportTask(args *ReportArgs, reply *RespondBody) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	checkStatus, ok := c.MapperRecord[args.WorkerID]
	if !ok {
		// Worker未注册
		return errors.New("Worker ID: " + fmt.Sprintf("%d", args.WorkerID) + " ReportTask Worker Record Error")
	}
	if checkStatus == WorkerStatusUnregisted || checkStatus == WorkerStatusFailed {
		// 确认Worker状态是否符合Coordinator记录
		reply.WorkerID = -1 // 返回特征值
		return errors.New("Worker ID: " + fmt.Sprintf("%d", args.WorkerID) + " ReportTask Worker Status Error " + fmt.Sprintf("%d", checkStatus))
	}
	if args.TaskSuccess {
		// Worker成功完成Task
		c.TaskMonitors[args.Task.TaskID].Status = TaskStatusFinished
		c.MapperRecord[args.WorkerID] = WorkerStatusFree
	} else {
		// Worker未完成Task
		c.TaskMonitors[args.Task.TaskID].Status = TaskStatusFailed
		c.MapperRecord[args.WorkerID] = WorkerStatusFree
	}
	//fmt.Printf(" workerID %v Report Task %v TaskStatus %v\n", args.WorkerID, args.Task, c.TaskMonitors[args.Task.TaskID].Status)
	return nil
}

//
// 定时更新状态
//
func (c *Coordinator) UpdateTimer() {
	for {
		if c.Done() {
			break
		}
		go c.Update()
		time.Sleep(UpdateIntervalTime)
	}
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
// 开始Map前 开始初始化
//
func (c *Coordinator) InitMapTasks() {
	c.TaskSqNum = 0
	c.CurrentType = TaskTypeMap
	c.TaskMonitors = make([]TaskMonitor, c.nMap)
}

//
// 开始Reduce前 开始初始化
//
func (c *Coordinator) InitRuduceTasks() {
	//fmt.Printf("InitRuduceTasks Start\n")
	c.TaskSqNum = 0
	c.CurrentType = TaskTypeReduce
	c.TaskMonitors = make([]TaskMonitor, c.nReduce)
	//fmt.Printf("InitRuduceTasks End\n")
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	ret := false
	// Your code here.
	ret = c.finished
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
	c.finished = false
	c.nMap = len(files)
	c.nReduce = nReduce
	if c.nMap > c.nReduce {
		c.TaskCh = make(chan *Task, c.nMap)
	} else {
		c.TaskCh = make(chan *Task, c.nReduce)
	}
	c.files = files
	c.MapperRecord = make(map[int]int, 0)
	c.InitMapTasks()
	c.server()
	go c.UpdateTimer()
	return &c
}
