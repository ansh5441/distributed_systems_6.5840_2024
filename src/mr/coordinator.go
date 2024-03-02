package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// create a lock for task read and write
var taskLock = sync.Mutex{}

// Task states for map and reduce tasks
const (
	Pending int = iota
	InProgress
	Completed
)

type MapTask struct {
	FileName  string // file name of the map task
	State     int    // 0: pending, 1: in progress, 2: completed
	Num       int    // task number
	StartTime int64  // time when the task was assigned

	//
}

type ReduceTask struct {
	FileNames []string // file name of the reduce task
	State     int      // 0: pending, 1: in progress, 2: completed
	StartTime int64    // time when the task was assigned
	Num       int      // task number
}

type Coordinator struct {
	// Your definitions here.
	MapTasks    *[]MapTask
	ReduceTasks *[]ReduceTask
	nReduce     int
}

func (c *Coordinator) mapTaskDone(mapTaskNum int) {
	// mark the map task as done
	taskLock.Lock()

	(*c.MapTasks)[mapTaskNum].State = Completed
	// Add reduce tasks
	for i := 0; i < c.nReduce; i++ {
		filename := fmt.Sprintf("/tmp/mr-%d-%d.txt", mapTaskNum, i)
		if len(*c.ReduceTasks) <= i {
			*c.ReduceTasks = append(*c.ReduceTasks, ReduceTask{[]string{filename}, Pending, 0, i})
		} else {
			(*c.ReduceTasks)[i].FileNames = append((*c.ReduceTasks)[i].FileNames, filename)
		}
	}
	taskLock.Unlock()
	log.Printf("Map done: %v", mapTaskNum)
}

func (c *Coordinator) reduceTaskDone(reduceTaskNum int) {
	// mark the reduce task as done
	taskLock.Lock()
	(*c.ReduceTasks)[reduceTaskNum].State = Completed
	taskLock.Unlock()
	log.Printf("Reduce done: %v", reduceTaskNum)
}

func (c *Coordinator) assignMapTask(reply *Reply) bool {
	// assign a map task if there is any
	taskLock.Lock()
	pendingMapTask := -1
	for i, task := range *c.MapTasks {
		if task.State == Pending {
			pendingMapTask = i
		}
	}
	if pendingMapTask != -1 {
		reply.FileName = (*c.MapTasks)[pendingMapTask].FileName
		reply.Command = "map"
		reply.NReduce = c.nReduce
		reply.MapTaskNum = pendingMapTask
		(*c.MapTasks)[pendingMapTask].State = InProgress
		(*c.MapTasks)[pendingMapTask].StartTime = time.Now().Unix()
		log.Printf("Map task %d assigned", pendingMapTask)
		taskLock.Unlock()
		return true
	}
	taskLock.Unlock()
	return false
}

func (c *Coordinator) assignReduceTask(reply *Reply) bool {
	taskLock.Lock()
	pendingReduceTask := -1
	// assign a reduce task if there is any
	for i, task := range *c.ReduceTasks {
		if task.State == Pending {
			pendingReduceTask = i
		}
	}
	if pendingReduceTask != -1 {
		reply.FileNames = (*c.ReduceTasks)[pendingReduceTask].FileNames
		reply.Command = "reduce"
		reply.ReduceTaskNum = (*c.ReduceTasks)[pendingReduceTask].Num
		(*c.ReduceTasks)[pendingReduceTask].State = InProgress
		(*c.ReduceTasks)[pendingReduceTask].StartTime = time.Now().Unix()
		log.Printf("Reduce task %d assigned", pendingReduceTask)
		taskLock.Unlock()
		return true
	}

	taskLock.Unlock()
	return false
}

func (c *Coordinator) RPCHandler(args *Args, reply *Reply) error {
	switch args.Query {
	case "map done":
		c.mapTaskDone(args.MapTaskNum)
	case "reduce done":
		c.reduceTaskDone(args.ReduceTaskNum)
	case "give me a job":
		// assign a map task if there is any
		if c.assignMapTask(reply) {
			return nil
		}
		// no map task left to assign, but some map tasks are still in progress
		// so wait for them to complete and then assign a reduce task
		if !c.AllMapTasksDone() {
			reply.Command = "wait"
			log.Println("No map task available, wait")
			return nil
		}
		// assign a reduce task if there is any
		if c.assignReduceTask(reply) {
			return nil
		}
		// no reduce task left to assign, but some reduce tasks are still in progress
		// so wait for them to complete and then check again
		if !c.AllReduceTasksDone() {
			reply.Command = "wait"
			log.Println("No reduce task available, wait")
			return nil
		}
		// no map or reduce task left to assign
		reply.Command = "done"
	default:
		log.Println("Wrong query")
	}
	return nil
}

// check if all map tasks are done
func (c *Coordinator) AllMapTasksDone() bool {
	taskLock.Lock()
	for _, task := range *c.MapTasks {
		if task.State != Completed {
			taskLock.Unlock()
			return false
		}
	}
	taskLock.Unlock()
	return true
}

// check if all map tasks are done
func (c *Coordinator) AllReduceTasksDone() bool {
	taskLock.Lock()

	for _, task := range *c.ReduceTasks {
		if task.State != Completed {
			taskLock.Unlock()
			return false
		}
	}
	taskLock.Unlock()
	return true
}

func (c *Coordinator) PeriodicHealthCheck() {

	// check if any task is in progress for more than 10 seconds
	// if so, mark it as pending
	taskLock.Lock()
	defer taskLock.Unlock()
	pendingMapTasks := []int{}

	// check if any map task is in progress for more than 10 seconds
	for i, task := range *c.MapTasks {
		if task.State == InProgress && time.Now().Unix()-task.StartTime > 10 {
			pendingMapTasks = append(pendingMapTasks, i)
			log.Println("Map task ", i, " timed out")
		}
	}

	// mark the timed out tasks as pending
	for _, i := range pendingMapTasks {
		(*c.MapTasks)[i].State = Pending
	}

	// check if any reduce task is in progress for more than 10 seconds
	pendingReduceTasks := []int{}
	for i, task := range *c.ReduceTasks {
		// log.Println("Reduce task ", i, " state: ", task.State)
		if task.State == InProgress && time.Now().Unix()-task.StartTime > 10 {
			pendingReduceTasks = append(pendingReduceTasks, i)
			log.Println("Reduce task ", i, " timed out")
		}
	}

	// mark the timed out tasks as pending
	for _, i := range pendingReduceTasks {
		(*c.ReduceTasks)[i].State = Pending
	}
}

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
	taskLock.Lock()
	for _, task := range *c.MapTasks {
		if task.State != Completed {
			taskLock.Unlock()
			return false
		}
	}
	for _, task := range *c.ReduceTasks {
		if task.State != Completed {
			taskLock.Unlock()
			return false
		}
	}
	taskLock.Unlock()
	log.Println("All tasks done on coordinator. Coordinator says good bye")
	return true
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.nReduce = nReduce
	c.MapTasks = &[]MapTask{}
	c.ReduceTasks = &[]ReduceTask{}
	for i, file := range files {
		mapTask := MapTask{file, Pending, i, 0}
		*c.MapTasks = append(*c.MapTasks, mapTask)
	}
	// run periodic health check
	go func() {
		for {
			time.Sleep(1 * time.Second)
			c.PeriodicHealthCheck()
		}
	}()

	c.server()

	return &c
}
