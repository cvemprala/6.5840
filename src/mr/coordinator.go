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

type TaskStatus int

const (
	Idle TaskStatus = iota
	Wait
	InProgress
	Completed
	Failed
	Exit
)

type TaskType int

const (
	TaskMap TaskType = iota
	TaskReduce
)

type State int

const (
	Map State = iota
	Reduce
	Done
)

type Task struct {
	ID         int
	Timestamp  time.Time
	InputFile  string
	TaskStatus TaskStatus
	TaskType   TaskType
	WorkerID   int
	NReduce    int
}

type Coordinator struct {
	mu          *sync.RWMutex
	tasks       map[string]*Task
	mapTasks    chan *Task
	mTaskCount  int
	reduceTasks chan *Task
	rTaskCount  int
	state       State
	nReduce     int
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	task := c.AssignTask(args.WorkerID)
	reply.Task = task
	if task.TaskStatus == InProgress {
		taskID := fmt.Sprintf("Map-%d", task.ID)
		if task.TaskType == TaskReduce {
			taskID = fmt.Sprintf("Reduce-%d", task.ID)
		}
		go c.checkTimeout(taskID)
	}
	return nil
}

func (c *Coordinator) MarkTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	reply.OK = c.MarkTaskCompleted(args.ID, args.TaskType, args.WorkerID)
	return nil
}

func (c *Coordinator) checkTimeout(taskID string) {
	time.Sleep(10 * time.Second)
	c.mu.Lock()
	defer c.mu.Unlock()
	task := c.tasks[taskID]
	if task.TaskStatus == InProgress {
		task.InputFile = ""
		task.TaskStatus = Idle
		task.WorkerID = -1
		task.Timestamp = time.Time{}
		if task.TaskType == TaskMap {
			c.mapTasks <- task
		} else {
			c.reduceTasks <- task
		}
	}
}

func (c *Coordinator) MarkTaskCompleted(taskID int, taskType TaskType, workerID int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	id := fmt.Sprintf("%s-%d", "Map", taskID)
	if taskType == TaskReduce {
		id = fmt.Sprintf("%s-%d", "Reduce", taskID)
	}
	task := c.tasks[id]
	if task.WorkerID == workerID { // task is assigned to the worker
		task.TaskStatus = Completed
		if task.TaskType == TaskMap {
			c.mTaskCount--
		} else {
			c.rTaskCount--
		}
		if c.mTaskCount == 0 && c.rTaskCount == 0 {
			c.state = Done
		}
		return true
	}
	return false // this happens when the task is reassigned to another worker
}

func (c *Coordinator) AssignTask(workerID int) *Task {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.state == Done {
		return &Task{TaskStatus: Exit}
	} else if c.mTaskCount > 0 {
		select {
		case task := <-c.mapTasks:
			task.TaskStatus = InProgress
			task.Timestamp = time.Now()
			task.WorkerID = workerID
			c.tasks[fmt.Sprintf("Map-%d", task.ID)] = task
			return task
		default:
			return &Task{TaskStatus: Wait}
		}
	} else if c.rTaskCount > 0 {
		select {
		case task := <-c.reduceTasks:
			task.TaskStatus = InProgress
			task.Timestamp = time.Now()
			task.WorkerID = workerID
			c.tasks[fmt.Sprintf("Reduce-%d", task.ID)] = task
			return task
		default:
			return &Task{TaskStatus: Wait}
		}
	} else {
		return &Task{TaskStatus: Wait}
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

func (c *Coordinator) Done() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.state == Done
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	mapTasks := make(chan *Task, len(files))
	reduceTasks := make(chan *Task, nReduce)
	taskMap := make(map[string]*Task)
	for index, file := range files {
		task := &Task{
			ID:         index,
			InputFile:  file,
			TaskStatus: Idle,
			TaskType:   TaskMap,
			WorkerID:   -1,
			NReduce:    nReduce,
		}
		mapTasks <- task
		taskMap[fmt.Sprintf("Map-%d", index)] = task
	}

	for index := 0; index < nReduce; index++ {
		task := &Task{
			ID:         index,
			InputFile:  "",
			TaskStatus: Idle,
			TaskType:   TaskReduce,
			WorkerID:   -1,
			NReduce:    nReduce,
		}
		reduceTasks <- task
		taskMap[fmt.Sprintf("Reduce-%d", index)] = task
	}

	c := Coordinator{
		tasks:       taskMap,
		mapTasks:    mapTasks,
		mTaskCount:  len(files),
		reduceTasks: reduceTasks,
		rTaskCount:  nReduce,
		state:       Map,
		nReduce:     nReduce,
		mu:          new(sync.RWMutex),
	}

	c.server()
	return &c
}
