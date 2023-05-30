package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sync"
)

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

	for {
		task := getTask()
		if task.TaskStatus == Exit {
			break
		} else if task.TaskStatus == Wait {
			continue
		}
		switch task.TaskType {
		case TaskMap:
			performMap(task, mapf)
		case TaskReduce:
			performReduce(task, reducef)
		}
	}
}

func reportTask(task *Task) {
	args := ReportTaskArgs{
		ID:       task.ID,
		TaskType: task.TaskType,
		WorkerID: task.WorkerID,
	}
	reply := ReportTaskReply{}
	call("Coordinator.MarkTask", &args, &reply)
}

func performMap(task *Task, mapf func(string, string) []KeyValue) {
	file, err := os.Open(task.InputFile)
	fmt.Printf("Opening %v\n", task.InputFile)
	if err != nil {
		fmt.Printf("cannot open the map file %v\n", task)
		return
	}
	contents, err := ioutil.ReadAll(file)
	if err != nil {
		fmt.Printf("cannot read the map file %v\n", task)
		return
	}
	file.Close()
	keyValues := mapf(task.InputFile, string(contents))

	// once I get the keyValues, we need to write them to intermediate files
	// in order to do that we need to group the keyValues by their key
	// the way we do that is by first calculating the hash of the key
	// then we use the hash to determine which file to write the keyValues to
	intermediateValues := make([][]KeyValue, task.NReduce)
	for _, keyValue := range keyValues {
		index := ihash(keyValue.Key) % task.NReduce
		intermediateValues[index] = append(intermediateValues[index], keyValue)
	}
	err = writeIntermediateFiles(intermediateValues, task.ID)
	if err != nil {
		fmt.Printf("cannot write the intermediate files %v\n", err)
		task.TaskStatus = Failed
	} else {
		task.TaskStatus = Completed
	}
	reportTask(task)
}

func writeIntermediateFiles(intermediateValues [][]KeyValue, taskID int) error {
	var wg sync.WaitGroup
	wg.Add(len(intermediateValues))
	errChan := make(chan error, len(intermediateValues))
	for index, keyValues := range intermediateValues {
		go func(index int, keyValues []KeyValue) {
			defer wg.Done()
			fileName := fmt.Sprintf("mr-%d-%d", taskID, index)
			file, err := os.Create(fileName)
			if err != nil {
				errChan <- err
				return
			}
			defer file.Close()
			enc := json.NewEncoder(file)
			for _, keyValue := range keyValues {
				err := enc.Encode(&keyValue)
				if err != nil {
					errChan <- err
					return
				}
			}
		}(index, keyValues)
	}

	go func() {
		wg.Wait()
		close(errChan)
	}()

	for err := range errChan {
		if err != nil {
			return err
		}
	}
	return nil
}

func performReduce(task *Task, reducef func(string, []string) string) {
	pattern := fmt.Sprintf("mr-*-%d", task.ID)
	matches, _ := filepath.Glob(pattern)
	foo := make(map[string][]string)
	for index, _ := range matches {
		f, _ := os.Open(matches[index])
		decoder := json.NewDecoder(f)
		for {
			var kv KeyValue
			if err := decoder.Decode(&kv); err != nil {
				break
			}
			foo[kv.Key] = append(foo[kv.Key], kv.Value)
		}
	}
	for key, val := range foo {
		fmt.Printf("key %v, count %v\n", key, reducef(key, val))
	}
	reportTask(task)
}

func getTask() *Task {
	args := GetTaskArgs{
		WorkerID: os.Getpid(),
	}
	reply := GetTaskReply{}
	call("Coordinator.GetTask", &args, &reply)
	return reply.Task
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
