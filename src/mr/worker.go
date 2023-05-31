package mr

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
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
		ID:         task.ID,
		TaskType:   task.TaskType,
		WorkerID:   task.WorkerID,
		TaskStatus: task.TaskStatus,
	}
	reply := ReportTaskReply{}
	call("Coordinator.MarkTask", &args, &reply)
}

func performMap(task *Task, mapf func(string, string) []KeyValue) {
	file, err := os.Open(task.InputFile)
	if err != nil {
		task.TaskStatus = Failed
		reportTask(task)
		return
	}
	contents, err := ioutil.ReadAll(file)
	if err != nil {
		task.TaskStatus = Failed
		reportTask(task)
		return
	}
	file.Close()
	keyValues := mapf(task.InputFile, string(contents))

	// once I get the keyValues, we need to write them to intermediate files
	// in order to do that we need to group the keyValues by their key
	// the way we do that is by first calculating the hash of the key
	// then we use the hash to determine which file to write the keyValues to
	intermediateValues := make(map[int][]KeyValue)
	for _, keyValue := range keyValues {
		index := ihash(keyValue.Key) % task.NReduce
		intermediateValues[index] = append(intermediateValues[index], keyValue)
	}
	err = writeIntermediateFiles(intermediateValues, task.ID)
	if err != nil {
		task.TaskStatus = Failed
	}
	reportTask(task)
}

func writeIntermediateFiles(intermediateValues map[int][]KeyValue, taskID int) error {
	var wg sync.WaitGroup
	wg.Add(len(intermediateValues))
	errChan := make(chan error, len(intermediateValues))
	for index, keyValues := range intermediateValues {
		go func(index int, keyValues []KeyValue) {
			defer wg.Done()
			fileName := fmt.Sprintf("mr-%d-%d", taskID, index)
			var buffer bytes.Buffer
			enc := json.NewEncoder(&buffer)
			for _, keyValue := range keyValues {
				err := enc.Encode(&keyValue)
				if err != nil {
					errChan <- err
					return
				}
			}
			err := atomicWrite(fileName, &buffer)
			if err != nil {
				errChan <- err
				return
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
	pattern := fmt.Sprintf("mr-*-%d", task.ID) // here task.ID represents the nth reducer
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return
	}
	results := make(map[string][]string)
	for index, _ := range matches {
		f, err := os.Open(matches[index])
		if err != nil {
			task.TaskStatus = Failed
			reportTask(task)
			return
		}
		decoder := json.NewDecoder(f)
		for {
			var kv KeyValue
			if err := decoder.Decode(&kv); err != nil {
				break
			}
			results[kv.Key] = append(results[kv.Key], kv.Value)
		}
	}

	/*
		Both bufio.NewWriter and strings.Builder are used in Go for efficient concatenation or creation of strings,
		but they are used in different scenarios and have different behaviors. Use bufio.NewWriter when you're writing
		data to an io.Writer and want to buffer those writes to reduce the number of system calls. This is typically
		used when writing data to files or network connections.

		Use strings.Builder when you're building a string in memory that will be used in your program. This is typically
		more efficient than string concatenation (+ or +=) for building up large strings.
	*/

	var buffer bytes.Buffer
	writer := bufio.NewWriter(&buffer)

	for key, val := range results {
		writer.WriteString(fmt.Sprintf("%v %v\n", key, reducef(key, val)))
	}
	err = writer.Flush()
	if err != nil {
		task.TaskStatus = Failed
		reportTask(task)
		return
	}

	err = atomicWrite(fmt.Sprintf("mr-out-%d", task.ID), &buffer)
	if err != nil {
		task.TaskStatus = Failed
		reportTask(task)
		return
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

func atomicWrite(filename string, r io.Reader) error {
	dir, file := filepath.Split(filename)
	if dir == "" {
		dir = "."
	}
	// create a temporary file
	tempFile, err := ioutil.TempFile(dir, file)
	if err != nil {
		return err
	}
	defer os.Remove(tempFile.Name())

	// write the contents to the temporary file
	_, err = io.Copy(tempFile, r)
	if err != nil {
		return err
	}

	// rename the temporary file to the original file
	err = os.Rename(tempFile.Name(), filename)
	if err != nil {
		return err
	}
	return nil
}
