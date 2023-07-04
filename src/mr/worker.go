package mr

import (
	"bufio"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)
import "log"
import "net/rpc"
import "hash/fnv"

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

	// Your worker implementation here.
	for true {
		taskReply := GetTask()
		workerNum := taskReply.PID
		if taskReply.Wait {
			continue
		}
		if taskReply.Finished {
			return
		}
		if taskReply.IsMap {
			contentByte, err := os.ReadFile(taskReply.Key)
			if err != nil {
				panic(err)
			}

			kvs := mapf(taskReply.Key, string(contentByte))
			intermedias := make([][]KeyValue, taskReply.NReduce)
			for _, kv := range kvs {
				idx := ihash(kv.Key) % taskReply.NReduce
				intermedias[idx] = append(intermedias[idx], kv)
			}
			tmpList := []string{}
			for i, intermedia := range intermedias {
				tmpFileName := fmt.Sprintf("tmp-%v-%v", workerNum, i)
				tmpList = append(tmpList, tmpFileName)
				file, err := os.OpenFile(tmpFileName,
					os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
				defer file.Close()
				if err != nil {
					panic(err)
				}
				for _, kv := range intermedia {
					file.WriteString(fmt.Sprintf("%v %v\n", kv.Key, kv.Value))
				}
			}
			if FinishTask(true, taskReply.Key, workerNum) {
				for _, fileName := range tmpList {
					os.Remove(fileName)
				}
			}

		} else {
			pwd, _ := os.Getwd()
			count := map[string][]string{}
			filepath.Walk(pwd, func(path string, info fs.FileInfo, err error) error {
				parts := strings.Split(info.Name(), "-")
				if parts[len(parts)-1] != taskReply.Key {
					return nil
				}
				file, err := os.Open(path)
				defer file.Close()
				if err != nil {
					return nil
				}
				scanner := bufio.NewScanner(file)

				for scanner.Scan() {
					line := scanner.Text()

					// 使用空格分割字符串
					parts := strings.Split(line, " ")
					if len(parts) == 2 {
						count[parts[0]] = append(count[parts[0]], parts[1])
					}
				}
				return nil
			})
			results := [][]string{}
			for k, v := range count {
				results = append(results, []string{k, reducef(k, v)})
			}
			file, _ := os.OpenFile(fmt.Sprintf("mr-out-%v", taskReply.Key),
				os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
			defer file.Close()
			for _, result := range results {
				file.WriteString(fmt.Sprintf("%v %v\n", result[0], result[1]))
			}
			if FinishTask(false, taskReply.Key, workerNum) {
				os.Remove(fmt.Sprintf("mr-out-%v", taskReply.Key))
			}
		}
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}
func GetTask() GetTaskReply {
	args := GetTaskArgs{}
	reply := GetTaskReply{}
	ok := call("Coordinator.GetTask", &args, &reply)
	if !ok {
		reply.Finished = true
	}
	return reply
}

func FinishTask(isMap bool, key string, id int) bool {
	args := FinishTaskArgs{
		IsMap: isMap,
		Key:   key,
		PID:   id,
	}
	reply := FinishTaskReply{}
	ok := call("Coordinator.FinishTask", &args, &reply)
	if !ok {
		panic(fmt.Errorf("cannot call finish task"))
	}
	return reply.Error

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
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
