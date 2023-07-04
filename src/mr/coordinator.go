package mr

import (
	"fmt"
	"log"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	lock                sync.Mutex
	mapTasks            []string
	totalMap            int
	runningWorker       map[int]struct{}
	finishedMapTasks    map[string]struct{}
	reduceTasks         []string
	finishedReduceTasks map[string]struct{}
	nReduce             int
	workerCounter       int
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	reply.Finished = false
	reply.NReduce = c.nReduce
	c.workerCounter += 1
	reply.PID = c.workerCounter
	if len(c.finishedMapTasks) < c.totalMap {
		if len(c.mapTasks) == 0 {
			reply.Wait = true
			return nil
		}
		reply.IsMap = true
		reply.Key = c.mapTasks[0]
		key := c.mapTasks[0]
		id := c.workerCounter
		c.mapTasks = c.mapTasks[1:]
		c.runningWorker[id] = struct{}{}
		go func() {
			time.Sleep(10 * time.Second)
			c.lock.Lock()
			defer c.lock.Unlock()
			if _, ok := c.finishedMapTasks[key]; ok {
				return
			} else {
				c.mapTasks = append(c.mapTasks, key)
				delete(c.runningWorker, id)
			}

		}()
	} else if len(c.finishedReduceTasks) < c.nReduce {
		reply.IsMap = false
		if len(c.reduceTasks) == 0 {
			reply.Wait = true
			return nil
		}
		reply.Key = c.reduceTasks[0]
		key := c.reduceTasks[0]
		id := c.workerCounter
		c.reduceTasks = c.reduceTasks[1:]
		c.runningWorker[id] = struct{}{}
		go func() {
			time.Sleep(10 * time.Second)
			c.lock.Lock()
			defer c.lock.Unlock()
			if _, ok := c.finishedReduceTasks[key]; ok {
				return
			} else {
				c.reduceTasks = append(c.reduceTasks, key)
				delete(c.runningWorker, id)
			}
		}()
	} else {
		reply.Finished = true
	}
	return nil
}

func (c *Coordinator) FinishTask(args *FinishTaskArgs, reply *FinishTaskReply) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if _, ok := c.runningWorker[args.PID]; !ok {
		reply.Error = true
		return nil
	}
	delete(c.runningWorker, args.PID)
	reply.Error = false
	if args.IsMap {
		c.finishedMapTasks[args.Key] = struct{}{}
	} else {
		c.finishedReduceTasks[args.Key] = struct{}{}
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
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
	ret := false

	// Your code here.
	if len(c.finishedReduceTasks) == c.nReduce {
		ret = true
		// 获取当前文件夹路径
		folderPath, err := os.Getwd()
		if err != nil {
			panic(err)
		}

		// 遍历当前文件夹下的所有文件
		err = filepath.Walk(folderPath, func(path string, info os.FileInfo, err error) error {
			// 检查文件名是否以 "tmp-" 前缀开头
			if !info.IsDir() && strings.HasPrefix(info.Name(), "tmp-") {
				// 删除文件
				err := os.Remove(path)
				if err != nil {
					fmt.Println(err)
				}
			}
			return nil
		})
	}
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.finishedReduceTasks = map[string]struct{}{}
	c.finishedMapTasks = map[string]struct{}{}
	c.runningWorker = map[int]struct{}{}
	c.mapTasks = files
	c.reduceTasks = []string{}
	c.totalMap = len(c.mapTasks)
	for i := 0; i < nReduce; i++ {
		c.reduceTasks = append(c.reduceTasks, strconv.Itoa(i))
	}
	c.nReduce = nReduce
	c.server()
	return &c
}
