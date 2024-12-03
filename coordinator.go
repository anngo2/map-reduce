package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

type Coordinator struct {
	lock sync.Mutex
	// Your definitions here.
	files            []File
	SuccessTask      []bool
	SuccessfulReduce []bool
	reduceTask       []RE
	reduceNum        int
}
type RE struct {
	retaskisDone bool
	Repending    bool
}
type File struct {
	filename string
	isDone   bool
	i        int
	Pending  bool
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
// the RPC argument and reply types are defined in rpc.go.

func (c *Coordinator) AssignFiles() (string, bool, int) {

	for i := 0; i < len(c.files); i++ {

		if !c.files[i].isDone && !c.files[i].Pending {
			fmt.Println(c.files[i].filename, i)
			c.files[i].Pending = true
			fmt.Println("assigned task " + strconv.Itoa(i))

			F := c.files[i].filename

			return F, true, i
		}

		//so we are assigning files, we can check if all the files are actually done by putting the successful files into a new array where it keeps track
		//of actual success, if it's the same as the len of the number of files

	}

	return "", false, -1
}

func (c *Coordinator) AssignReduce() (bool, int) {
	for i := 0; i < c.reduceNum; i++ {

		if !c.ReduceDone() && !c.reduceTask[i].Repending {
			c.reduceTask[i].Repending = true

			return true, i
		}

	}

	return false, -1
}

func (c *Coordinator) RPCcoordinator(args *MapArgs, reply *MapReply) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if !c.MapDone() {
		reply.TType = "map"

		reply.NumReduce = c.reduceNum

		store, working, IDtask := c.AssignFiles()

		if working {
			// Use `file` here for processing

			//c.SuccessTask[IDtask]
			//check to see if we are able to connect channels

			reply.File = store

			reply.TaskID = IDtask
			fmt.Println(store)

			go c.CheckFail(IDtask)
			fmt.Println("rpc call")

		}
	} else if !c.ReduceDone() {
		reply.TType = "reduce"
		working, reTask := c.AssignReduce()
		if working {
			reply.File = ""
			reply.TaskID = reTask

			reply.NumReduce = c.reduceNum

			reply.ReduceTask = len(c.files)
			go c.ReCheckFail(reTask)

		}
	} else {
		fmt.Println(reply.TType + " HITS THE WAIT CASE")
		reply.TType = "wait"

	}

	return nil
}

func (c *Coordinator) CheckwError(Ask *WorkerCompleteArg, reply *WorkerCompleteReply) error {
	//this is the function that worker calls to flip the values of Success and isDone. This is basically checking to see
	//if the worker task is TRULY done
	c.lock.Lock()
	//fmt.Println(strconv.Itoa(Ask.ID) + "hits true")
	c.SuccessTask[Ask.ID] = true
	c.files[Ask.ID].isDone = true
	c.lock.Unlock()
	return nil
}

func (c *Coordinator) ReduceCheckError(Ask *WorkerCompleteArg, reply *WorkerCompleteReply) error {
	//this is checking to see if the task has failed within 10 seconds.
	c.lock.Lock()
	c.reduceTask[Ask.ID].retaskisDone = true
	c.SuccessfulReduce[Ask.ID] = true
	c.lock.Unlock()
	return nil
}

func (c *Coordinator) ReCheckFail(task int) {
	//this is checking to see if the task has failed within 10 seconds.
	time.Sleep(time.Duration(10 * time.Second))
	c.lock.Lock()
	if !c.SuccessfulReduce[task] && c.reduceTask[task].Repending {
		c.reduceTask[task].Repending = false
		c.reduceTask[task].retaskisDone = false

	}
	c.lock.Unlock()

}

func (c *Coordinator) CheckFail(task int) {

	time.Sleep(time.Duration(10 * time.Second))
	c.lock.Lock()
	if !c.SuccessTask[task] && c.files[task].Pending {
		c.files[task].Pending = false
		c.files[task].isDone = false
	}
	c.lock.Unlock()

}

func (c *Coordinator) MapDone() bool {

	for i := 0; i < len(c.SuccessTask); i++ {

		if !c.SuccessTask[i] {

			return false
		}

	}
	return true

}

func (c *Coordinator) ReduceDone() bool {
	for i := 0; i < len(c.SuccessfulReduce); i++ {

		if !c.SuccessfulReduce[i] {

			return false
		}

	}
	return true
}

// mr-main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {

	// Your code here.
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.ReduceDone() && c.MapDone()
}

// create a Coordinator.
// mr-main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	filelen := len(files)

	allfiles := make([]File, filelen)
	for i, fname := range files {
		allfiles[i] = File{
			filename: fname,
			isDone:   false,
			i:        i,
		}
	}

	success := make([]bool, filelen)
	successReduce := make([]bool, nReduce)
	reducefiles := make([]RE, nReduce)

	c := Coordinator{sync.Mutex{}, allfiles, success, successReduce, reducefiles, nReduce}

	// Your code here.

	c.server()
	return &c
}

// start a thread that listens for RPCs from worker.go
// DO NOT MODIFY
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
