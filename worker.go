package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// mr-main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.

	// fill in the argument(s).

	// declare a reply structure.
	args := MapArgs{}
	// send the RPC request, wait for the reply.
	for {

		reply := MapReply{}
		call("Coordinator.RPCcoordinator", &args, &reply)
		if reply.TType == "map" {
			// fmt.Println("REACHED slay")
			fmt.Println(reply.File)
			file, err := os.Open(reply.File)
			if err != nil {
				log.Fatalf("cannot open %v", reply.File)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", reply.File)
			}
			file.Close()
			kva := mapf(reply.File, string(content))

			// fmt.Println("MAPPED slay")
			mapping := make(map[int][]KeyValue)
			for _, kv := range kva {
				hashKey := ihash(kv.Key)
				reduceTask := hashKey % reply.NumReduce
				mapping[reduceTask] = append(mapping[reduceTask], kv)
			}
			//fmt.Println(kva)
			for i := 0; i < reply.NumReduce; i++ {

				file, err = os.OpenFile("mr-"+strconv.Itoa(reply.TaskID+1)+"-"+strconv.Itoa(i), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
				//fmt.Println("mr-" + strconv.Itoa(reply.TaskID+1) + "-" + strconv.Itoa(i))
				if err != nil {
					log.Fatal(err)
				}
				groupString := ""
				for j := 0; j < len(mapping[i]); j++ {
					key := mapping[i][j].Key
					value := mapping[i][j].Value
					groupString += key + ": " + value + "\n"
				}
				_, err = file.WriteString(groupString)

				if err != nil {
					log.Fatal(err)
				} else {
					a := WorkerCompleteArg{reply.TaskID}
					r := WorkerCompleteReply{}
					call("Coordinator.CheckwError", &a, &r)

				}
				file.Close()

			}
		} else if reply.TType == "reduce" {
			intermediate := []KeyValue{}
			currentdir := "./"

			dirfile, err := ioutil.ReadDir(currentdir)

			if err != nil {
				log.Fatal("Can't read dir")

			}

			for _, fi := range dirfile {
				if strings.HasSuffix(fi.Name(), strconv.Itoa(reply.TaskID)) {
					content, err := os.ReadFile(filepath.Join(currentdir, fi.Name()))

					if err != nil {
						log.Fatalf("cannot open!!!!!! %v", "mr-"+"-"+strconv.Itoa(reply.TaskID))
					}

					lines := strings.Split(string(content), "\n")
					for _, line := range lines {
						line = strings.TrimSpace(line)
						parts := strings.SplitN(line, ": ", 2)
						if len(parts) < 2 {
							continue
						}
						intermediate = append(intermediate, KeyValue{Key: parts[0], Value: parts[1]})
					}
				}

			}
			sort.Sort(ByKey(intermediate))
			i := 0
			oname := "mr-out-" + strconv.Itoa(reply.TaskID)
			ofile, _ := os.Create(oname)
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				output := reducef(intermediate[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

				i = j
			}

			a := WorkerCompleteArg{reply.TaskID}
			r := WorkerCompleteReply{}
			call("Coordinator.ReduceCheckError", &a, &r)

		} else if reply.TType == "wait" {

			time.Sleep(time.Duration(1 * time.Second))
		}
	}
}

//	CallExample()
//
//}

// example function to show how to make an RPC call to the coordinator.
// the RPC argument and reply types are defined in rpc.go.
// func CallExample() {

// 	 declare an argument structure.

// 	reply.Y should be 100.

// 	fmt.Printf("reply.Y %v\n", reply)

// }

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
// DO NOT MODIFY
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

	fmt.Println("Unable to Call", rpcname, "- Got error:", err)
	return false
}
