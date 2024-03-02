package mr

import (
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

const (
	ErrorMessage = "error in calling coordinator"
	RPCHandler   = "Coordinator.RPCHandler"
)

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
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	isDone := false
	// create a random worker id
	b := make([]byte, 1)
	rand.Read(b)
	workerId := fmt.Sprintf("%d", b[0])
	log.Printf("worker id: %v", workerId)
	for !isDone {
		log.Printf("worker %v calling coordinator for a job", workerId)
		reply, err := CallNeedJob(workerId)
		if err != nil {
			log.Printf("workerId: %v error in calling coordinator: exiting", workerId)
			isDone = true
		}
		switch reply.Command {
		case "wait":
			log.Printf("worker %v waiting for a job", workerId)
			time.Sleep(time.Second)

		case "map":
			log.Printf("worker %v executing map task %d", workerId, reply.MapTaskNum)
			success, err := executeMap(mapf, reply.FileName, reply.NReduce, reply.MapTaskNum)
			if err != nil {
				log.Printf("worker %v error in executing map", workerId)
			}
			if success {
				log.Printf("worker %v map task %d done", workerId, reply.MapTaskNum)
				CallMapSuccess(reply.MapTaskNum)
			}
		case "reduce":
			log.Printf("worker %v executing reduce task %d", workerId, reply.ReduceTaskNum)
			success, err := executeReduce(reducef, reply.FileNames, reply.ReduceTaskNum)
			if err != nil {
				log.Printf("worker %v error in executing reduce", workerId)
			}
			if success {
				log.Printf("worker %v reduce task %d done", workerId, reply.ReduceTaskNum)
				CallReduceSuccess(reply.ReduceTaskNum)
			}

		case "done":
			isDone = true
			log.Printf("worker %v all tasks done: time to exit", workerId)
		default:
			log.Printf("worker %v reply: %v", workerId, reply)
			log.Printf("worker %v unknown command", workerId)
			isDone = true
		}
	}

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

func CallNeedJob(workerId string) (Reply, error) {
	args := Args{}
	args.Query = "give me a job"
	reply := Reply{}
	ok := call(RPCHandler, &args, &reply)
	if ok {
		return reply, nil
	}
	log.Printf("worker %v error in calling coordinator", workerId)
	return Reply{}, errors.New(ErrorMessage)
}

func CallMapSuccess(taskNum int) (Reply, error) {
	args := Args{}
	args.Query = "map done"
	args.MapTaskNum = taskNum
	reply := Reply{}
	ok := call(RPCHandler, &args, &reply)
	if ok {
		return reply, nil
	}
	log.Println(ErrorMessage)
	return Reply{}, errors.New(ErrorMessage)
}

func CallReduceSuccess(reduceTaskNum int) (Reply, error) {
	args := Args{}
	args.Query = "reduce done"
	args.ReduceTaskNum = reduceTaskNum
	reply := Reply{}
	ok := call(RPCHandler, &args, &reply)
	if ok {
		return reply, nil
	}
	log.Println(ErrorMessage)
	return Reply{}, errors.New(ErrorMessage)
}

func executeReduce(reducef func(string, []string) string, fileNames []string, reduceTaskNum int) (bool, error) {
	// Read intermediate files
	kva := []KeyValue{}
	// create a temp file for output. to be atomically renamed to final output file
	// because multiple workers may be writing to the same output file
	tempOutfile, err := os.CreateTemp("", "mr-out-temp*")
	if err != nil {
		log.Printf("cannot open temp file for reduce output")
		return false, err
	}
	for _, filename := range fileNames {
		jsonFile, err := os.Open(filename)
		if err != nil {
			log.Printf("cannot open %v", filename)
			return false, err
		}
		dec := json.NewDecoder(jsonFile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err == io.EOF {
				break
			} else if err != nil {
				log.Printf("error in decoding json file %v", filename)
				return false, err
			}
			kva = append(kva, kv)
		}
		jsonFile.Close()
	}
	// sort key value pairs by key
	sort.Slice(kva, func(i, j int) bool {
		return kva[i].Key < kva[j].Key
	})

	// Collect values for each key
	for i := 0; i < len(kva); {
		values := []string{}
		// find the end point for the key
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		// collect all values for the key
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		// Call reduce function
		output := reducef(kva[i].Key, values)
		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tempOutfile, "%v %v\n", kva[i].Key, output)
		i = j
	}
	tempOutfile.Close()
	// rename temp file to final output file
	log.Printf("worker %d renaming temp file to final output file", reduceTaskNum)
	err = os.Rename(tempOutfile.Name(), fmt.Sprintf("mr-out-%d", reduceTaskNum))
	if err != nil {
		log.Printf("error in renaming temp file to final output file")
		return false, err
	}
	log.Printf("worker %d renamed temp file to final output file", reduceTaskNum)
	return true, nil
}

func executeMap(mapf func(string, string) []KeyValue, filename string, nReduce int, mapTaskNum int) (bool, error) {
	// Open input file
	file, err := os.Open(filename)
	if err != nil {
		log.Printf("cannot open %v", filename)
		return false, err
	}
	defer file.Close()
	content, err := io.ReadAll(file)
	if err != nil {
		log.Printf("cannot read %v", filename)
		return false, err
	}
	// Call map function
	kva := mapf(filename, string(content))
	// Create intermediate files
	filesList := []*json.Encoder{}
	for r := 0; r < nReduce; r++ {
		intermediateFilename := fmt.Sprintf("/tmp/mr-%d-%d.txt", mapTaskNum, r)
		intermediateFile, err := os.Create(intermediateFilename)
		if err != nil {
			log.Printf("cannot open %v", intermediateFilename)
			return false, err
		}
		defer intermediateFile.Close()
		filesList = append(filesList, json.NewEncoder(intermediateFile))
	}
	// Write intermediate key value pairs to intermediate files
	for _, kv := range kva {
		reduceNum := ihash(kv.Key) % nReduce
		enc := *filesList[reduceNum]
		err := enc.Encode(&kv)
		if err != nil {
			log.Printf("error in writing to intermediate file - key: %v, reduce num: %d", kv.Key, reduceNum)
			return false, err
		}
	}
	return true, nil
}
