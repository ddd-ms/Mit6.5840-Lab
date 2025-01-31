package mr

import (
	"bytes"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sync"
	"time"
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

func doHeartbeat() *HeartbeatResponse {
	resp := HeartbeatResponse{}
	call("Coordinator.Heartbeat", &HeartbeatRequest{}, &resp)
	return &resp
}
func doReport(id int, phase SchedulePhase) {
	call("Coordinator.Report", &ReportRequest{id, phase}, &ReportResponse{})
}

func doMap(mapf func(string, string) []KeyValue, resp *HeartbeatResponse) {
	fileName := resp.FilePath
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatal("doMap: ", err)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("doMap: cant read %v", fileName)
	}
	file.Close()
	// apply mapfunc to file content and prepare to correct shape for writing.
	kva := mapf(fileName, string(content))
	intermediates := make([][]KeyValue, resp.NReduce)

	for _, kv := range kva {
		index := ihash(kv.Key) % resp.NReduce
		intermediates[index] = append(intermediates[index], kv)
	}
	// 打印intermediates的长度， 并打印每个元素的长度
	// log.Printf("-----intermediates: %v\n", len(intermediates))
	// for i := 0; i < len(intermediates); i++ {
	// 	log.Printf("---------subs: [%v] len: %v\n", i, len(intermediates[i]))
	// }
	// multi-go-routine to write into file.
	var wg sync.WaitGroup
	for index, intermediate := range intermediates {
		wg.Add(1)
		go func(index int, intermediate []KeyValue) {
			defer wg.Done()
			interFilePath := generateMapResultFileName(resp.Id, index) //
			var buf bytes.Buffer
			enc := json.NewEncoder(&buf)
			for _, kv := range intermediate {
				err := enc.Encode(&kv)
				if err != nil {
					log.Fatalf("doMap: %v", err)
				}
			}
			atomicWriteFile(interFilePath, &buf)
		}(index, intermediate)
	}
	wg.Wait()
	doReport(resp.Id, MapPhase)
}

func doReduce(reduceF func(string, []string) string, resp *HeartbeatResponse) {
	var kva []KeyValue
	for i := 0; i < resp.NMap; i++ {
		filePath := generateMapResultFileName(i, resp.Id)
		file, err := os.Open(filePath)
		if err != nil {
			log.Fatalf("cant open %v --doReduce()", filePath)
		}
		decoder := json.NewDecoder(file)
		for {
			var kv KeyValue
			err := decoder.Decode(&kv)
			if err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	// str->[]str
	results := make(map[string][]string)
	// TODO:: modify to mergesort
	for _, kv := range kva {
		results[kv.Key] = append(results[kv.Key], kv.Value)
	}
	var buf bytes.Buffer
	for k, v := range results {
		output := reduceF(k, v)
		//write to buf
		fmt.Fprintf(&buf, "%v %v\n", k, output)
	}
	atomicWriteFile(generateReduceFileName(resp.Id), &buf)
	fmt.Printf("write file %v\n", generateReduceFileName(resp.Id))
	doReport(resp.Id, ReducePhase)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for { // endless for loop
		resp := doHeartbeat()
		log.Printf("Worker: received job %v", resp)
		switch resp.JobType {
		case MapJob:
			doMap(mapf, resp)
		case ReduceJob:
			doReduce(reducef, resp)
		case WaitJob:
			time.Sleep(time.Second)
		case CompleteJob:
			return
		default:
			panic(fmt.Sprintf("unknown job type %v", resp))
		}
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
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
