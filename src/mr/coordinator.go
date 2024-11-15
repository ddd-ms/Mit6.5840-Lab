package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

const (
	MaxTaskRunInterval = time.Second * 10
)

type Task struct {
	fileName  string
	id        int
	startTime time.Time
	status    TaskStatus
}
type hearbeatMsg struct {
	response *HeartbeatResponse
	ok       chan struct{}
}
type reportMsg struct {
	request *ReportRequest
	ok      chan struct{}
}

type Coordinator struct {
	// Your definitions here.
	// Worker-stateless, Channel-based impl
	files   []string
	nReduce int
	nMap    int
	phase   SchedulePhase
	tasks   []Task

	//channels
	heartbeatCh chan hearbeatMsg
	reportCh    chan reportMsg
	doneCh      chan struct{}
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}
func (c *Coordinator) Heartbeat(request *HeartbeatRequest, response *HeartbeatResponse) error {
	msg := hearbeatMsg{response, make(chan struct{})}
	c.heartbeatCh <- msg
	<-msg.ok
	return nil
}
func (c *Coordinator) Report(request *ReportRequest, response *ReportResponse) error {
	msg := reportMsg{request, make(chan struct{})}
	c.reportCh <- msg
	<-msg.ok
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
	<-c.doneCh
	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:       files,
		nReduce:     nReduce,
		nMap:        len(files),
		heartbeatCh: make(chan hearbeatMsg),
		reportCh:    make(chan reportMsg),
		doneCh:      make(chan struct{}, 1),
	}

	// Your code here.
	c.server()
	go c.schedule()
	return &c
}

/*
  - 接受一个地址参数，用于写入配置信息以便worker运行
    返回bool，如果True表示所有任务都已经完成
*/
func (c *Coordinator) selectTask(resp *HeartbeatResponse) bool {
	allDone, hasNewJob := true, false
	for id, task := range c.tasks {
		switch task.status {
		case Idle:
			allDone, hasNewJob = false, true
			c.tasks[id].status = Working
			c.tasks[id].startTime = time.Now()
			resp.NReduce = c.nReduce
			resp.Id = id
			if c.phase == MapPhase {
				resp.JobType = MapJob
				resp.FilePath = c.files[id]
			} else {
				// Reduce phase
				resp.JobType = ReduceJob
				resp.NMap = c.nMap
			}
		case Working:
			allDone = false
			if time.Since(task.startTime) > MaxTaskRunInterval {
				hasNewJob = true
				c.tasks[id].startTime = time.Now()
				resp.NReduce, resp.Id = c.nReduce, id
				if c.phase == MapPhase {
					resp.JobType, resp.FilePath = MapJob, c.files[id]
				} else {
					resp.JobType = ReduceJob
					resp.NMap = c.nMap
				}
			}
		case Finished:
		}
		if hasNewJob {
			break
		}
	}
	if !hasNewJob {
		resp.JobType = WaitJob
	}
	return allDone
}
func (c *Coordinator) schedule() {
	c.initMapPhase()
	for { //endless loop until completePhase
		select {
		case msg := <-c.heartbeatCh:
			if c.phase == CompletePhase {
				msg.response.JobType = CompleteJob
			} else if c.selectTask(msg.response) {
				// selectTask return True:: all tasks are finished at current phase
				// switch to next phase
				switch c.phase {
				case MapPhase:
					log.Print("Coordinator: switch to Reduce phase\n")
					c.initReducePhase()
					c.selectTask(msg.response)
				case ReducePhase:
					log.Print("Coordinator: switch to Complete phase\n")
					c.initCompletePhase()
					msg.response.JobType = CompleteJob
				default:
					panic("coordinator: unknown phase")
				}
			}
			log.Printf("Coordinator: assigned a task %v to woker \n", msg.response)
			// 大小为零的结构体 ，不占用任何内存
			msg.ok <- struct{}{}
		case msg := <-c.reportCh:
			// worker完成工作后调用doReport函数，使用RPC调用Coordinator的Report函数
			// 统一在schedule中维护tasks数据结构，使用单个go routine,避免了数据竞争（PS:理想情况下每个task 对应一个go routine，但是不排除超时情况，以及避免了把coordinator内部的变量暴露出去）
			if msg.request.Phase == c.phase {
				log.Printf("Coordinator: worker has executed task %v\n", msg.request)
				c.tasks[msg.request.Id].status = Finished
			}
			msg.ok <- struct{}{}
		}

	}
}
func (c *Coordinator) initMapPhase() {
	c.phase = MapPhase
	// len(c.files) 就是 NMap
	c.tasks = make([]Task, len(c.files))
	for i, file := range c.files {
		c.tasks[i] = Task{
			fileName: file,
			id:       i,
			status:   Idle,
		}
	}
}
func (c *Coordinator) initReducePhase() {
	c.phase = ReducePhase
	// 重新建立任务序列，状态设置为闲置
	c.tasks = make([]Task, c.nReduce)
	for i := 0; i < c.nReduce; i++ {
		c.tasks[i] = Task{
			id:     i,
			status: Idle,
		}
	}
}
func (c *Coordinator) initCompletePhase() {
	c.phase = CompletePhase
	c.doneCh <- struct{}{}
}
