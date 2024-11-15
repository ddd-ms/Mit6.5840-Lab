## Coordinator
分配任务，检查任务是否超时，若超时则将相同任务分配给新worker

## Worker
从Coordinator获取任务，执行任务，将任务结果写入文件

## Rule
The map phase should divide the intermediate keys into buckets for nReduce reduce tasks, where nReduce is the number of reduce tasks -- the argument that main/mrcoordinator.go passes to MakeCoordinator(). Each mapper should create nReduce intermediate files for consumption by the reduce tasks.

The worker implementation should put the output of the X'th reduce task in the file mr-out-X.

A mr-out-X file should contain one line per Reduce function output. The line should be generated with the Go "%v %v" format, called with the key and value. Have a look in main/mrsequential.go for the line commented "this is the correct format". The test script will fail if your implementation deviates too much from this format.

You can modify mr/worker.go, mr/coordinator.go, and mr/rpc.go. You can temporarily modify other files for testing, but make sure your code works with the original versions; we'll test with the original versions.

The worker should put intermediate Map output in files in the current directory, where your worker can later read them as input to Reduce tasks.

main/mrcoordinator.go expects mr/coordinator.go to implement a Done() method that returns true when the MapReduce job is completely finished; at that point, mrcoordinator.go will exit.

When the job is completely finished, the worker processes should exit. A simple way to implement this is to use the return value from call(): if the worker fails to contact the coordinator, it can assume that the coordinator has exited because the job is done, so the worker can terminate too. Depending on your design, you might also find it helpful to have a "please exit" pseudo-task that the coordinator can give to workers.