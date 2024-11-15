package mr

import (
	"fmt"
)

type JobType uint8

const (
	MapJob JobType = iota
	ReduceJob
	WaitJob
	CompleteJob
)

func (j JobType) String() string {
	switch j {
	case MapJob:
		return "map"
	case ReduceJob:
		return "reduce"
	case WaitJob:
		return "wait"
	case CompleteJob:
		return "complete"
	default:
		panic(fmt.Sprintf("unknown job type %d", j))
	}
}

func generateMapResultFileName(mapNumber, reduceNumber int) string {
	return fmt.Sprintf("mr-%d-%d", mapNumber, reduceNumber)
}

type SchedulePhase uint8

const (
	MapPhase SchedulePhase = iota
	ReducePhase
	CompletePhase
)

func (phase SchedulePhase) String() string {
	switch phase {
	case MapPhase:
		return "MapPhase"
	case ReducePhase:
		return "ReducePhase"
	case CompletePhase:
		return "CompletePhase"
	}
	panic(fmt.Sprintf("unexpected SchedulePhase %d", phase))
}

type TaskStatus uint8

const (
	Idle TaskStatus = iota
	Working
	Finished
)
