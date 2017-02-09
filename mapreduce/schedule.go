package mapreduce

import (
	"fmt"
	"sync"
)

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//
	
	var wg sync.WaitGroup
	wg.Add(ntasks)

	var worker string
	var args DoTaskArgs
	
	for i := 0; i < ntasks; i = i+1 {
		args = DoTaskArgs{mr.jobName, mr.files[i], phase, i, nios}
		// block wait worker complete last task, or new worker Register
		worker = <-mr.registerChannel

		go mr.dispatch(worker, args, &wg)
	}
	
	wg.Wait()
	fmt.Printf("Schedule: %v phase done2 \n", phase)
}

func (mr *Master) dispatch(worker string, args DoTaskArgs, wg *sync.WaitGroup) {
	//TODO: can not use defer, the bug need fix
	//defer wg.Done()
	Retry:
	ok := call(worker, "Worker.DoTask", &args, new(struct{}))
	wg.Done()
	if ok {
		mr.registerChannel <- worker
	} else {
		//select other worker to handle this task.
		// maybe cause deadlock!!!!!!!!
		// use buffer channel to solve this bug
		worker = <-mr.registerChannel
		wg.Add(1)
		goto Retry
	}
}
