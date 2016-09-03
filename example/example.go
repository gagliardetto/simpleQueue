package main

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/gagliardetto/q"
)

func main() {
	var (
		maxWorkers   = 100
		maxQueueSize = 1000
	)

	qq := q.NewQueue()
	qq.SetMaxSize(maxQueueSize)
	qq.SetWorkers(maxWorkers)
	qq.Consumer = Consumer
	qq.Start()

	for i := 1; i <= 10000; i++ {
		qq.TaskQueue <- Task{Name: fmt.Sprintf("%v", i), Delay: time.Millisecond * 100}
	}

	//time.Sleep(time.Second * 3)
	/*
		for {
			m, ok := <-qq.TaskQueue
			if ok {
				fmt.Println("not proc: ", m.(Task).Name)
			}
		}
	*/
	rem := qq.Stop()
	fmt.Println("remaining: ", rem)
	fmt.Println("counter: ", counter)

	//////////////////////////////////////////////////////////
	/*
		// Create the task queue.
		taskQueue := make(chan Task, maxQueueSize)

		// Start the dispatcher.
		dispatcher := NewDispatcher(taskQueue, maxWorkers)
		dispatcher.run()
	*/
	/*
		// Create Task and push the work onto the taskQueue.
		task := Task{Name: name, Delay: delay}
		taskQueue <- task
	*/
}

// Task holds the attributes needed to perform unit of work
type Task struct {
	Name  string
	Delay time.Duration
}

var counter int64 = 0

func Consumer(j interface{}) error {
	if j == nil {
		return fmt.Errorf("%v", "j is nil")
	}
	task := j.(Task)

	//fmt.Printf("%v --> name: %v; delay: %v\n", counter, task.Name, task.Delay)
	time.Sleep(task.Delay)
	atomic.AddInt64(&counter, 1)

	return nil
}
