package simpleQueue

import (
	"fmt"
	"sync"
)

const (
	debugging           = false
	defaultMaxQueueSize = 100
	defaultMaxWorkers   = 5
)

// Queue is the main queue object
type Queue struct {
	TaskQueue     chan interface{}
	Consumer      func(interface{}) error
	ErrorCallback func(error)

	workerPool   chan chan interface{}
	maxQueueSize int
	maxWorkers   int

	wg    sync.WaitGroup
	quits []chan bool
}

// NewQueue return a new queue object loaded with some default values
func NewQueue() *Queue {
	return &Queue{
		maxQueueSize: defaultMaxQueueSize,
		maxWorkers:   defaultMaxWorkers,
	}
}

// SetMaxSize is used to set the max capacity of the queue (buffer length)
func (q *Queue) SetMaxSize(i int) *Queue {
	if i >= 1 {
		q.maxQueueSize = i
	}
	return q
}

// SetWorkers is used to set the number of workers that process tasks from the queue
func (q *Queue) SetWorkers(i int) *Queue {
	if i >= 1 {
		q.maxWorkers = i
	}
	return q
}

// SetConsumer is used to set the cosumer function that workers execute
// when received a new task.
func (q *Queue) SetConsumer(consumer func(interface{}) error) *Queue {
	if consumer != nil {
		q.Consumer = consumer
	}
	return q
}

// SetErrorCallback is used to set the function that will be run when
// the consumer function returns an error.
func (q *Queue) SetErrorCallback(callback func(error)) *Queue {
	if callback != nil {
		q.ErrorCallback = callback
	}
	return q
}

// Start starts workers which now wait for tasks
func (q *Queue) Start() {
	if q.Consumer == nil {
		panic("please set a Consumer function; Consumer cannot be nil")
	}
	if q.ErrorCallback == nil {
		panic("please set a ErrorCallback; ErrorCallback cannot be nil")
	}

	// initialize TaskQueue
	q.TaskQueue = make(chan interface{}, q.maxQueueSize)

	// initialize pool of workers
	q.workerPool = make(chan chan interface{}, q.maxWorkers)

	// create workers and link them to the pool
	for i := 1; i <= q.maxWorkers; i++ {
		q.wg.Add(1)

		// create new worker and link it to pool
		worker := newWorker(i, q.workerPool)

		// register the quitChan of the worker in q.quits registry
		q.quits = append(q.quits, worker.quitChan)

		// start worker with Consumer and ErrorCallback
		worker.start(q.Consumer, q.ErrorCallback, &q.wg)
	}

	go q.dispatch()
}

type worker struct {
	id         int
	taskQueue  chan interface{}
	workerPool chan chan interface{}
	quitChan   chan bool
}

// newWorker returns a new initialized worker
func newWorker(id int, workerPool chan chan interface{}) worker {
	return worker{
		id:         id,
		taskQueue:  make(chan interface{}),
		workerPool: workerPool,
		quitChan:   make(chan bool),
	}
}

// start starts the worker
func (w worker) start(consumer func(interface{}) error, errorCallback func(error), wg *sync.WaitGroup) {
	go func() {
		// wwg is the worker wait group
		var wwg sync.WaitGroup
		for {
			// Commit this worker's taskQueue to the worker pool,
			// making it available to receive tasks.
			w.workerPool <- w.taskQueue

			select {
			// Fetch task from taskQueue
			case task := <-w.taskQueue:
				//fmt.Printf("worker%v starting task %v\n", w.id, task.(Task).Name)

				// make known that this worker is processing a task
				wwg.Add(1)

				// process the task with the consumer function
				err := consumer(task)
				if err != nil {
					if errorCallback != nil {
						// in case of error: pass the error to the errorCallback
						errorCallback(err)
					}
				}
				//fmt.Printf("worker%v FINISHED task %v\n\n", w.id, task.(Task).Name)

				// Signal that the task has been processed,
				// and that this worker is not working on any task.
				wwg.Done()
			case <-w.quitChan:
				// We have been asked to stop.
				debugf("worker%d stopping; remaining: %v\n", w.id, len(w.taskQueue))
				w.taskQueue = nil

				// wait for current task of this worker to be completed
				wwg.Wait()

				// close(w.taskQueue)

				// signal that this worker has finished the current task
				// and currently is not running any tasks.
				wg.Done()

				return
			}
		}
	}()
}

func (w worker) stop() {
	go func() {
		w.quitChan <- true
	}()
}

func (q *Queue) dispatch() {
	for {

		select {

		// fetch a task from the TaskQueue of the queue
		case task, ok := <-q.TaskQueue:
			if ok {
				//debugln("taskN:", task.(Task).Name)

				debugf("\nFETCHING workerTaskQueue, \n")
				// some tasks will never be assigned, because there will be no workers !!!

				select {
				// fetch a task queue of a worker from the workerPool
				case workerTaskQueue, ok := <-q.workerPool:
					//go func() {
					if ok {
						//fmt.Printf("ADDING task to workerTaskQueue, %v\n\n", task.(Task).Name)

						// if the workerTaskQueue is not nil (nil means the worker is shutting down)
						if workerTaskQueue != nil {
							// pass the task to the task queue of the worker
							workerTaskQueue <- task
						} else {
							// return the task to the TaskQueue
							q.TaskQueue <- task
							return
						}

					} else {
						q.workerPool = nil
						debugln("workerpool Channel closed!")
						return
					}
					//}()
					//default:
					//fmt.Println("No worker ready, moving on.")
					//	go func() {
					// Add task to backburner, where all the tasks that
					// can't be completed (because no worker is ready) go.
					//	}()
				}

				if q.workerPool == nil {
					break
				}

			} else {
				debugln("task Channel closed!")
				return
			}
			//default:
			//fmt.Println("No task ready, moving on.")

		}

	}
}

// Stop waits for all workers to finish the task they are working on, and then exits
func (q *Queue) Stop() (notProcessed int) {

	fmt.Println("#####################################################")
	fmt.Println("################### STOPPING QUEUE ##################")
	fmt.Println("#####################################################")

	debugln("@@@ remaining: ", len(q.TaskQueue))

	for i := range q.quits {
		q.quits[i] <- true
	}

	debugln("@@@ remaining: ", len(q.TaskQueue))

	// close(q.TaskQueue)
	// close(q.workerPool)

	// wait for all workers to finish their current tasks
	q.wg.Wait()

	// count not-processed tasks
	notProcessed = len(q.TaskQueue)

	return
}

// @@@@@@@@@@@@@@@ Utils for debugging @@@@@@@@@@@@@@@

func debugf(format string, a ...interface{}) (int, error) {
	if debugging {
		return fmt.Printf(format, a...)
	}
	return 0, nil
}

func debugln(a ...interface{}) (int, error) {
	if debugging {
		return fmt.Println(a...)
	}
	return 0, nil
}
