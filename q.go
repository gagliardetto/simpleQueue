package simpleQueue

// loosely inspired by http://marcio.io/2015/07/handling-1-million-requests-per-minute-with-golang/

import (
	"errors"
	"fmt"
	"sync"
	"time"
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

	wg              sync.WaitGroup
	quitWorkers     []chan bool
	queueIsQuitting bool

	sync.Mutex
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

		// register the quitChan of the worker in q.quitWorkers registry
		q.quitWorkers = append(q.quitWorkers, worker.quitChan)

		// start worker with Consumer and ErrorCallback
		worker.start(q.Consumer, q.ErrorCallback, &q.wg)
	}

	go q.startManager()
}

// Stop waits for all workers to finish the task they are working on, and then exits
func (q *Queue) Stop() (countOfNonProcessedTasks int) {

	fmt.Println("#####################################################")
	fmt.Println("################### STOPPING QUEUE ##################")
	fmt.Println("#####################################################")

	debugln("@@@ remaining tasks: ", len(q.TaskQueue))

	q.Lock()
	q.queueIsQuitting = true
	q.Unlock()

	for i := range q.quitWorkers {
		go func(i int) {
			debugln("Signaling to worker to quit...; worker ", i+1)
			q.quitWorkers[i] <- true
			debugln("Signal to worker to quit sent; worker ", i+1)
		}(i)
	}

	debugln("@@@ remaining tasks: ", len(q.TaskQueue))

	// close(q.TaskQueue)
	// close(q.workerPool)

	debugln("Waiting for wg to quit...")
	// wait for all workers to finish their current tasks
	if waitTimeout(&q.wg, time.Second*60) {
		debugln("\nTimed out waiting for wg")
	} else {
		debugln("\nwg finished by itself")
	}

	// count not-processed tasks
	countOfNonProcessedTasks = len(q.TaskQueue)

	debugln("@@@ remaining tasks: ", countOfNonProcessedTasks)

	return
}

type worker struct {
	id         int
	taskCart   chan interface{}
	workerPool chan chan interface{}
	quitChan   chan bool
}

// newWorker returns a new initialized worker
func newWorker(id int, workerPool chan chan interface{}) worker {
	return worker{
		id:         id,
		taskCart:   make(chan interface{}),
		workerPool: workerPool,
		quitChan:   make(chan bool),
	}
}

// start starts the worker
func (w *worker) start(consumer func(interface{}) error, errorCallback func(error), wg *sync.WaitGroup) {
	go func(w *worker) {
		// wwg is the worker wait group
		var wwg sync.WaitGroup
		var workerIsQuitting bool

	workLoop:
		for {

			if workerIsQuitting {
				return
			}

			select {
			// Commit this worker's taskCart to the worker pool,
			// making it available to receive tasks.
			case w.workerPool <- w.taskCart:
			case <-time.After(time.Second):
				continue workLoop
			}

			select {
			// Fetch task from taskCart
			case task := <-w.taskCart:
				debugf("\nWorker %v starting task %v", w.id, task)

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
				debugf("\nWorker %v FINISHED task %v", w.id, task)

				// Signal that the task has been processed,
				// and that this worker is not working on any task.
				wwg.Done()
			case <-w.quitChan:
				// We have been asked to stop.
				debugf("\nWorker %d stopping; remaining tasks: %v", w.id, len(w.taskCart))
				w.taskCart = nil

				workerIsQuitting = true

				debugf("\nWorker %v is waiting for current task to complete...", w.id)
				// wait for current task of this worker to be completed
				wwg.Wait()
				debugf("\nWorker %v is now about to exit.", w.id)

				// close(w.taskCart)

				// signal that this worker has finished the current task
				// and currently is not running any tasks.
				wg.Done()
				debugf("\nWorker %v has signaled that it is done with work.", w.id)

				return

			}
		}
	}(w)
}

func (w *worker) stop() {
	go func() {
		w.quitChan <- true
	}()
}

func (q *Queue) startManager() {
	for {

		q.Lock()
		if q.queueIsQuitting {
			q.Unlock()
			return
		}
		q.Unlock()

		select {

		// fetch a task from the TaskQueue of the queue
		case task, ok := <-q.TaskQueue:
			if ok {
				//debugln("taskN:", task.(Task).Name)

				if task == nil {
					continue
				}

				debugf("\nFETCHING workerTaskCart, \n")
				// some tasks will never be assigned, because there will be no workers !!!

				select {
				// fetch a task cart of a worker from the workerPool
				case workerTaskCart, ok := <-q.workerPool:
					//go func() {
					if ok {
						//fmt.Printf("ADDING task to workerTaskCart, %v\n\n", task.(Task).Name)

						// if the workerTaskCart is not nil (nil means the worker is shutting down)
						if workerTaskCart != nil {
							// pass the task to the task cart of the worker
							workerTaskCart <- task
						} else {
							// return the task to the TaskQueue
							go func(task interface{}) {
								q.TaskQueue <- task
							}(task)
							return
						}

					} else {
						//q.workerPool = nil
						debugln("workerpool Channel closed!")
						go func(task interface{}) {
							q.TaskQueue <- task
						}(task)
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

// PushTask pushes a task to the queue
func (q *Queue) PushTask(task interface{}) error {
	var err error
	defer func() {
		if x := recover(); x != nil {
			err = fmt.Errorf("Unable to send: %v", x)
		}
	}()
	q.TaskQueue <- task
	return err
}

// ErrorWriteTimeout happens when the write timeouts
var ErrorWriteTimeout = errors.New("write timeout")

// PushTaskWithTimeout pushes a task to the queue, or timeouts if cannot write to queue
// in the specified time.
func (q *Queue) PushTaskWithTimeout(task interface{}, timeout time.Duration) error {
	var err error
	defer func() {
		if x := recover(); x != nil {
			err = fmt.Errorf("Unable to send: %v", x)
		}
	}()

	select {
	case q.TaskQueue <- task:
	case <-time.After(timeout):
		return ErrorWriteTimeout
	}

	return err
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

// waitTimeout waits for the waitgroup for the specified max timeout.
// Returns true if waiting timed out.
func waitTimeout(wg *sync.WaitGroup, timeout time.Duration) bool {
	c := make(chan struct{})
	go func() {
		defer close(c)
		wg.Wait()
	}()
	select {
	case <-c:
		return false // completed normally
	case <-time.After(timeout):
		return true // timed out
	}
}
