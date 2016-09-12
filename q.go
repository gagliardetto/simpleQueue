package simpleQueue

import (
	"fmt"
	"sync"

	"sync/atomic"
	"time"
)

type Worker struct {
	id         int
	taskQueue  chan interface{}
	workerPool chan chan interface{}
	quitChan   chan bool
}

// newWorker creates takes a numeric id and a channel w/ worker pool.
func newWorker(id int, workerPool chan chan interface{}) Worker {
	return Worker{
		id:         id,
		taskQueue:  make(chan interface{}),
		workerPool: workerPool,
		quitChan:   make(chan bool),
	}
}

func (w Worker) stop() {
	go func() {
		w.quitChan <- true
	}()
}

type Queue struct {
	TaskQueue chan interface{}
	Consumer  func(interface{}) error

	workerPool   chan chan interface{}
	maxQueueSize int
	maxWorkers   int

	wg    sync.WaitGroup
	quits []chan bool
}

func NewQueue() *Queue {
	return &Queue{
		maxQueueSize: 100,
		maxWorkers:   5,
	}
}

func (q *Queue) SetMaxSize(i int) *Queue {
	if i >= 1 {
		q.maxQueueSize = i
	}
	return q
}

func (q *Queue) SetWorkers(i int) *Queue {
	if i >= 1 {
		q.maxWorkers = i
	}
	return q
}

func (q *Queue) SetConsumer(consumer func(interface{}) error) *Queue {
	if consumer != nil {
		q.Consumer = consumer
	}
	return q
}

func (q *Queue) Start() {
	if q.Consumer == nil {
		panic("please set a consumer; cansumer cannot be nil")
	}

	q.TaskQueue = make(chan interface{}, q.maxQueueSize)
	q.workerPool = make(chan chan interface{}, q.maxWorkers)

	for i := 1; i <= q.maxWorkers; i++ {
		q.wg.Add(1)
		worker := newWorker(i, q.workerPool)
		q.quits = append(q.quits, worker.quitChan)
		worker.start(q.Consumer, &q.wg)
	}

	go q.dispatch()
}

func (w Worker) start(consumer func(interface{}) error, wg *sync.WaitGroup) {
	go func() {
		var wwg sync.WaitGroup
		for {
			// Add my taskQueue to the worker pool.
			w.workerPool <- w.taskQueue

			select {
			case task := <-w.taskQueue:
				// Dispatcher has added a task to my taskQueue.
				//fmt.Printf("worker%v starting task %v\n", w.id, task.(Task).Name)
				wwg.Add(1)
				err := consumer(task)
				if err != nil {
					fmt.Println("consumer error:", err)
				}
				//fmt.Printf("worker%v FINISHED task %v\n\n", w.id, task.(Task).Name)
				wwg.Done()
			case <-w.quitChan:
				// We have been asked to stop.
				debugf("worker%d stopping; remaining: %v\n", w.id, len(w.taskQueue))
				w.taskQueue = nil
				wwg.Wait()
				// close(w.taskQueue)
				wg.Done()
				return
			}
		}
	}()
}

func (q *Queue) dispatch() {
	for {

		select {
		case task, ok := <-q.TaskQueue:
			if ok {
				//debugln("taskN:", task.(Task).Name)

				debugf("\nFETCHING workerTaskQueue, \n")
				// some tasks will never be assigned, because there will be no workers !!!
				select {
				case workerTaskQueue, ok := <-q.workerPool:
					//go func() {
					if ok {
						//fmt.Printf("ADDING task to workerTaskQueue, %v\n\n", task.(Task).Name)

						if workerTaskQueue != nil {
							workerTaskQueue <- task
						} else {
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
	fmt.Println("#####################################################")
	fmt.Println("#####################################################")

	debugln("@@@ remaining: ", len(q.TaskQueue))

	for i := range q.quits {
		q.quits[i] <- true
	}

	debugln("@@@ remaining: ", len(q.TaskQueue))

	// close(q.TaskQueue)
	// close(q.workerPool)

	q.wg.Wait()

	notProcessed = len(q.TaskQueue)

	return
}

var debugging = false

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

////////////////////////////////////

func Example() {
	var (
		maxWorkers   = 10
		maxQueueSize = 1000
	)

	qq := NewQueue()
	qq.SetMaxSize(maxQueueSize)
	qq.SetWorkers(maxWorkers)
	qq.Consumer = Consumer
	qq.Start()

	go func() {
	R:
		for i := 1; i <= 1000; i++ {
			select {
			case qq.TaskQueue <- Task{Name: fmt.Sprintf("%v", i), Delay: time.Millisecond * 330}:
			default:
				break R
				fmt.Println("@@@@@@@@ cannot send")
				fmt.Println("@@@@@@@@ cannot send")
				fmt.Println("@@@@@@@@ cannot send")
				fmt.Println("@@@@@@@@ cannot send")
			}
		}
	}()

	time.Sleep(time.Millisecond * 3100)
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
