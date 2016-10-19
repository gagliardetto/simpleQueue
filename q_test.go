package simpleQueue

import (
	"fmt"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCounter1(t *testing.T) {
	var counter int64

	maxQueueSize := 100
	maxWorkers := 5

	type TaskStruct struct {
		IncrementBy int64
	}

	consumer := func(t interface{}) error {
		task := t.(TaskStruct)

		atomic.AddInt64(&counter, task.IncrementBy)
		time.Sleep(time.Second)
		return nil
	}

	newQueue := NewQueue().
		SetMaxSize(maxQueueSize).
		SetWorkers(maxWorkers).
		SetConsumer(consumer).
		SetErrorCallback(func(err error) {
			fmt.Printf("error while processing task: %v", err)
		})

	newQueue.Start()
	defer newQueue.Stop()

	newQueue.TaskQueue <- TaskStruct{
		IncrementBy: 1,
	}
	newQueue.TaskQueue <- TaskStruct{
		IncrementBy: 7,
	}
	newQueue.TaskQueue <- TaskStruct{
		IncrementBy: 22,
	}

	time.Sleep(time.Second * 2)

	//fmt.Printf("counter value is: %v", counter)
	assert.Equal(t, int64(30), counter, "they should be equal")
}

func TestCounter2(t *testing.T) {
	var counter int64

	maxQueueSize := 100
	maxWorkers := 5

	type TaskStruct struct {
		IncrementBy int64
	}

	consumer := func(t interface{}) error {
		task := t.(TaskStruct)

		fmt.Printf("\ncounter+%v\n", task.IncrementBy)
		atomic.AddInt64(&counter, task.IncrementBy)
		time.Sleep(time.Second)
		return nil
	}

	newQueue := NewQueue().
		SetMaxSize(maxQueueSize).
		SetWorkers(maxWorkers).
		SetConsumer(consumer).
		SetErrorCallback(func(err error) {
			fmt.Printf("error while processing task: %v", err)
		})

	newQueue.Start()

	defer func() {
		notProcessedTasksCount := newQueue.Stop()
		fmt.Printf("############### notProcessedTasksCount: %v ###########\n", notProcessedTasksCount)
	}()

	newQueue.TaskQueue <- TaskStruct{
		IncrementBy: 1,
	}
	newQueue.TaskQueue <- TaskStruct{
		IncrementBy: 7,
	}
	newQueue.TaskQueue <- TaskStruct{
		IncrementBy: 22,
	}

	// wait the time to initialize everything
	time.Sleep(time.Millisecond * 10)

	assert.Equal(t, int64(30), counter, "they should be equal")
}

func TestCounter3(t *testing.T) {
	var counter int64

	maxQueueSize := 100
	maxWorkers := 1

	type TaskStruct struct {
		IncrementBy int64
	}

	consumer := func(t interface{}) error {
		task := t.(TaskStruct)

		fmt.Printf("\ncounter+%v\n", task.IncrementBy)
		atomic.AddInt64(&counter, task.IncrementBy)
		time.Sleep(time.Second)
		return nil
	}

	newQueue := NewQueue().
		SetMaxSize(maxQueueSize).
		SetWorkers(maxWorkers).
		SetConsumer(consumer).
		SetErrorCallback(func(err error) {
			fmt.Printf("error while processing task: %v", err)
		})

	newQueue.Start()

	defer func() {
		notProcessedTasksCount := newQueue.Stop()
		fmt.Printf("############### notProcessedTasksCount: %v ###########\n", notProcessedTasksCount)
	}()

	newQueue.TaskQueue <- TaskStruct{
		IncrementBy: 1,
	}
	newQueue.TaskQueue <- TaskStruct{
		IncrementBy: 7,
	}
	newQueue.TaskQueue <- TaskStruct{
		IncrementBy: 22,
	}

	// wait the time to initialize everything
	time.Sleep(time.Millisecond * 1)

	assert.Equal(t, int64(1), counter, "they should be equal")
}

func TestCounter4(t *testing.T) {
	var counter int64

	maxQueueSize := 1000
	maxWorkers := 10

	tasksToAddToQueue := 1000
	var defaultIncrementBy int64 = 22

	type TaskStruct struct {
		IncrementBy int64
	}

	consumer := func(t interface{}) error {
		task := t.(TaskStruct)

		fmt.Printf("\ncounter+%v\n", task.IncrementBy)
		atomic.AddInt64(&counter, task.IncrementBy)
		time.Sleep(time.Second * 3)
		return nil
	}

	newQueue := NewQueue().
		SetMaxSize(maxQueueSize).
		SetWorkers(maxWorkers).
		SetConsumer(consumer).
		SetErrorCallback(func(err error) {
			fmt.Printf("error while processing task: %v", err)
		})

	newQueue.Start()

	defer func() {
		notProcessedTasksCount := newQueue.Stop()
		fmt.Printf("############### notProcessedTasksCount: %v ###########\n", notProcessedTasksCount)
	}()

	for i := 0; i <= tasksToAddToQueue; i++ {
		/*
		   WARNING: if taskCount > maxQueueSize, the adding to queue will block,
		   and this loop will run until all tasks have been added to the queue.
		*/
		newQueue.TaskQueue <- TaskStruct{
			IncrementBy: defaultIncrementBy,
		}
	}

	// wait the time to initialize everything
	time.Sleep(time.Millisecond * 1)

	assert.Equal(t, int64(defaultIncrementBy*int64(maxWorkers)), counter, "they should be equal")
}

func TestCountNotProcessedTasks(t *testing.T) {
	var counter int64

	maxQueueSize := 1000
	maxWorkers := 10

	tasksToAddToQueue := 1000
	var defaultIncrementBy int64 = 22

	type TaskStruct struct {
		IncrementBy int64
	}

	consumer := func(t interface{}) error {
		task := t.(TaskStruct)

		fmt.Printf("\ncounter+%v\n", task.IncrementBy)
		atomic.AddInt64(&counter, task.IncrementBy)
		time.Sleep(time.Second * 1)
		return nil
	}

	newQueue := NewQueue().
		SetMaxSize(maxQueueSize).
		SetWorkers(maxWorkers).
		SetConsumer(consumer).
		SetErrorCallback(func(err error) {
			fmt.Printf("error while processing task: %v", err)
		})

	newQueue.Start()

	for i := 0; i <= tasksToAddToQueue; i++ {
		/*
		   WARNING: if taskCount > maxQueueSize, the adding to queue will block,
		   and this loop will run until all tasks have been added to the queue.
		*/
		newQueue.TaskQueue <- TaskStruct{
			IncrementBy: defaultIncrementBy,
		}
	}

	// wait the time to initialize everything
	time.Sleep(time.Millisecond * 1)

	assert.Equal(t, int64(defaultIncrementBy*int64(maxWorkers)), counter, "they should be equal")

	notProcessedTasksCount := newQueue.Stop()
	fmt.Printf("############### notProcessedTasksCount: %v ###########\n", notProcessedTasksCount)

	assert.Equal(t, int(tasksToAddToQueue-maxWorkers), notProcessedTasksCount, "they should be equal")
}

func randomInt(min, max int) int {
	rand.Seed(time.Now().Unix())
	return rand.Intn(max-min) + min
}
