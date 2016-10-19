package simpleQueue

import (
	"fmt"
	"sync/atomic"
	"time"
)

func ExampleBasic() {
	maxQueueSize := 100
	maxWorkers := 5

	type TaskStruct struct {
		Name string
	}

	consumer := func(t interface{}) error {
		task := t.(TaskStruct)

		fmt.Println(task.Name)
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
		Name: "Al",
	}
	newQueue.TaskQueue <- TaskStruct{
		Name: "John",
	}
	newQueue.TaskQueue <- TaskStruct{
		Name: "Jack",
	}

}

func ExampleCounter() {
	var counter int64

	maxQueueSize := 100
	maxWorkers := 5

	type TaskStruct struct {
		IncrementBy int64
	}

	consumer := func(t interface{}) error {
		task := t.(TaskStruct)

		fmt.Printf("/ncounter+%v/n", task.IncrementBy)
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

	fmt.Printf("counter value is: %v", counter)
}
