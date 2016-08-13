package pool

import (
	"github.com/konjoot/blurr/hooks"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPool_Basic(t *testing.T) {
	var err error
	workers := make(chan struct{}, 11)
	started := make(chan struct{})

	hooks.OnWorkerStart = func(int) {
		workers <- struct{}{}
		started <- struct{}{}
	}
	hooks.OnWorkerExit = func(int) {
		<-workers
	}

	expect := assert.New(t)

	_, err = New(0, false)
	expect.Equal(ErrWrongPoolSize, err)

	_, err = New(1001, false)
	expect.Equal(ErrWrongPoolSize, err)

	// starting pool
	pool, err := New(10, false)
	expect.Nil(err)

	// waiting for workers
	for i := 0; i < 10; i++ {
		<-started
	}

	expect.Equal(10, len(workers))

	// cancelling pool and waiting for workers
	pool.Cancel()
	pool.Wait()

	expect.Equal(0, len(workers))
}

func TestPool_WorkingWithTasks(t *testing.T) {
	started := make(chan struct{})
	finished := make(chan int)
	done := make(chan struct{})

	hooks.Reset()

	hooks.OnWorkerStart = func(int) {
		started <- struct{}{}
	}
	hooks.OnTaskFinish = func(i int) {
		finished <- i
	}

	// starting pool
	expect := assert.New(t)
	pool, err := New(10, false)
	expect.Nil(err)

	// waiting for workers
	for i := 0; i < 10; i++ {
		<-started
	}

	tasks := make([]*fakeTask, 0, 20)

	go func() {
		// push tasks to the pool
		for i := 0; i < 20; i++ {
			task := &fakeTask{count: counter(0)}
			pool.In <- task
			tasks = append(tasks, task)
		}

		done <- struct{}{}
	}()

	workers := make(map[int]bool)
	// waiting for work to finish
	for i := 0; i < 20; i++ {
		c := <-finished
		if _, ok := workers[c]; !ok {
			workers[c] = true
		}
	}

	// check that tasks performed by different workers
	for i := 0; i < 10; i++ {
		val, ok := workers[i]
		expect.True(val)
		expect.True(ok)
	}

	<-done

	for _, task := range tasks {
		expect.True(task.performed)
	}

	// cancelling pool and waiting for workers
	pool.Cancel()
	pool.Wait()
}

func TestPool_WorkingGreedyWithTasks(t *testing.T) {
	started := make(chan struct{})
	finished := make(chan int, 10)

	hooks.Reset()

	hooks.OnWorkerStart = func(int) {
		started <- struct{}{}
	}
	hooks.OnTaskFinish = func(i int) {
		finished <- i
	}

	// starting pool
	expect := assert.New(t)
	pool, err := New(10, true)
	expect.Nil(err)

	// waiting for workers
	for i := 0; i < 10; i++ {
		<-started
	}

	task := &fakeTask{count: counter(19)}

	// push task to the pool
	pool.In <- task

	workers := make(map[int]bool)
	// waiting for work to finish
	for i := 0; i < 20; i++ {
		c := <-finished
		if _, ok := workers[c]; !ok {
			workers[c] = true
		}
	}

	// check that tasks performed by different workers
	for i := 0; i < 10; i++ {
		val, ok := workers[i]
		expect.True(val)
		expect.True(ok)
	}

	expect.True(task.performed)
	expect.Zero(<-task.count)

	// cancelling pool and waiting for workers
	pool.Cancel()
	pool.Wait()
}

type fakeTask struct {
	count     chan int
	performed bool
}

func (t *fakeTask) Perform() int {
	t.performed = true

	return <-t.count
}

func counter(count int) chan int {
	ch := make(chan int, count)

	for i := count; i > 0; i-- {
		ch <- i
	}

	close(ch)

	return ch
}
