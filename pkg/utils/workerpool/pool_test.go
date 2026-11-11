package workerpool

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestWorkerPool_Submit(t *testing.T) {
	var count int32
	pool := New(func(task int) {
		atomic.AddInt32(&count, 1)
	}, 2, 10)
	pool.Start()
	defer pool.Stop()

	for i := 0; i < 100; i++ {
		pool.Submit(i)
	}

	time.Sleep(100 * time.Millisecond)
	if atomic.LoadInt32(&count) != 100 {
		t.Errorf("expected count 100, got %d", atomic.LoadInt32(&count))
	}
}

func TestWorkerPool_TrySubmit(t *testing.T) {
	var count int32
	pool := New(func(task int) {
		time.Sleep(10 * time.Millisecond)
		atomic.AddInt32(&count, 1)
	}, 1, 2)
	pool.Start()
	defer pool.Stop()

	for i := 0; i < 5; i++ {
		if !pool.TrySubmit(i) {
			atomic.AddInt32(&count, 1)
		}
	}

	time.Sleep(100 * time.Millisecond)
	if atomic.LoadInt32(&count) != 5 {
		t.Errorf("expected count 5, got %d", atomic.LoadInt32(&count))
	}
}

func TestWorkerPool_GracefulShutdown(t *testing.T) {
	var count int32
	pool := New(func(task int) {
		time.Sleep(50 * time.Millisecond)
		atomic.AddInt32(&count, 1)
	}, 1, 10)
	pool.Start()

	for i := 0; i < 5; i++ {
		pool.Submit(i)
	}

	pool.Stop()

	if atomic.LoadInt32(&count) != 5 {
		t.Errorf("expected count 5 (all tasks drained), got %d", atomic.LoadInt32(&count))
	}
}

func TestWorkerPool_WorkerCount(t *testing.T) {
	pool := New(func(task int) {}, 5, 10)
	if pool.WorkerCount() != 5 {
		t.Errorf("expected worker count 5, got %d", pool.WorkerCount())
	}
}

func TestWorkerPool_BufferSize(t *testing.T) {
	pool := New(func(task int) {}, 2, 100)
	if pool.BufferSize() != 100 {
		t.Errorf("expected buffer size 100, got %d", pool.BufferSize())
	}
}
