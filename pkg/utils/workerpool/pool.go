package workerpool

import (
	"sync"
)

const (
	DefaultWorkerCount = 4
	DefaultBufferSize  = 1024
)

type WorkerPool[T any] struct {
	taskCh      chan T
	handler     func(T)
	workerCount int
	wg          sync.WaitGroup

	// 防重复 Stop close 已关闭 channel
	stopOnce sync.Once
	stopped  bool
	mu       sync.Mutex
}

func New[T any](handler func(T), workerCount int, bufferSize int) *WorkerPool[T] {
	if workerCount <= 0 {
		workerCount = DefaultWorkerCount
	}
	if bufferSize <= 0 {
		bufferSize = DefaultBufferSize
	}
	return &WorkerPool[T]{
		taskCh:      make(chan T, bufferSize),
		handler:     handler,
		workerCount: workerCount,
	}
}

func (wp *WorkerPool[T]) Start() {
	for i := 0; i < wp.workerCount; i++ {
		wp.wg.Add(1)
		go wp.worker()
	}
}

// Stop 优雅关闭：先 close(taskCh) 让 worker 排空缓冲任务，再 wg.Wait() 等全部退出。
// close(taskCh) 后 Submit/TrySubmit 会被 sync.Once + stopped 标志拦截，不 panic。
func (wp *WorkerPool[T]) Stop() {
	wp.stopOnce.Do(func() {
		wp.mu.Lock()
		wp.stopped = true
		close(wp.taskCh)
		wp.mu.Unlock()
	})
	wp.wg.Wait()
}

func (wp *WorkerPool[T]) worker() {
	defer wp.wg.Done()
	for {
		task, ok := <-wp.taskCh
		if !ok {
			return
		}
		wp.handler(task)
	}
}

func (wp *WorkerPool[T]) WorkerCount() int {
	return wp.workerCount
}

func (wp *WorkerPool[T]) BufferSize() int {
	return cap(wp.taskCh)
}

func (wp *WorkerPool[T]) Submit(task T) {
	wp.mu.Lock()
	defer wp.mu.Unlock()
	if wp.stopped {
		return
	}
	wp.taskCh <- task
}

func (wp *WorkerPool[T]) TrySubmit(task T) bool {
	wp.mu.Lock()
	if wp.stopped {
		wp.mu.Unlock()
		return false
	}
	wp.mu.Unlock()
	select {
	case wp.taskCh <- task:
		return true
	default:
		return false
	}
}
