package workerpool

import (
	"sync"

	"github.com/PaperMan11/goim-sdk-core/pkg/utils/safego"
)

const (
	DefaultWorkerCount = 4
	DefaultBufferSize  = 1024
)

type WorkerPool[T any] struct {
	taskCh      chan T
	handler     func(T)
	workerCount int
	logf        func(format string, args ...interface{})
	wg          sync.WaitGroup

	// 防重复 Stop close 已关闭 channel
	stopOnce sync.Once
	stopped  bool
	mu       sync.Mutex
}

// New 构造 worker 池；logf 接收 worker handler 的 panic 堆栈。
func New[T any](handler func(T), workerCount int, bufferSize int, logf func(format string, args ...interface{})) *WorkerPool[T] {
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
		logf:        logf,
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
	// Recover 先于 wg.Done 执行（LIFO）：handler panic 只损失当前 worker，
	// 不杀进程（wasm 下进程死亡 = SDK 静默失效）
	defer safego.Recover("workerpool.worker", wp.logf)
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
