// Package parallel 提供一次性受限并发执行工具。
//
// 与 workerpool 的区别：
//   - runConcurrent 用完即弃，goroutine 随函数返回自动回收；workerpool 是长生命周期 worker
//   - runConcurrent 返回首个非 nil error（其余任务仍执行完）；workerpool handler 无返回值
//   - runConcurrent 非阻塞投递（goroutine 数 = 任务数，并发度靠 sem 限流）；workerpool Submit 会阻塞
//
// 与 run.Group 的区别：
//   - run.Group 首个 actor 返回后中断其他 actor；runConcurrent 等所有任务完成才返回
//   - run.Group 无并发度限制；runConcurrent 可指定 concurrency
package parallel

import (
	"sync"
)

// Run 受限并发执行 tasks，返回第一个非 nil error（其余任务仍执行完）。
// concurrency<=0 时退化为无并发（顺序执行）。
//
// 典型用途：批量 PUT 分片、批量 API 调用等需要限流但不中断全量的场景。
func Run[T any](tasks []T, concurrency int, fn func(T) error) error {
	if len(tasks) == 0 {
		return nil
	}
	if concurrency <= 0 {
		concurrency = 1
	}
	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup
	var firstErr error
	var errMu sync.Mutex
	for _, t := range tasks {
		wg.Add(1)
		sem <- struct{}{}
		go func(task T) {
			defer wg.Done()
			defer func() { <-sem }()
			if err := fn(task); err != nil {
				errMu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				errMu.Unlock()
			}
		}(t)
	}
	wg.Wait()
	return firstErr
}
