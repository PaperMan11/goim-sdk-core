package parallel

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestRun_AllSuccess(t *testing.T) {
	var count int32
	tasks := make([]int, 20)
	for i := range tasks {
		tasks[i] = i
	}
	err := Run(tasks, 4, func(i int) error {
		atomic.AddInt32(&count, 1)
		return nil
	})
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if atomic.LoadInt32(&count) != 20 {
		t.Fatalf("expected 20 tasks done, got %d", count)
	}
}

func TestRun_FirstErrorReturnedButOthersStillRun(t *testing.T) {
	var count int32
	tasks := make([]int, 10)
	failAt := 0 // 第一个任务就失败，确保 firstErr 一定被设置
	sentinel := errors.New("boom")
	err := Run(tasks, 2, func(i int) error {
		atomic.AddInt32(&count, 1)
		if i == failAt {
			return sentinel
		}
		return nil
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected sentinel error, got %v", err)
	}
	// 即使出错，所有任务都应执行（Run 内部 wg.Wait 等全部完成）
	if atomic.LoadInt32(&count) != 10 {
		t.Fatalf("expected all 10 tasks done even after error, got %d", atomic.LoadInt32(&count))
	}
}

func TestRun_ZeroConcurrence(t *testing.T) {
	var count int32
	tasks := []int{1, 2, 3}
	err := Run(tasks, 0, func(i int) error {
		atomic.AddInt32(&count, 1)
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if atomic.LoadInt32(&count) != 3 {
		t.Fatalf("expected 3, got %d", atomic.LoadInt32(&count))
	}
}

func TestRun_EmptyTasks(t *testing.T) {
	err := Run([]int{}, 4, func(i int) error { return nil })
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestRun_ConcurrencyLimit(t *testing.T) {
	var active, maxActive int32
	tasks := make([]int, 50)
	_ = Run(tasks, 4, func(i int) error {
		cur := atomic.AddInt32(&active, 1)
		for {
			old := atomic.LoadInt32(&maxActive)
			if cur <= old || atomic.CompareAndSwapInt32(&maxActive, old, cur) {
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
		atomic.AddInt32(&active, -1)
		return nil
	})
	if atomic.LoadInt32(&maxActive) > 4 {
		t.Fatalf("concurrency exceeded limit 4, got %d", atomic.LoadInt32(&maxActive))
	}
}

func TestRun_NoTasks(t *testing.T) {
	// nil slice 也应该安全
	err := Run(nil, 4, func(i int) error { return nil })
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}
