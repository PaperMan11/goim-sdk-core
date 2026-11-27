// Package safego 为后台 goroutine 提供 panic 兜底。
//
// 背景：wasm 场景下任何 goroutine 的 panic 都会杀死整个 Go 运行时
// （go.run resolve → exited=true），此后所有排队中的 JS 侧回调——Go 定时器
// （wasm_exec.js scheduleTimeoutEvent → _resume）、IndexedDB resume、事件推送——
// 都会抛 "Go program has already exited"，表现为 SDK 全部功能静默失效。
// 因此后台 goroutine 一律经 safego.Go 启动或函数首行 defer safego.Recover，
// 把 panic 降级为日志（logf 为 nil 时回落 stderr，浏览器 wasm 下输出到 console）。
//
// 注意：fatal error（如 concurrent map read/write）不可 recover，本包无法拦截；
// 那类问题仍需在代码层面消除数据竞争。
package safego

import (
	"fmt"
	"os"
	"runtime/debug"
)

// Recover 在 defer 中调用：捕获 panic 并输出堆栈。logf 可为 nil（回落 stderr）。
func Recover(what string, logf func(format string, args ...interface{})) {
	r := recover()
	if r == nil {
		return
	}
	msg := fmt.Sprintf("safego: panic recovered in %s: %v\n%s", what, r, debug.Stack())
	if logf != nil {
		logf("%s", msg)
		return
	}
	fmt.Fprintln(os.Stderr, msg)
}

// Go 启动带 panic 兜底的 goroutine（logf 可为 nil）。
func Go(what string, logf func(format string, args ...interface{}), fn func()) {
	go func() {
		defer Recover(what, logf)
		fn()
	}()
}

// Call 同步执行 fn 并把 panic 转为 error（保留堆栈）。
//
// 适用「panic 后必须继续产出结果」的场景，例如 run.Group 的 actor goroutine：
// 直接 Recover 会导致不再向结果 channel 发送、Group.Run 永久阻塞；
// 经 Call 包装后 panic 表现为该 actor 以 error 退出，group 语义完整保留。
func Call(what string, fn func() error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("safego: panic recovered in %s: %v\n%s", what, r, debug.Stack())
		}
	}()
	return fn()
}
