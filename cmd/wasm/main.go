//go:build js && wasm

// Package main Web 平台 syscall/js 入口。
//
// 构建：
//
//	GOOS=js GOARCH=wasm go build -o goim.wasm ./cmd/wasm
//	cp "$(go env GOROOT)/lib/wasm/wasm_exec.js" ./
//
// JS 侧调用（全部挂到 globalThis.goimSdk）：
//
//	goimSdk.init(configJSON, onSuccess, onError)       // 异步：构建 Client（含 IndexedDB 打开）+ 注入 Dispatcher
//	goimSdk.call(method, argsJSON, onSuccess, onError)      // onSuccess(resultJSON), onError(code, msg)
//	goimSdk.syncCall(method, argsJSON)                → {"code":0,"data":"..."} / {"code":x,"msg":"..."}
//
//	⚠️ syncCall 只适用于「纯本地、不阻塞」方法（Status / SyncState / Methods）。
//	   任何走 HTTP / WS / IndexedDB 的方法都必须用 call，否则 Go 阻塞等待
//	   JS Promise 时 JS 主线程正卡在同步调用栈上，形成死锁。
//
//	goimSdk.login(userID, platformID, onSuccess, onError)
//	goimSdk.loginWithToken(userID, platformID, token, expireTimeSeconds, onSuccess, onError)
//	goimSdk.logout(clearData, onSuccess, onError)
//	goimSdk.connect(onSuccess, onError) / goimSdk.disconnect(onSuccess, onError)
//	goimSdk.status()                                  → 0/1/2
//	goimSdk.subscribe(eventType, onEvent)             → 订阅 id；goimSdk.unsubscribe(id)
//	goimSdk.setEventCallback(cb)                      // cb(eventTypeString, payloadJSON)，null 注销
//	goimSdk.updateToken(userID, platformID, token, expireTimeSeconds)   // 预取模式 token 注入
//	goimSdk.shutdown(onDone)                          // 优雅退出：断开 WS + 取消监听 → Go 运行时结束
//
// ⚠️ shutdown 后 Go 运行时返回、wasm 实例退出，goimSdk 上的其它方法全部失效；
//    页面需重新实例化 wasm 才能再初始化。
package main

import (
	"encoding/json"
	"sync"
	"syscall/js"
	"time"

	"github.com/PaperMan11/goim-sdk-core/pkg/call"
)

var (
	dispatcher = call.NewDispatcher()

	subsMu sync.Mutex
	subsID int
	subs   = make(map[int]func())

	// shutdownCh 由 goimSdk.shutdown 触发，main 收到后返回 → go.run resolve → 实例退出
	shutdownCh = make(chan struct{}, 1)
)

func main() {
	js.Global().Set("goimSdk", js.ValueOf(map[string]interface{}{
		"init":             js.FuncOf(initJS),
		"call":             js.FuncOf(callJS),
		"syncCall":         js.FuncOf(syncCallJS),
		"login":            js.FuncOf(loginJS),
		"loginWithToken":   js.FuncOf(loginWithTokenJS),
		"logout":           js.FuncOf(logoutJS),
		"connect":          js.FuncOf(connectJS),
		"disconnect":       js.FuncOf(disconnectJS),
		"status":           js.FuncOf(statusJS),
		"subscribe":        js.FuncOf(subscribeJS),
		"unsubscribe":      js.FuncOf(unsubscribeJS),
		"setEventCallback": js.FuncOf(setEventCallbackJS),
		"updateToken":      js.FuncOf(updateTokenJS),
		"shutdown":         js.FuncOf(shutdownJS),
	}))
	// 保持 main goroutine 常驻，wasm 实例存活。
	// 不能用 select{} / <-ch 这类「无事件源」的阻塞：Go runtime 的 checkdead 只认
	// timer / netpoll / 可运行 goroutine，认不出「以后有人往 channel 发」，会判定
	// "all goroutines are asleep - deadlock" 并直接退出（实测 Go 1.26 wasm）。
	// 所以这里 timer 负责保活、shutdownCh 只作为退出开关。
	// 注意单次 timer 不能超过 JS setTimeout 上限 2^31-1 ms（约 24.8 天），否则会被
	// 当成 1ms 立即触发 → 循环重设，保持永久保活。
	for {
		select {
		case <-shutdownCh:
			// JS 调 goimSdk.shutdown()：main 返回 → go.run resolve → 实例退出
			return
		case <-time.After(24 * time.Hour):
		}
	}
}

// shutdownJS 优雅退出：断开 WS + 取消事件监听，再通知 JS 并结束 Go 运行时。
//
//	onDone 可选，在 Go 退出前回调一次（之后 goimSdk 全部方法失效）。
func shutdownJS(this js.Value, args []js.Value) interface{} {
	onDone := js.Undefined()
	if len(args) > 0 && args[0].Type() == js.TypeFunction {
		onDone = args[0]
	}
	go func() {
		done := make(chan struct{}, 1)
		dispatcher.Disconnect(
			func() { done <- struct{}{} },
			func(int32, string) { done <- struct{}{} })
		select {
		case <-done:
		case <-time.After(3 * time.Second): // 断开超时不阻塞退出
		}
		dispatcher.Close()
		if onDone.Type() == js.TypeFunction {
			onDone.Invoke()
		}
		select {
		case shutdownCh <- struct{}{}:
		default: // 重复 shutdown 幂等
		}
	}()
	return nil
}

// initJS 异步初始化：必须在 goroutine 中执行。
//
// Init 会打开 IndexedDB（awaitPromise 阻塞等 JS Promise），若直接在 js.FuncOf
// 同步回调里阻塞，JS 主线程正卡在调用栈上无法处理微任务 → 互相等待死锁。
func initJS(this js.Value, args []js.Value) interface{} {
	cfg := args[0].String()
	onSuccess, onError := args[1], args[2]
	go func() {
		if err := dispatcher.Init(cfg); err != nil {
			code, msg := call.NormalizeError(err)
			onError.Invoke(code, msg)
			return
		}
		onSuccess.Invoke()
	}()
	return nil
}

func callJS(this js.Value, args []js.Value) interface{} {
	onSuccess, onError := args[2], args[3]
	dispatcher.Call(args[0].String(), args[1].String(),
		func(s string) { onSuccess.Invoke(s) },
		func(code int32, msg string) { onError.Invoke(code, msg) })
	return nil
}

func syncCallJS(this js.Value, args []js.Value) interface{} {
	result, err := dispatcher.SyncCall(args[0].String(), args[1].String())
	if err != nil {
		code, msg := call.NormalizeError(err)
		bs, _ := json.Marshal(map[string]interface{}{"code": code, "msg": msg})
		return string(bs)
	}
	bs, _ := json.Marshal(map[string]interface{}{"code": 0, "data": result})
	return string(bs)
}

func loginJS(this js.Value, args []js.Value) interface{} {
	onSuccess, onError := args[2], args[3]
	dispatcher.Login(args[0].String(), int32(args[1].Int()),
		func() { onSuccess.Invoke() },
		func(code int32, msg string) { onError.Invoke(code, msg) })
	return nil
}

func loginWithTokenJS(this js.Value, args []js.Value) interface{} {
	onSuccess, onError := args[4], args[5]
	dispatcher.LoginWithToken(args[0].String(), int32(args[1].Int()), args[2].String(), int64(args[3].Int()),
		func() { onSuccess.Invoke() },
		func(code int32, msg string) { onError.Invoke(code, msg) })
	return nil
}

func logoutJS(this js.Value, args []js.Value) interface{} {
	onSuccess, onError := args[1], args[2]
	dispatcher.Logout(args[0].Bool(),
		func() { onSuccess.Invoke() },
		func(code int32, msg string) { onError.Invoke(code, msg) })
	return nil
}

func connectJS(this js.Value, args []js.Value) interface{} {
	onSuccess, onError := args[0], args[1]
	dispatcher.Connect(
		func() { onSuccess.Invoke() },
		func(code int32, msg string) { onError.Invoke(code, msg) })
	return nil
}

func disconnectJS(this js.Value, args []js.Value) interface{} {
	onSuccess, onError := args[0], args[1]
	dispatcher.Disconnect(
		func() { onSuccess.Invoke() },
		func(code int32, msg string) { onError.Invoke(code, msg) })
	return nil
}

func statusJS(this js.Value, args []js.Value) interface{} {
	return int(dispatcher.Status())
}

func subscribeJS(this js.Value, args []js.Value) interface{} {
	cb := args[1]
	unsub := dispatcher.Subscribe(int32(args[0].Int()), func(eventJSON string) {
		cb.Invoke(eventJSON)
	})
	subsMu.Lock()
	subsID++
	id := subsID
	subs[id] = unsub
	subsMu.Unlock()
	return id
}

func unsubscribeJS(this js.Value, args []js.Value) interface{} {
	id := args[0].Int()
	subsMu.Lock()
	unsub, ok := subs[id]
	delete(subs, id)
	subsMu.Unlock()
	if ok {
		unsub()
	}
	return nil
}

// jsEventCallback 实现 call.CallBack
type jsEventCallback struct{ cb js.Value }

func (c *jsEventCallback) OnEvent(eventType string, payloadJSON string) {
	c.cb.Invoke(eventType, payloadJSON)
}

func setEventCallbackJS(this js.Value, args []js.Value) interface{} {
	cb := args[0]
	if cb.IsNull() || cb.IsUndefined() {
		dispatcher.RegisterCallback(nil)
		return nil
	}
	dispatcher.RegisterCallback(&jsEventCallback{cb: cb})
	return nil
}

func updateTokenJS(this js.Value, args []js.Value) interface{} {
	dispatcher.UpdateToken(args[0].String(), int32(args[1].Int()), args[2].String(), int64(args[3].Int()))
	return nil
}
