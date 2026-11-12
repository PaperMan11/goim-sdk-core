//go:build cgo

package main

/*
#include <stdint.h>
#include <stdlib.h>

typedef void (*goim_success_cb)(char*);
typedef void (*goim_error_cb)(int32_t, char*);
typedef void (*goim_event_cb)(char*);

// 日志回调：level 取 debug/info/warn/error，msg 为已格式化文本。
// C 侧须线程安全且不得阻塞（SDK 在内部 goroutine 上同步调用）。
typedef void (*goim_log_cb)(const char* level, const char* msg);

// 终端模式 token 续期回调：C 侧实现，返回 malloc 的 JSON 字符串
// {"token":"...","expireTimeSeconds":3600}；失败返回 NULL。
// Go 侧会 free 该返回字符串。
typedef char* (*goim_token_provider_cb)(const char* userID, int32_t platformID);

static void goim_invoke_success(goim_success_cb cb, char* s) { if (cb) cb(s); }
static void goim_invoke_error(goim_error_cb cb, int32_t code, char* s) { if (cb) cb(code, s); }
static void goim_invoke_event(goim_event_cb cb, char* s) { if (cb) cb(s); }
static void goim_invoke_log(goim_log_cb cb, char* level, char* msg) { if (cb) cb(level, msg); }
static char* goim_invoke_token_provider(goim_token_provider_cb cb, const char* userID, int32_t platformID) {
    if (!cb) return NULL;
    return cb(userID, platformID);
}
*/
import "C"

import (
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/PaperMan11/goim-sdk-core/pkg/call"
)

// ⚠️ 回调契约：C 侧必须在回调返回前拷贝字符串——Go 侧调用回调后立即 free 该字符串。
//
// 全局单例 Dispatcher（C ABI 无对象概念，单实例足够桌面 SDK 场景）。
var dispatcher = call.NewDispatcher()

// configJSON 见 pkg/call initConfig；返回空串成功，否则错误消息（调用方负责 free）。
//
// 日志：configJSON 的 logging 字段控制开关/级别/本地输出，例如
// {"logging":{"level":"debug","format":"console","outputPaths":["sdk.log"]}}；
// 不配 logging 且未注册 GoimSdkSetLogCallback 时日志静默。
//
//export GoimSdkInit
func GoimSdkInit(configJSON *C.char) *C.char {
	if err := dispatcher.Init(C.GoString(configJSON)); err != nil {
		return C.CString(err.Error())
	}
	return C.CString("")
}

// —— 日志输出 ——

// cLogBridge 把 C 函数指针包装成 call.LogCallback
type cLogBridge struct{ cb C.goim_log_cb }

// OnLog 实现 call.LogCallback（Go 调用回调后立即 free 字符串，C 侧须在回调内拷贝）
func (b *cLogBridge) OnLog(level string, msg string) {
	cl := C.CString(level)
	cm := C.CString(msg)
	C.goim_invoke_log(b.cb, cl, cm)
	C.free(unsafe.Pointer(cl))
	C.free(unsafe.Pointer(cm))
}

//export GoimSdkSetLogCallback
//
// 注册日志回调（C 函数指针），传 NULL 注销；可在 GoimSdkInit 之前或之后调用。
// SDK 日志出口按写入时状态动态分发：已注册回调 → 转发 C 侧；未注册 → 回落到
// init configJSON 的 logging 配置（outputPaths 文件 / stdout，未配置即静默）。
// 日志级别由 logging.level 控制（缺省 info），logging.disable=true 时整体静默。
func GoimSdkSetLogCallback(cb C.goim_log_cb) {
	if cb == nil {
		dispatcher.SetLogCallback(nil)
		return
	}
	dispatcher.SetLogCallback(&cLogBridge{cb: cb})
}

// 异步调用：结果走 onSuccess(resultJSON) / onError(code, msg)
//
//export GoimSdkCall
func GoimSdkCall(method *C.char, argsJSON *C.char, onSuccess C.goim_success_cb, onError C.goim_error_cb) {
	dispatcher.Call(C.GoString(method), C.GoString(argsJSON),
		func(s string) {
			cs := C.CString(s)
			C.goim_invoke_success(onSuccess, cs)
			C.free(unsafe.Pointer(cs))
		},
		func(code int32, msg string) {
			cm := C.CString(msg)
			C.goim_invoke_error(onError, C.int32_t(code), cm)
			C.free(unsafe.Pointer(cm))
		})
}

// 同步调用：返回 JSON 信封 {"code":0,"data":"<resultJSON>"} / {"code":<code>,"msg":"<msg>"}
// （调用方负责 free 返回字符串）
//
//export GoimSdkSyncCall
func GoimSdkSyncCall(method *C.char, argsJSON *C.char) *C.char {
	result, err := dispatcher.SyncCall(C.GoString(method), C.GoString(argsJSON))
	if err != nil {
		code, msg := call.NormalizeError(err)
		bs, _ := json.Marshal(map[string]interface{}{"code": code, "msg": msg})
		return C.CString(string(bs))
	}
	bs, _ := json.Marshal(map[string]interface{}{"code": 0, "data": result})
	return C.CString(string(bs))
}

//export GoimSdkLogin
func GoimSdkLogin(userID *C.char, platformID int32, onSuccess C.goim_success_cb, onError C.goim_error_cb) {
	dispatcher.Login(C.GoString(userID), platformID,
		func() { C.goim_invoke_success(onSuccess, nil) },
		func(code int32, msg string) {
			cm := C.CString(msg)
			C.goim_invoke_error(onError, C.int32_t(code), cm)
			C.free(unsafe.Pointer(cm))
		})
}

//export GoimSdkLoginWithToken
func GoimSdkLoginWithToken(userID *C.char, platformID int32, token *C.char, expireTimeSeconds int64,
	onSuccess C.goim_success_cb, onError C.goim_error_cb) {
	dispatcher.LoginWithToken(C.GoString(userID), platformID, C.GoString(token), expireTimeSeconds,
		func() { C.goim_invoke_success(onSuccess, nil) },
		func(code int32, msg string) {
			cm := C.CString(msg)
			C.goim_invoke_error(onError, C.int32_t(code), cm)
			C.free(unsafe.Pointer(cm))
		})
}

//export GoimSdkLogout
func GoimSdkLogout(clearData int32, onSuccess C.goim_success_cb, onError C.goim_error_cb) {
	dispatcher.Logout(clearData != 0,
		func() { C.goim_invoke_success(onSuccess, nil) },
		func(code int32, msg string) {
			cm := C.CString(msg)
			C.goim_invoke_error(onError, C.int32_t(code), cm)
			C.free(unsafe.Pointer(cm))
		})
}

//export GoimSdkConnect
func GoimSdkConnect(onSuccess C.goim_success_cb, onError C.goim_error_cb) {
	dispatcher.Connect(
		func() { C.goim_invoke_success(onSuccess, nil) },
		func(code int32, msg string) {
			cm := C.CString(msg)
			C.goim_invoke_error(onError, C.int32_t(code), cm)
			C.free(unsafe.Pointer(cm))
		})
}

//export GoimSdkDisconnect
func GoimSdkDisconnect(onSuccess C.goim_success_cb, onError C.goim_error_cb) {
	dispatcher.Disconnect(
		func() { C.goim_invoke_success(onSuccess, nil) },
		func(code int32, msg string) {
			cm := C.CString(msg)
			C.goim_invoke_error(onError, C.int32_t(code), cm)
			C.free(unsafe.Pointer(cm))
		})
}

//export GoimSdkStatus
func GoimSdkStatus() int32 {
	return dispatcher.Status()
}

// —— 终端模式 token 续期 ——

// cgoTokenBridge 把 C 函数指针包装成 call.TokenProviderBridge
type cgoTokenBridge struct {
	cb C.goim_token_provider_cb
}

// FetchToken 同步调用 C 侧 token 刷新函数；C 返回 malloc 的 JSON 字符串，Go 负责 free
func (b *cgoTokenBridge) FetchToken(userID string, platformID int32) (string, error) {
	cUser := C.CString(userID)
	defer C.free(unsafe.Pointer(cUser))
	cResult := C.goim_invoke_token_provider(b.cb, cUser, C.int32_t(platformID))
	if cResult == nil {
		return "", fmt.Errorf("token provider: native returned NULL")
	}
	defer C.free(unsafe.Pointer(cResult))
	return C.GoString(cResult), nil
}

//export GoimSdkSetTokenProvider
//
// 终端模式：注册 token 刷新回调（C 函数指针）。SDK token 过期时同步调用，
// C 侧应向宿主业务服务端请求新 token 并返回 JSON {"token","expireTimeSeconds"}。
// 传 NULL 注销。可在 GoimSdkInit 之后调用（call 层动态读取）。
func GoimSdkSetTokenProvider(cb C.goim_token_provider_cb) {
	if cb == nil {
		dispatcher.SetTokenProvider(nil)
		return
	}
	dispatcher.SetTokenProvider(&cgoTokenBridge{cb: cb})
}

//export GoimSdkUpdateToken
//
// 终端模式预取：C 侧刷新 token 后主动注入（等价于 wasm 的 updateToken）。
// 未注册 SetTokenProvider 时，SDK token 过期刷新会读取最近一次注入的值。
func GoimSdkUpdateToken(userID *C.char, platformID int32, token *C.char, expireTimeSeconds int64) {
	dispatcher.UpdateToken(C.GoString(userID), platformID, C.GoString(token), expireTimeSeconds)
}

// —— 事件订阅 ——

var (
	subSeq  uint64
	subRegs sync.Map // uint64 → func() 取消函数
)

// 订阅指定事件类型，返回订阅 ID（GoimSdkUnsubscribe 取消）。
// onEvent 收到 EventPayload JSON：{"type":<int32>,"timestamp":<ms>,"payload":<json>}
//
//export GoimSdkSubscribe
func GoimSdkSubscribe(eventType int32, onEvent C.goim_event_cb) uint64 {
	unsub := dispatcher.Subscribe(eventType, func(eventJSON string) {
		cs := C.CString(eventJSON)
		C.goim_invoke_event(onEvent, cs)
		C.free(unsafe.Pointer(cs))
	})
	id := atomic.AddUint64(&subSeq, 1)
	subRegs.Store(id, unsub)
	return id
}

//export GoimSdkUnsubscribe
func GoimSdkUnsubscribe(id uint64) {
	if v, ok := subRegs.LoadAndDelete(id); ok {
		v.(func())()
	}
}

// —— 全量事件回调 ——

type cEventCallback struct{ cb C.goim_event_cb }

// OnEvent 实现 call.CallBack：payloadJSON 即 EventPayload JSON（含数值 type 字段）
func (c *cEventCallback) OnEvent(eventType string, payloadJSON string) {
	cs := C.CString(payloadJSON)
	C.goim_invoke_event(c.cb, cs)
	C.free(unsafe.Pointer(cs))
}

// 注册全量事件回调（单点）；重复调用覆盖。传 NULL 注销。
//
//export GoimSdkSetEventCallback
func GoimSdkSetEventCallback(cb C.goim_event_cb) {
	if cb == nil {
		dispatcher.RegisterCallback(nil)
		return
	}
	dispatcher.RegisterCallback(&cEventCallback{cb: cb})
}
