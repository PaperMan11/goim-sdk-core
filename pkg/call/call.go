// Package call 跨语言反射调用内核 —— gomobile / cgo / wasm 三端共用的薄适配层。
//
// 为什么需要这一层：gomobile / cgo / wasm 只能传 Go 基本类型
// （string / int / bool / []byte），不能传 channel、泛型、复杂 interface。
// pkg/client 的 Client 接口返回 (*model.XxxResp, error)、<-chan events.Event
// 这些类型在跨语言边界都无法直接导出，所以 call 层做统一转换：
//
//	Native 侧                    call 层                      core 层
//	  │  method string + argsJSON   │  反射分发 + JSON 编解码      │  类型安全调用
//	  │ ──────────────────────────→ │ ──────────────────────────→ │
//	  │  onSuccess(json) /          │                             │
//	  │  onError(code, msg)         │ ←────────────────────────── │
//	  │ ←────────────────────────── │                             │
//
// 跨语言契约（全部 gomobile 可绑定）：
//   - 方法标识：method string，如 "MsgMgr.SendMsg"、"Login"
//   - 参数：argsJSON 为 JSON 数组，按方法签名位置对齐（见 converter.go）
//   - 返回值 / 事件载荷：JSON string
//   - 错误：归一化为 (code int32, msg string)（见 error.go）
//
// 本层不参与 core 层设计，是纯增量适配层（docs/DEVELOPMENT.md §5.2）。
package call

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/PaperMan11/goim-sdk-core/pkg/client"
	"github.com/PaperMan11/goim-sdk-core/pkg/events"
	"github.com/PaperMan11/goim-sdk-core/pkg/logger"
	"github.com/PaperMan11/goim-sdk-core/pkg/manager"
	"github.com/PaperMan11/goim-sdk-core/pkg/model"
	"github.com/PaperMan11/goim-sdk-core/pkg/transport"
)

// Dispatcher 反射分发器（全局单例，由 NewDispatcher 构造）
//
// 线程安全：全部导出方法可并发调用。
// gomobile 导出后对应 iOS ObjC 类 / Android Java 类。
type Dispatcher struct {
	mu       sync.RWMutex
	cli      client.Client
	methods  map[string]reflect.Value // method string → bound func（首参 ctx）
	loggedIn bool

	callTimeout time.Duration // 单次反射调用 ctx 超时（默认 defaultCallTimeout）

	// TokenProvider 桥（须在 Init 前设置，见 token.go）
	tokenProvider TokenProviderBridge
	tokenCache    prefetchedTokenCache // UpdateToken 预取缓存（无桥时的 TokenProvider 数据源）

	// 内部事件监听（登录态跟踪）取消函数
	watchCancel context.CancelFunc

	// 全量事件回调（RegisterCallback 注册；SetClient 后自动重绑）
	cb       CallBack
	cbCancel context.CancelFunc

	// 日志回调（SetLogCallback 注册，Init 构建的日志出口按写入时状态动态分发）
	logCB LogCallback
	// Init 构建的日志出口（Close 时释放其本地文件句柄；SetClient 场景为 nil）
	logSink logger.Logger
}

// defaultCallTimeout 单次反射调用的默认超时
const defaultCallTimeout = 30 * time.Second

// NewDispatcher 构造 Dispatcher。
//
// 之后二选一：
//   - Init(configJSON)：由 call 层内部构建 Client（gomobile / wasm 场景）
//   - SetClient(c)：注入外部已构建的 Client（Go 内嵌 / cgo 场景）
func NewDispatcher() *Dispatcher {
	return &Dispatcher{methods: make(map[string]reflect.Value), callTimeout: defaultCallTimeout}
}

// SetCallTimeoutSec 设置单次反射调用的超时秒数（默认 30s，须 >0）
func (d *Dispatcher) SetCallTimeoutSec(sec int64) {
	if sec <= 0 {
		return
	}
	d.mu.Lock()
	d.callTimeout = time.Duration(sec) * time.Second
	d.mu.Unlock()
}

// initConfig Init 的 JSON 配置（client.ClientConfig 的可序列化子集）
type initConfig struct {
	BaseURL         string         `json:"baseURL"`         // HTTP API 基础地址（含 /api/v1 前缀）
	Endpoint        string         `json:"endpoint"`        // WS 连接地址（ws:// / wss://）
	PlatformID      int32          `json:"platformID"`      // 默认平台 ID
	DeviceID        string         `json:"deviceID"`        // 设备 ID（空则自动生成并持久化）
	SDKVersion      string         `json:"sdkVersion"`      // SDK 版本号（空用编译期 Version）
	Compression     string         `json:"compression"`     // 压缩算法（none / gzip / zlib）
	SessionDataPath string         `json:"sessionDataPath"` // SQLite 文件路径（空用内存存储；wasm 不支持）
	AdminSecret     string         `json:"adminSecret"`     // 应用密钥（仅受信环境；终端模式用 LoginWithToken）
	AdminUserID     string         `json:"adminUserID"`     // 管理员账号（默认 imAdmin）
	Logging         *logger.Config `json:"logging"`         // 日志开关/级别/本地输出（nil 时仅回调节点生效，无回调即静默）
}

// Init 按 JSON 配置构建 Client 并注入 Dispatcher（gomobile / wasm 场景入口）。
//
// 若此前调用过 SetTokenProvider，桥接会被包装进 ClientConfig.TokenProvider。
//
// 日志：configJSON 的 logging 字段控制开关/级别/本地输出（见 logger.Config）；
// 已注册 LogCallback 时日志优先转发宿主，未注册时回落到 logging 配置的本地输出。
// 重复调用会先 Close 旧 Client。
func (d *Dispatcher) Init(configJSON string) error {
	var cfg initConfig
	if err := json.Unmarshal([]byte(configJSON), &cfg); err != nil {
		return fmt.Errorf("init config decode: %w", err)
	}

	cc := &client.ClientConfig{
		BaseURL:            cfg.BaseURL,
		Endpoint:           cfg.Endpoint,
		DefaultPlatformID:  cfg.PlatformID,
		DefaultDeviceID:    cfg.DeviceID,
		DefaultSDKVersion:  cfg.SDKVersion,
		DefaultCompression: cfg.Compression,
		SessionDataPath:    cfg.SessionDataPath,
		AdminSecret:        cfg.AdminSecret,
		AdminUserID:        cfg.AdminUserID,
	}

	// 日志出口（含跨语言回调注入路径）——必须在 NewClient 前注入，
	// 保证 Client 构建期（如 SQLite 初始化失败）的日志也能被宿主拿到
	lg := d.replaceLogSink(cfg.Logging)
	cc.Logger = lg
	// TokenProvider：桥优先；其次预取缓存（wasm / 无回调平台）；
	// 配了 AdminSecret 时保持 nil，走 core 层 admin bootstrap（见 client_impl.go refresh 闭包优先级）
	cc.TokenProvider = d.resolveTokenProvider(cfg.AdminSecret != "")

	c := client.NewClient(cc)
	if c == nil {
		return model.NewSDKError(model.ErrCodeInternalError, "init client failed (see log)")
	}
	d.SetClient(c)
	return nil
}

// SetClient 注入外部构建的 Client（Go 内嵌 / cgo 场景）。
//
// 重复注入会取消旧的内部事件监听，但不会 Close 旧 Client（所有权在调用方）。
func (d *Dispatcher) SetClient(c client.Client) {
	d.mu.Lock()
	if d.watchCancel != nil {
		d.watchCancel()
		d.watchCancel = nil
	}
	if d.cbCancel != nil {
		d.cbCancel()
		d.cbCancel = nil
	}
	d.cli = c
	d.loggedIn = false
	d.mu.Unlock()

	d.buildMethodTable()
	d.watchLoginState()
	d.bindCallback()
}

// Close 释放内部资源（取消事件监听、关闭日志本地输出；不 Close Client——所有权在注入方 / Init 方重复 Init 时才转移）
func (d *Dispatcher) Close() {
	d.mu.Lock()
	if d.watchCancel != nil {
		d.watchCancel()
		d.watchCancel = nil
	}
	if d.cbCancel != nil {
		d.cbCancel()
		d.cbCancel = nil
	}
	sink := d.logSink
	d.logSink = nil
	d.mu.Unlock()

	closeLogSink(sink)
}

// ———————————————————————————— 反射调用内核 ————————————————————————————

// Call 异步调用：goroutine 执行，结果走 onSuccess(resultJSON) / onError(code, msg)
//
//	gomobile 导出：iOS onSuccess (String)->Void；Android (String)->Unit
func (d *Dispatcher) Call(method string, argsJSON string, onSuccess func(string), onError func(int32, string)) {
	go func() {
		result, err := d.invoke(method, argsJSON)
		if err != nil {
			code, msg := NormalizeError(err)
			onError(code, msg)
			return
		}
		onSuccess(result)
	}()
}

// SyncCall 同步调用：阻塞直到完成，返回 (resultJSON, error)
func (d *Dispatcher) SyncCall(method string, argsJSON string) (string, error) {
	return d.invoke(method, argsJSON)
}

// MessageCall 消息类调用：与 Call 相同，额外带进度回调 onProgress(percent)。
//
// 当前 core 层发送方法不回报进度，成功时回调 onProgress(100)；
// 预留给后续大文件消息上传进度接线。
func (d *Dispatcher) MessageCall(method string, argsJSON string,
	onSuccess func(string), onError func(int32, string), onProgress func(int32)) {
	go func() {
		result, err := d.invoke(method, argsJSON)
		if err != nil {
			code, msg := NormalizeError(err)
			onError(code, msg)
			return
		}
		if onProgress != nil {
			onProgress(100)
		}
		onSuccess(result)
	}()
}

// invoke 同步执行入口：中间件 → 查表 → 解码参数 → 反射调用 → 编码返回值
func (d *Dispatcher) invoke(method string, argsJSON string) (string, error) {
	return d.withMiddleware(method, func() (string, error) {
		d.mu.RLock()
		cli := d.cli
		fn, ok := d.methods[method]
		d.mu.RUnlock()

		if cli == nil {
			return "", model.NewSDKError(ErrCodeNotInitialized, "dispatcher: client not initialized, call Init/SetClient first")
		}
		if !ok {
			return "", model.NewSDKError(ErrCodeUnknownMethod, "dispatcher: unknown method "+method)
		}
		// 登录态校验（白名单跳过）——放在方法查找之后，未知方法优先报 UnknownMethod
		if !isAuthBypass(method) && !d.isLoggedIn() {
			return "", model.NewSDKError(ErrCodeNotLoggedIn, "not logged in")
		}

		ctx, cancel := context.WithTimeout(context.Background(), d.callTimeout)
		defer cancel()

		args, err := decodeArgs(argsJSON, fn.Type(), ctx)
		if err != nil {
			return "", model.NewSDKError(ErrCodeInvalidArgs, fmt.Sprintf("dispatcher: %s args: %v", method, err))
		}

		outs := fn.Call(args)

		// 任一 error 返回值非 nil → 失败
		for _, out := range outs {
			if out.Type() == errorType && !out.IsNil() {
				return "", out.Interface().(error)
			}
		}
		return encodeResult(outs)
	})
}

// Methods 返回已注册方法清单（JSON 数组），调试用
func (d *Dispatcher) Methods() string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	names := make([]string, 0, len(d.methods))
	for name := range d.methods {
		names = append(names, name)
	}
	bs, _ := json.Marshal(names)
	return string(bs)
}

// ———————————————————————————— 方法分发表构建 ————————————————————————————

// managerReceivers receiver 名 → 从 Client 取 Manager
var managerReceivers = []struct {
	name string
	get  func(client.Client) interface{}
	ifc  reflect.Type
}{
	{"AuthMgr", func(c client.Client) interface{} { return c.Auth() }, reflect.TypeOf((*manager.AuthMgr)(nil)).Elem()},
	{"UserMgr", func(c client.Client) interface{} { return c.User() }, reflect.TypeOf((*manager.UserMgr)(nil)).Elem()},
	{"MsgMgr", func(c client.Client) interface{} { return c.Msg() }, reflect.TypeOf((*manager.MsgMgr)(nil)).Elem()},
	{"GroupMgr", func(c client.Client) interface{} { return c.Group() }, reflect.TypeOf((*manager.GroupMgr)(nil)).Elem()},
	{"RelMgr", func(c client.Client) interface{} { return c.Rel() }, reflect.TypeOf((*manager.RelMgr)(nil)).Elem()},
	{"ConvMgr", func(c client.Client) interface{} { return c.Conv() }, reflect.TypeOf((*manager.ConvMgr)(nil)).Elem()},
	{"ThirdMgr", func(c client.Client) interface{} { return c.Third() }, reflect.TypeOf((*manager.ThirdMgr)(nil)).Elem()},
	{"FileUploader", func(c client.Client) interface{} { return c.FileUploader() }, reflect.TypeOf((*manager.FileUploader)(nil)).Elem()},
}

// buildMethodTable 构建 method string → bound func 分发表
//
// 两部分：
//  1. 生命周期方法（Login / Logout / Connect ...）——包装为 func(ctx, ...) 闭包
//  2. 八个 Manager 的全部接口方法——反射绑定
//
// 参数类型不可 JSON 解码的方法（io.Reader / func / chan 等）跳过注册。
func (d *Dispatcher) buildMethodTable() {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.methods = make(map[string]reflect.Value)
	c := d.cli
	if c == nil {
		return
	}

	// 1. 生命周期方法（闭包包装，成功时同步登录态标记）
	d.methods["Login"] = reflect.ValueOf(func(ctx context.Context, userID string, platformID int32) error {
		return c.Login(ctx, userID, platformID)
	})
	d.methods["LoginWithToken"] = reflect.ValueOf(func(ctx context.Context, userID string, platformID int32, token string, expireTimeSeconds int64) error {
		return c.LoginWithToken(ctx, userID, platformID, token, expireTimeSeconds)
	})
	d.methods["Logout"] = reflect.ValueOf(func(ctx context.Context, clearData bool) error {
		return c.Logout(clearData)
	})
	d.methods["Connect"] = reflect.ValueOf(func(ctx context.Context) error {
		return c.Connect(ctx)
	})
	d.methods["Disconnect"] = reflect.ValueOf(func(ctx context.Context) error {
		return c.Disconnect()
	})
	d.methods["Status"] = reflect.ValueOf(func(ctx context.Context) int32 {
		return int32(c.Status())
	})
	d.methods["SyncState"] = reflect.ValueOf(func(ctx context.Context) int32 {
		return int32(c.SyncState())
	})

	// 2. Manager 接口方法反射注册
	for _, recv := range managerReceivers {
		mgr := recv.get(c)
		if mgr == nil {
			continue
		}
		mgrVal := reflect.ValueOf(mgr)
		for i := 0; i < recv.ifc.NumMethod(); i++ {
			m := recv.ifc.Method(i)
			if !isJSONCompatible(m.Type) {
				continue // 如 FileUploader.Upload（io.Reader 参数无法跨语言）
			}
			d.methods[recv.name+"."+m.Name] = mgrVal.MethodByName(m.Name)
		}
	}
}

// isJSONCompatible 检查方法签名是否可 JSON 编解码（ctx / error 之外的参数与返回值）
func isJSONCompatible(mt reflect.Type) bool {
	check := func(t reflect.Type) bool {
		switch t.Kind() {
		case reflect.Func, reflect.Chan, reflect.UnsafePointer, reflect.Interface:
			return t == errorType || t == ctxType
		}
		return true
	}
	for i := 0; i < mt.NumIn(); i++ {
		if !check(mt.In(i)) {
			return false
		}
	}
	for i := 0; i < mt.NumOut(); i++ {
		if !check(mt.Out(i)) {
			return false
		}
	}
	return true
}

// ———————————————————————————— 登录态跟踪 ————————————————————————————

func (d *Dispatcher) isLoggedIn() bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.loggedIn
}

func (d *Dispatcher) setLoggedIn(v bool) {
	d.mu.Lock()
	d.loggedIn = v
	d.mu.Unlock()
}

// watchLoginState 订阅登录相关事件，保持 loggedIn 与实际状态一致
// （覆盖宿主绕过 Dispatcher 直接调 client.Login 的场景，以及被踢下线）
func (d *Dispatcher) watchLoginState() {
	d.mu.RLock()
	c := d.cli
	d.mu.RUnlock()
	if c == nil {
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	d.mu.Lock()
	d.watchCancel = cancel
	d.mu.Unlock()

	// c.OnLoginSuccess(func() {
	// 	d.setLoggedIn(true)
	// })
	// c.OnLoginFailed(func(err error) {
	// 	d.setLoggedIn(false)
	// })
	// c.OnForceLogout(func() {
	// 	d.setLoggedIn(false)
	// })
	ch, unsub := c.SubscribeAll()
	go func() {
		defer unsub()
		for {
			select {
			case <-ctx.Done():
				return
			case ev, ok := <-ch:
				if !ok {
					return
				}
				switch ev.Type {
				case events.EventLoginSuccess:
					d.setLoggedIn(true)
				case events.EventLoginFailed, events.EventForceLogout:
					d.setLoggedIn(false)
				}
			}
		}
	}()
}

// ———————————————————————————— 生命周期便捷导出 ————————————————————————————
//
// gomobile 可直接绑定 func() / func(int32, string) 签名，所以 Login 等高频
// 方法除走 Call("Login", ...) 外，也提供专用导出（参数位置更清晰）。

// Login 登录（受信环境模式：ClientConfig 已配 AdminSecret）
func (d *Dispatcher) Login(userID string, platformID int32, onSuccess func(), onError func(int32, string)) {
	args, _ := json.Marshal([]interface{}{userID, platformID})
	d.Call("Login", string(args), func(string) { onSuccess() }, onError)
}

// LoginWithToken 登录（终端模式：宿主业务服务端下发的 user token）
func (d *Dispatcher) LoginWithToken(userID string, platformID int32, token string, expireTimeSeconds int64, onSuccess func(), onError func(int32, string)) {
	args, _ := json.Marshal([]interface{}{userID, platformID, token, expireTimeSeconds})
	d.Call("LoginWithToken", string(args), func(string) { onSuccess() }, onError)
}

// Logout 登出（clearData=true 时额外清除本地数据）
func (d *Dispatcher) Logout(clearData bool, onSuccess func(), onError func(int32, string)) {
	args, _ := json.Marshal([]interface{}{clearData})
	d.Call("Logout", string(args), func(string) { onSuccess() }, onError)
}

// Connect 单独建立 WS 连接
func (d *Dispatcher) Connect(onSuccess func(), onError func(int32, string)) {
	d.Call("Connect", "", func(string) { onSuccess() }, onError)
}

// Disconnect 断开 WS 连接（不清理 TokenStore）
func (d *Dispatcher) Disconnect(onSuccess func(), onError func(int32, string)) {
	d.Call("Disconnect", "", func(string) { onSuccess() }, onError)
}

// Status 当前传输层状态（0=Disconnected 1=Connecting 2=Connected）
func (d *Dispatcher) Status() int32 {
	d.mu.RLock()
	c := d.cli
	d.mu.RUnlock()
	if c == nil {
		return int32(transport.TransportStatusDisconnected)
	}
	return int32(c.Status())
}
