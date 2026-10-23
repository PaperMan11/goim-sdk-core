package transport

import (
	"context"
	"sync"

	"github.com/PaperMan11/goim-sdk-core/pkg/logger"
	"github.com/PaperMan11/goim-sdk-core/pkg/utils/run"
	"github.com/PaperMan11/goim-sdk-core/pkg/utils/timex"
)

// ConnectParams 连接认证参数（服务端统一定义，WS / TCP 等所有连接类型共用）
//
// WS 场景拼到 URL query（见 ws.go encodeQuery）；
// TCP 场景可在握手包中携带同一份字段。
type ConnectParams struct {
	UserID       string // 用户 ID
	Token        string // 登录令牌
	PlatformID   int    // 平台 ID
	DeviceID     string // 设备 ID
	OperationID  string // 连接级 operationID
	SDKType      string // "go" / "js"，空默认 GoSDK
	SDKVersion   string // SDK 版本号
	Compression  string // "none" / "gzip" / "zlib"，空默认 "none"
	SendResponse bool   // 是否需要服务端回执
	IsBackground bool   // 是否后台运行状态
}

// withDefaults 填充默认值
func (p ConnectParams) withDefaults() ConnectParams {
	if p.SDKType == "" {
		p.SDKType = GoSDK
	}
	if p.Compression == "" {
		p.Compression = "none"
	}
	return p
}

// TransportStatus 传输层连接状态
type TransportStatus int32

const (
	TransportStatusDisconnected TransportStatus = iota
	TransportStatusConnecting
	TransportStatusConnected
)

func (s TransportStatus) String() string {
	switch s {
	case TransportStatusDisconnected:
		return "Disconnected"
	case TransportStatusConnecting:
		return "Connecting"
	case TransportStatusConnected:
		return "Connected"
	default:
		return "Unknown"
	}
}

// MessageTransport 通用消息推送传输层接口
// WebSocket 和 TCP 等实现都要满足此接口，上层 manager/client 只依赖它
type MessageTransport interface {
	// Connect 建立连接（阻塞直到连接成功或超时）
	Connect(ctx context.Context) error
	// Disconnect 主动断开连接（停止重连、关闭底层连接）
	Disconnect() error
	// IsConnected 是否已连接
	IsConnected() bool
	// Status 当前连接状态
	Status() TransportStatus
	// SendRequest 发送 Request 协议包（单向，不等响应）
	SendRequest(ctx context.Context, req *Request) error
	// SendRequestAndWait 发送请求并同步等待响应（按 OperationID 匹配）
	SendRequestAndWait(ctx context.Context, req *Request) (*Response, error)
	// Router 返回消息路由器（按 ReqIdentifier 路由 Response，推送 handler 从这里注册）
	Router() *MessageRouter

	// === 生命周期钩子（上层注入事件派发等行为） ===

	// SetConnectParams 更新连接参数（Login 时覆盖 userID/token 后同步；建议在 Connect 之前调用）
	SetConnectParams(p ConnectParams)
	// SetOnReconnected 注册重连成功回调（被动断开后自动重连成功时触发）
	SetOnReconnected(func(ctx context.Context))
	// SetOnDisconnected 注册被动断开回调（非主动 Disconnect，如网络波动 / 对端关闭；在重连循环启动前触发一次）
	SetOnDisconnected(func())
}

// baseTransport 通用传输层基类，提供状态机 / 重连 / 消息路由 / actor 并发管理
//
// 使用方式（子类嵌入）：
//  1. Connect 时设置 reconnectTarget / reconnectDial / reconnectOnConnected
//  2. dial 成功后调 startActors(readLoop, writeLoop)
//  3. Disconnect 时调 stopActors + close 底层连接
//  4. run.Group 管理所有 actor，任意一个退出会自动 interrupt 其余 actor，然后触发重连
type baseTransport struct {
	mu     sync.RWMutex
	status TransportStatus
	router *MessageRouter
	logger logger.Logger

	// 连接目标（仅日志展示用）
	reconnectTarget string

	// 子类注入的重连实现（在 Connect 成功后赋值）
	reconnectDial        func(ctx context.Context) error
	reconnectOnConnected func()

	// run.Group 管理的并发 actor（读循环 + 写循环）
	group       run.Group
	groupCancel context.CancelFunc
	groupWg     sync.WaitGroup
	actorMu     sync.Mutex

	// onActorExit actor 退出时的资源回收钩子（子类注入，如关闭底层连接以解除阻塞读）
	onActorExit func()

	// 重连控制
	reconnectMu     sync.Mutex
	reconnecting    bool
	reconnectCancel context.CancelFunc

	// 重连成功回调（上层注入，如触发 SeqSync）
	OnReconnected func(ctx context.Context)

	// OnDisconnected 被动断开回调（非主动 Disconnect，如网络波动 / 对端关闭）
	// 在 actorsExited 里、runReconnectLoop 之前触发一次
	OnDisconnected func()
}

// newBaseTransport 构造基类实例
func newBaseTransport(l logger.Logger) baseTransport {
	return baseTransport{
		status: TransportStatusDisconnected,
		router: NewMessageRouter(l),
		logger: l,
	}
}

func (b *baseTransport) Router() *MessageRouter {
	return b.router
}

func (b *baseTransport) Status() TransportStatus {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.status
}

func (b *baseTransport) IsConnected() bool {
	return b.Status() == TransportStatusConnected
}

// SetOnReconnected 实现 MessageTransport 接口
func (b *baseTransport) SetOnReconnected(fn func(ctx context.Context)) {
	b.OnReconnected = fn
}

// SetOnDisconnected 实现 MessageTransport 接口
func (b *baseTransport) SetOnDisconnected(fn func()) {
	b.OnDisconnected = fn
}

func (b *baseTransport) setState(s TransportStatus) {
	b.mu.Lock()
	b.status = s
	b.mu.Unlock()
}

// startActors 启动并发 actor（WS 场景为读循环 + 写循环，TCP 场景可只传一个）
//
// 每个函数签名都是 func(ctx context.Context)，内部用 select ctx.Done() 退出。
// run.Group 保证：任意一个 actor 退出 → interrupt 其余 actor（调 cancel）→ 等全部退出。
// group.Run() 返回后 → actorsExited() 被调用。
func (b *baseTransport) startActors(actors ...func(ctx context.Context)) {
	ctx, cancel := context.WithCancel(context.Background())

	b.actorMu.Lock()
	b.groupCancel = cancel
	b.group = run.Group{}
	for _, actor := range actors {
		fn := actor
		b.group.Add(
			func() error { fn(ctx); return nil },
			func(err error) { b.interruptActors() },
		)
	}
	b.actorMu.Unlock()

	b.groupWg.Add(1)
	go func() {
		defer b.groupWg.Done()
		if err := b.group.Run(); err != nil {
			b.logger.Warnf("transport actors group exited, err=%v", err)
		}
		b.actorsExited()
	}()

	// 启动瞬间若已被主动断开，立即回收，避免残留 actor
	if !b.IsConnected() {
		b.stopActors()
	}
}

// interruptActors 中断所有 actor：先 cancel（让循环感知主动退出），再回收底层资源（关闭连接解除阻塞读）
func (b *baseTransport) interruptActors() {
	b.actorMu.Lock()
	cancel := b.groupCancel
	b.actorMu.Unlock()

	if cancel != nil {
		cancel()
	}
	if b.onActorExit != nil {
		b.onActorExit()
	}
}

// stopActors 同步停止所有并发 actor（Disconnect 时调用）
func (b *baseTransport) stopActors() {
	b.interruptActors()
	b.groupWg.Wait()
}

// actorsExited run.Group.Run() 返回后的钩子
// 主动 Disconnect 时 status 已经是 Disconnected → 跳过重连
// 被动断开时仍保持 Connected 状态 → 触发重连
func (b *baseTransport) actorsExited() {
	// 连接断开：唤醒所有 pending 等待方，避免卡到超时
	if b.router != nil {
		b.router.failAllPending()
	}

	b.mu.RLock()
	stillConnected := b.status == TransportStatusConnected
	b.mu.RUnlock()
	if !stillConnected {
		return
	}

	// 被动断开：上层需要感知（如派发 EventDisconnect）
	if b.OnDisconnected != nil {
		b.OnDisconnected()
	}
	go b.runReconnectLoop()
}

// routeMessage 统一消息路由入口（readLoop 调用，所有权移交给 router）
func (b *baseTransport) routeMessage(resp *Response) {
	if b.router != nil {
		b.router.Route(resp)
	}
}

// runReconnectLoop 通用指数退避重连循环
// 使用之前在 Connect 时注入的 reconnectDial / reconnectOnConnected
func (b *baseTransport) runReconnectLoop() {
	if b.reconnectDial == nil {
		b.logger.Errorf("reconnectDial is nil, cannot reconnect")
		return
	}

	ctx, cancel := context.WithCancel(context.Background())

	b.reconnectMu.Lock()
	if b.reconnecting {
		b.reconnectMu.Unlock()
		cancel()
		return
	}
	b.reconnecting = true
	b.reconnectCancel = cancel
	b.reconnectMu.Unlock()

	defer func() {
		b.reconnectMu.Lock()
		b.reconnecting = false
		b.reconnectCancel = nil
		b.reconnectMu.Unlock()
		cancel()
	}()

	b.setState(TransportStatusDisconnected)

	backoff := timex.Second
	for {
		// 主动 Disconnect 会 cancel 该 ctx，立刻结束重连
		select {
		case <-ctx.Done():
			b.logger.Infof("transport reconnect cancelled, target=%s", b.reconnectTarget)
			return
		default:
		}

		b.logger.Infof("transport reconnecting, backoff=%v, target=%s", backoff, b.reconnectTarget)

		select {
		case <-ctx.Done():
			return
		case <-timex.After(backoff):
		}

		b.setState(TransportStatusConnecting)

		dialCtx, dialCancel := context.WithTimeout(ctx, 10*timex.Second)
		err := b.reconnectDial(dialCtx)
		dialCancel()
		if ctx.Err() != nil {
			return
		}

		if err == nil {
			b.setState(TransportStatusConnected)
			if b.reconnectOnConnected != nil {
				b.reconnectOnConnected()
			}
			b.logger.Infof("transport reconnected, target=%s", b.reconnectTarget)
			if b.OnReconnected != nil {
				b.OnReconnected(context.Background())
			}
			return
		}

		b.logger.Warnf("transport reconnect failed, target=%s, error=%v", b.reconnectTarget, err)
		b.setState(TransportStatusDisconnected)

		backoff *= 2
		if backoff > 30*timex.Second {
			backoff = 30 * timex.Second
		}
	}
}

// stopReconnect 取消正在进行的重连（主动 Disconnect 时调用）
func (b *baseTransport) stopReconnect() {
	b.reconnectMu.Lock()
	cancel := b.reconnectCancel
	b.reconnectCancel = nil
	b.reconnectMu.Unlock()

	if cancel != nil {
		cancel()
	}
}

// isReconnecting 是否有重连循环在运行
func (b *baseTransport) isReconnecting() bool {
	b.reconnectMu.Lock()
	defer b.reconnectMu.Unlock()
	return b.reconnecting
}

// === 通用错误 ===

type notConnectedError struct {
	transport string // "websocket" / "tcp" 等
}

func (e *notConnectedError) Error() string {
	return e.transport + " transport: not connected"
}

func newNotConnectedError(transport string) error {
	return &notConnectedError{transport: transport}
}
