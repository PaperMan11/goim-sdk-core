package transport

import (
	"sync"

	"github.com/PaperMan11/goim-sdk-core/pkg/logger"
)

// PushHandler 服务端推送消息处理器（WSPushMsg / WSKickOnlineMsg 等推送类 ReqIdentifier）
//
// handler 在 readLoop goroutine 中同步执行，必须快速返回（proto.Unmarshal + 非阻塞投递是安全的），
// 禁止在 handler 内做网络请求 / 锁等待等重操作。
// resp 由 GC 管理，handler 可安全持有，但注意 resp.Data 底层数组不应被修改。
type PushHandler func(resp *Response)

// MessageRouter 消息路由器：按 ReqIdentifier / OperationID 路由服务端 Response
//
// 路由优先级：
//  1. OperationID 命中 pending 请求（同步请求-响应，ReqIdentifier 1001-1007）
//     → 投递给 SendRequestAndWait 的等待方
//  2. ReqIdentifier 命中注册的 push handler（服务端主动推送，2001-2005）
//     → 同步调用 handler
//  3. 都未命中 → warn 日志
type MessageRouter struct {
	logger logger.Logger

	mu       sync.RWMutex
	handlers map[int]PushHandler

	pendingMu sync.Mutex
	pending   map[string]chan *Response // operationID → 等待通道
}

// NewMessageRouter 创建消息路由器
func NewMessageRouter(l logger.Logger) *MessageRouter {
	return &MessageRouter{
		logger:   l,
		handlers: make(map[int]PushHandler),
		pending:  make(map[string]chan *Response),
	}
}

// RegisterPush 注册推送类消息的处理器（重复注册同一会覆盖）
func (r *MessageRouter) RegisterPush(reqIdentifier int, h PushHandler) {
	r.mu.Lock()
	r.handlers[reqIdentifier] = h
	r.mu.Unlock()
}

// UnregisterPush 注销推送处理器
func (r *MessageRouter) UnregisterPush(reqIdentifier int) {
	r.mu.Lock()
	delete(r.handlers, reqIdentifier)
	r.mu.Unlock()
}

// Route 路由 Response（readLoop 调用）
func (r *MessageRouter) Route(resp *Response) {
	if resp == nil {
		return
	}

	// 1. 同步响应：OperationID 匹配 pending 请求
	if resp.OperationID != "" && r.deliverPending(resp.OperationID, resp) {
		return
	}

	// 2. 服务端推送：按 ReqIdentifier 分发
	r.mu.RLock()
	h, ok := r.handlers[resp.ReqIdentifier]
	r.mu.RUnlock()

	if ok {
		h(resp)
	} else {
		r.logger.Warnf("message unrouted, reqIdentifier=%d, operationID=%s, errCode=%d",
			resp.ReqIdentifier, resp.OperationID, resp.ErrCode)
	}
}

// === pending 请求管理（同步请求-响应匹配） ===

// waitResponse 注册一个 pending 请求，返回响应接收通道（buffer=1，不阻塞 Route）
func (r *MessageRouter) waitResponse(operationID string) chan *Response {
	ch := make(chan *Response, 1)
	r.pendingMu.Lock()
	r.pending[operationID] = ch
	r.pendingMu.Unlock()
	return ch
}

// cancelWait 取消等待（SendRequestAndWait 退出时调用，幂等）
func (r *MessageRouter) cancelWait(operationID string) {
	r.pendingMu.Lock()
	delete(r.pending, operationID)
	r.pendingMu.Unlock()
}

// deliverPending 尝试投递到 pending 等待方，返回是否命中
func (r *MessageRouter) deliverPending(operationID string, resp *Response) bool {
	r.pendingMu.Lock()
	ch, ok := r.pending[operationID]
	if ok {
		delete(r.pending, operationID) // 一次性交付，立即移除
	}
	r.pendingMu.Unlock()

	if !ok {
		return false
	}

	select {
	case ch <- resp:
	default:
		// 等待方已超时退出，通道无人接收 → 回收
		return false
	}
	return true
}

// failAllPending 连接断开时唤醒所有 pending 等待方（投递 nil 表示连接错误）
func (r *MessageRouter) failAllPending() {
	r.pendingMu.Lock()
	pending := r.pending
	r.pending = make(map[string]chan *Response)
	r.pendingMu.Unlock()

	for _, ch := range pending {
		select {
		case ch <- nil:
		default:
		}
	}
}
