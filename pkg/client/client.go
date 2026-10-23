// Package client 对外统一入口 —— 聚合六个 Manager + 统一事件订阅 API。
//
// 事件体系（统一走 pkg/utils/eventbus）：
//
//	WS 推送    → MessageRouter.PushHandler → proto.Unmarshal → Event{Type, Payload}
//	传输层状态  → transport 回调                          → Event{Type}
//	Manager 操作 → Manager 实现里 Publish                  → Event{Type, Payload}
//	系统事件   → Storage / SeqSync 内部 Publish             → Event{Type}
//
// 上层业务统一用 Subscribe / OnConnect / OnForceLogout 等 API 监听。
// gomobile 封装层后续把 channel 桥接成原生回调。
//
// 本文件仅定义 Client 接口骨架 + ClientConfig 配置骨架，
// DefaultClient 完整实现放在 Phase 4。
package client

import (
	"context"
	"fmt"

	"github.com/PaperMan11/goim-sdk-core/pkg/events"
	"github.com/PaperMan11/goim-sdk-core/pkg/manager"
	"github.com/PaperMan11/goim-sdk-core/pkg/transport"
	"github.com/PaperMan11/goim-sdk-core/pkg/utils/eventbus"
)

// Client SDK 统一入口接口
type Client interface {
	// —— 生命周期 ——
	// Login 获取 User Token + 建立 WS 连接 + 触发 SeqSync
	Login(ctx context.Context, userID string, platformID int32) error
	// Logout 主动登出：断开 WS + 清理 TokenStore
	Logout() error
	// Connect 单独建立 WS 连接（Login 内部会调用，也可重连场景单独用）
	Connect(ctx context.Context) error
	// Disconnect 断开 WS 连接（不清理 TokenStore，Logout 内部会调用）
	Disconnect() error
	// Status 当前传输层状态
	Status() transport.TransportStatus

	// —— Manager 聚合 ——
	Auth() manager.AuthMgr
	User() manager.UserMgr
	Msg() manager.MsgMgr
	Group() manager.GroupMgr
	Rel() manager.RelMgr
	Conv() manager.ConvMgr

	// —— 事件订阅（统一走 eventbus）——
	// Subscribe 订阅指定 EventType，返回事件 channel 和取消函数
	Subscribe(eventType events.EventType) (<-chan events.Event, eventbus.UnsubscribeFunc)
	// SubscribeAll 订阅全部事件（调试用）
	SubscribeAll() (<-chan events.Event, eventbus.UnsubscribeFunc)

	// —— 便捷生命周期 Handler ——
	// OnConnect WS 连接成功（首次 + 重连都会触发）
	OnConnect(h func())
	// OnDisconnect WS 连接断开（被动断开会触发，主动 Disconnect 不触发）
	OnDisconnect(h func(err error))
	// OnReconnect 自动重连成功
	OnReconnect(h func())
	// OnLoginSuccess 登录成功（Token + WS 就绪）
	OnLoginSuccess(h func())
	// OnLoginFailed 登录失败
	OnLoginFailed(h func(err error))
	// OnForceLogout 被踢下线（WSKickOnlineMsg / WSLogoutMsg 推送触发）
	OnForceLogout(h func())
}

// ClientConfig Client 构造配置
type ClientConfig struct {
	// HTTP API 基础地址（如 https://api.goim.io/api/v1）
	BaseURL string
	// WS 连接地址（如 ws://api.goim.io:10001/ws）
	WSURL string

	// 认证默认参数（WS Connect 用，Login 时 userID/token 会动态替换）
	DefaultPlatformID  int
	DefaultDeviceID    string
	DefaultSDKVersion  string
	DefaultCompression string

	// 依赖注入（可用于单元测试 mock；nil 时 DefaultClient 内部构造）
	HTTP transport.HTTPClient
	WS   transport.MessageTransport
}

// DefaultClient 默认 Client 实现（骨架，Phase 4 填充）
//
// 完整实现需要：
//  1. 初始化 eventbus + logger + TokenStore + HTTPClient + WSClient
//  2. 构造六个 Manager（依赖注入）
//  3. 注册 MessageRouter.PushHandler 映射表（5 个 WS 推送 → EventType）
//  4. 注入 baseTransport.OnReconnected → eventbus.Publish(EventReconnect)
type DefaultClient struct{}

// NewClient 构造 DefaultClient（Phase 4 填充完整实现）
func NewClient(cfg *ClientConfig) Client {
	return &DefaultClient{}
}

// —— 骨架占位方法（Phase 4 填充完整实现）——

func (c *DefaultClient) Login(ctx context.Context, userID string, platformID int32) error {
	return fmt.Errorf("client.Login: not implemented")
}
func (c *DefaultClient) Logout() error {
	return fmt.Errorf("client.Logout: not implemented")
}
func (c *DefaultClient) Connect(ctx context.Context) error {
	return fmt.Errorf("client.Connect: not implemented")
}
func (c *DefaultClient) Disconnect() error {
	return fmt.Errorf("client.Disconnect: not implemented")
}
func (c *DefaultClient) Status() transport.TransportStatus {
	return transport.TransportStatusDisconnected
}
func (c *DefaultClient) Auth() manager.AuthMgr   { return nil }
func (c *DefaultClient) User() manager.UserMgr   { return nil }
func (c *DefaultClient) Msg() manager.MsgMgr     { return nil }
func (c *DefaultClient) Group() manager.GroupMgr { return nil }
func (c *DefaultClient) Rel() manager.RelMgr     { return nil }
func (c *DefaultClient) Conv() manager.ConvMgr   { return nil }
func (c *DefaultClient) Subscribe(eventType events.EventType) (<-chan events.Event, eventbus.UnsubscribeFunc) {
	return nil, func() {}
}
func (c *DefaultClient) SubscribeAll() (<-chan events.Event, eventbus.UnsubscribeFunc) {
	return nil, func() {}
}
func (c *DefaultClient) OnConnect(h func())              {}
func (c *DefaultClient) OnDisconnect(h func(err error))  {}
func (c *DefaultClient) OnReconnect(h func())            {}
func (c *DefaultClient) OnLoginSuccess(h func())         {}
func (c *DefaultClient) OnLoginFailed(h func(err error)) {}
func (c *DefaultClient) OnForceLogout(h func())          {}
