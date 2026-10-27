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
// gomobile 封装层后续把 channel 桥接成原生回调（见 DEVELOPMENT.md Phase 5）。
//
// 完整实现见 client_impl.go。
package client

import (
	"context"

	"github.com/PaperMan11/goim-sdk-core/pkg/events"
	"github.com/PaperMan11/goim-sdk-core/pkg/manager"
	"github.com/PaperMan11/goim-sdk-core/pkg/model"
	"github.com/PaperMan11/goim-sdk-core/pkg/storage"
	"github.com/PaperMan11/goim-sdk-core/pkg/transport"
	"github.com/PaperMan11/goim-sdk-core/pkg/utils/eventbus"
)

// Client SDK 统一入口接口
type Client interface {
	// —— 生命周期 ——
	// Login 获取 User Token + 建立 WS 连接 + 触发 SeqSync
	Login(ctx context.Context, userID string, platformID int32) error
	// Logout 登出：断开连接 + 清理 TokenStore
	//
	// clearData=false 时本地存储（消息/会话/好友/游标）保留，下次登录走游标增量，快速恢复；
	// clearData=true 时额外清除该 owner 全部本地数据（共享设备隐私场景、账号注销），
	// 下次登录将触发服务端 full=true 全量同步。
	Logout(clearData bool) error
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
	// OnConnecting WS 连接中（首次连接 / 重连循环每次 dial 前，UI 可显示"连接中"）
	OnConnecting(h func())
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

	// —— 高频业务回调 ——
	// OnNewMessage 收到新消息（WSPushMsg 2001 推送，payload 已转为 model.MsgData）
	OnNewMessage(h func(msg *model.MsgData))
	// OnConversationChanged 会话变更（会话列表 UI 刷新信号）
	OnConversationChanged(h func())
	// OnMessageRevoked 消息被撤回（回调参数为被撤回消息的 ClientMsgID）
	OnMessageRevoked(h func(clientMsgID string))
}

// ClientConfig Client 构造配置
type ClientConfig struct {
	// HTTP API 基础地址（如 https://api.goim.io/api/v1）
	BaseURL string
	// 传输层连接目标（如 ws://api.goim.io:10001/ws 或 wss://...）
	// 当前默认传输层是 WebSocket，scheme 为 ws/wss；未来 TCP 实现复用同一字段（tcp://host:port）。
	// 使用自定义 Transport 注入时此字段被忽略。
	Endpoint string

	// 认证默认参数（WS Connect 用，Login 时 userID/token 会动态替换）
	// DefaultPlatformID 仅在未 Login、直接 Connect 的场景作兜底；类型与 Login 的 platformID 对齐
	DefaultPlatformID  int32
	DefaultDeviceID    string
	DefaultSDKVersion  string
	DefaultCompression string

	// 依赖注入（可用于单元测试 mock；nil 时 DefaultClient 内部构造）
	HTTP      transport.HTTPClient
	Transport transport.MessageTransport

	// 存储配置（二选一；都为空时用内存存储，重启后数据丢失）
	Storage         storage.Storage // 直接注入（单测注入 MemoryStorage / :memory: SQLite）
	SessionDataPath string          // SQLite 文件路径（Storage 为 nil 且非空时内部创建 SQLiteStorage，如 "sdk_data/data.db"）
}
