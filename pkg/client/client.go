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
// gomobile 封装层后续把 channel 桥接成原生回调（见 docs/DEVELOPMENT.md Phase 5）。
//
// 完整实现见 client_impl.go。
package client

import (
	"context"

	"github.com/PaperMan11/goim-sdk-core/pkg/events"
	"github.com/PaperMan11/goim-sdk-core/pkg/logger"
	"github.com/PaperMan11/goim-sdk-core/pkg/manager"
	"github.com/PaperMan11/goim-sdk-core/pkg/model"
	"github.com/PaperMan11/goim-sdk-core/pkg/storage"
	"github.com/PaperMan11/goim-sdk-core/pkg/transport"
	"github.com/PaperMan11/goim-sdk-core/pkg/utils/eventbus"
	"github.com/PaperMan11/goim-sdk-core/version"
)

// Version 当前 SDK 版本，随 WS 握手 sdkVersion 上报。
//
// 权威定义在 version/VERSION（go:embed 注入，CI/发布脚本可直接读取）；
// 编译期仍可用 ldflags 覆盖：
//
//	go build -ldflags "-X github.com/PaperMan11/goim-sdk-core/pkg/client.Version=x.y.z" ./...
var Version = version.Version

// Client SDK 统一入口接口
type Client interface {
	// —— 生命周期 ——
	// Login 获取 User Token + 建立 WS 连接 + 触发 SeqSync
	//
	// 两种凭证模式（二选一）：
	//   - 受信环境（服务端集成/运维工具）：配置 ClientConfig.AdminSecret，
	//     Login 内部自动 admin bootstrap（GetAdminToken → GetUserToken）
	//   - 终端模式：token 由宿主业务服务端下发，用 LoginWithToken 注入（SDK 不接触 secret）
	Login(ctx context.Context, userID string, platformID int32) error
	// LoginWithToken 使用宿主下发的 user token 登录（终端模式，SDK 不接触 secret）
	LoginWithToken(ctx context.Context, userID string, platformID int32, token string, expireTimeSeconds int64) error
	// Logout 登出：断开连接 + 清理 TokenStore
	//
	// clearData=false 时本地存储（消息/会话/好友/游标）保留，下次登录走游标增量，快速恢复；
	// clearData=true 时额外清除该 owner 全部本地数据（共享设备隐私场景、账号注销），
	// 下次登录将触发服务端 full=true 全量同步。
	Logout(clearData bool) error
	// Close 释放资源：断开 WS + 关闭底层存储（SessionDataPath 创建的 SQLite 句柄）。
	// Logout 后（或宿主 App 退出前）调用；Close 后 client 不可再用
	Close() error
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
	// HTTP API 基础地址（如 https://api.goim.io，不含 /api/v1 前缀，SDK 自动拼接）
	BaseURL string
	// 传输层连接目标（如 ws://api.goim.io:10001/ws 或 wss://...）
	// 当前默认传输层是 WebSocket，scheme 为 ws/wss；未来 TCP 实现复用同一字段（tcp://host:port）。
	// 使用自定义 Transport 注入时此字段被忽略。
	Endpoint string

	// 认证默认参数（WS Connect 用，Login 时 userID/token 会动态替换）
	// DefaultPlatformID 仅在未 Login、直接 Connect 的场景作兜底；类型与 Login 的 platformID 对齐
	DefaultPlatformID int32
	DefaultDeviceID   string
	// DefaultSDKVersion 随 WS 握手上报的 SDK 版本号，空时使用 Version（编译期可用
	// ldflags 覆盖：-ldflags "-X github.com/PaperMan11/goim-sdk-core/pkg/client.Version=x.y.z"）
	DefaultSDKVersion  string
	DefaultCompression string

	// 依赖注入（可用于单元测试 mock；nil 时 DefaultClient 内部构造）
	HTTP      transport.HTTPClient
	Transport transport.MessageTransport

	// 日志（三选一优先级：Logger 注入 > Logging 配置构建 zap > NopLogger 静默）
	// Logger 直接注入完整实现（测试传 NopLogger / 复用宿主 App 日志）
	Logger logger.Logger
	// Logging 按配置内部构建 zap Logger（Logger 为 nil 时生效，nil 时保持静默）
	Logging *logger.Config

	// 存储配置（二选一；都为空时用内存存储，重启后数据丢失）
	Storage         storage.Storage // 直接注入（单测注入 MemoryStorage / :memory: SQLite）
	SessionDataPath string          // SQLite 文件路径（Storage 为 nil 且非空时内部创建 SQLiteStorage，如 "sdk_data/data.db"）

	// —— 凭证（受信环境模式，终端模式用 LoginWithToken）——
	// AdminSecret 应用密钥：非空时 Login 自动 admin bootstrap。
	// ⚠️ 仅限服务端/受信环境使用；终端 App 严禁内嵌 secret（应使用 LoginWithToken）
	// ⚠️ 实测 goim 服务端 get_admin_token 不校验 secret（见 INTEGRATION_TEST_PLAN.md R9），
	//    权限边界是 AdminUserID 是否在服务端管理员列表；secret 字段保留以兼容未来服务端恢复校验
	AdminSecret string
	// AdminUserID 管理员账号（默认 "imAdmin"，必须在服务端管理员列表内，否则 Login 失败）
	AdminUserID string

	// TokenProvider 终端模式 token 刷新回调（LoginWithToken 模式推荐配置）。
	// token 过期时 SDK 自动调用，宿主应向自己的业务服务端请求新 token 后返回
	// （业务服务端持有 secret / 管理员凭证，通过 IM 管理接口换取新 token）。
	// 返回 (token, 剩余有效秒数, error)；error 非空时该次请求/重连失败并上报事件。
	// 未配置时：AdminSecret 模式自动 bootstrap，LoginWithToken 模式刷新失败
	TokenProvider func(ctx context.Context, userID string, platformID int32) (token string, expireTimeSeconds int64, err error)
}
