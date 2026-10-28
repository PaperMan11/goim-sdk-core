package client

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/PaperMan11/goim-sdk-core/pkg/events"
	"github.com/PaperMan11/goim-sdk-core/pkg/logger"
	"github.com/PaperMan11/goim-sdk-core/pkg/manager"
	"github.com/PaperMan11/goim-sdk-core/pkg/model"
	"github.com/PaperMan11/goim-sdk-core/pkg/storage"
	sdkSync "github.com/PaperMan11/goim-sdk-core/pkg/sync"
	"github.com/PaperMan11/goim-sdk-core/pkg/transport"
	"github.com/PaperMan11/goim-sdk-core/pkg/utils/convert"
	"github.com/PaperMan11/goim-sdk-core/pkg/utils/eventbus"
	"github.com/PaperMan11/goim-sdk-core/pkg/utils/timex"
	"github.com/PaperMan11/goim-sdk-core/pkg/utils/uuidx"

	pbconstant "github.com/PaperMan11/goim/pkg/protocol/constant"
	pbsdk "github.com/PaperMan11/goim/pkg/protocol/sdkws"
)

// DefaultClient Client 接口完整实现
type DefaultClient struct {
	cfg *ClientConfig

	http      transport.HTTPClient
	transport transport.MessageTransport
	tokens    transport.TokenStore
	storage   storage.Storage
	bus       *eventbus.EventBus
	logger    logger.Logger

	params transport.ConnectParams // WS 连接参数（构造时填默认，Login 时覆盖 userID/token）

	// Manager 实例
	auth  *manager.DefaultAuthMgr
	user  *manager.DefaultUserMgr
	msg   *manager.DefaultMsgMgr
	group *manager.DefaultGroupMgr
	rel   *manager.DefaultRelMgr
	conv  *manager.DefaultConvMgr

	mu            sync.RWMutex
	userID        string
	platformID    int32
	loggedIn      bool
	connectCancel context.CancelFunc // 连接相关 goroutine 的 cancel

	// 同步引擎（Login / 重连后执行）
	seqSync    *sdkSync.SeqSyncEngine
	entitySync *sdkSync.EntitySyncEngine
}

// NewClient 构造 DefaultClient
//
// 完整初始化：
//  1. 依赖构建（logger / eventbus / tokenStore / storage / HTTP / WS）
//  2. Manager 构造（六个 Default*Mgr，依赖注入）
//  3. 传输层生命周期钩子（OnReconnected → EventReconnect，OnDisconnected → EventDisconnect）
//  4. 推送处理器注册（5 个 WS ReqIdentifier → EventType 映射）
func NewClient(cfg *ClientConfig) Client {
	if cfg == nil {
		cfg = &ClientConfig{}
	}

	// 1. 依赖构建
	// Logger 注入优先（测试/宿主 App 日志）；否则按 Logging 配置构建 zap；都为空 → NopLogger 静默
	l := cfg.Logger
	if l == nil {
		l = logger.New(cfg.Logging)
	}
	bus := eventbus.New(eventbus.WithLogger(l))

	// TokenStore 刷新闭包：token 过期时获取新 token。
	// 优先级：TokenProvider 宿主回调（终端模式）> AdminSecret 自动 bootstrap > 失败
	var c *DefaultClient
	refresh := func(ctx context.Context, userID string, platformID int32) (string, int64, error) {
		if cfg.TokenProvider != nil {
			return cfg.TokenProvider(ctx, userID, platformID)
		}
		if c == nil || c.auth == nil {
			return "", 0, fmt.Errorf("token refresh: client not initialized")
		}
		// 先失效本地缓存再重取：服务端可能已清除 token 记录（如 Redis TTL 先于
		// expireTimeSeconds 到期）而本地仍视为有效；若带着旧缓存走 admin bootstrap，
		// 旧 admin token 会被原样复用 → 刷新结果与旧 token 相同 → 刷新/重试死循环
		_ = c.tokens.Delete(userID, platformID)
		resp, err := c.acquireUserToken(ctx, userID, platformID)
		if err != nil {
			return "", 0, err
		}
		return resp.Token, resp.ExpireTimeSeconds, nil
	}
	tokens := transport.NewTokenStore(refresh)

	// 存储：注入优先；否则 SessionDataPath 有值 → SQLite；都空 → 内存
	var stor storage.Storage
	if cfg.Storage != nil {
		stor = cfg.Storage
	} else if cfg.SessionDataPath != "" {
		db, err := storage.NewSQLiteStorage(cfg.SessionDataPath, l)
		if err != nil {
			l.Errorf("client: init sqlite storage, path=%s, error=%v", cfg.SessionDataPath, err.Error())
			return nil
		}
		stor = db
	} else {
		stor = storage.NewMemoryStorage()
	}

	var http transport.HTTPClient
	if cfg.HTTP != nil {
		http = cfg.HTTP
	} else {
		http = transport.NewHTTPClient(cfg.BaseURL, tokens, l)
	}

	// 设备 ID 解析：配置优先 → 存储持久化值 → 自动生成并持久化。
	// 跨重启稳定很重要：多端互踢策略下，同一设备重连不应被当成新设备
	deviceID := cfg.DefaultDeviceID
	if deviceID == "" {
		if v, _ := stor.GetMeta(metaKeyDeviceID); v != "" {
			deviceID = v
		} else {
			deviceID = uuidx.New()
			_ = stor.SetMeta(metaKeyDeviceID, deviceID)
		}
	}

	params := transport.ConnectParams{
		PlatformID:  convert.ToInt(cfg.DefaultPlatformID),
		DeviceID:    deviceID,
		SDKVersion:  cfg.DefaultSDKVersion,
		Compression: cfg.DefaultCompression,
		OperationID: uuidx.NewOperationID("connect"),
	}
	if params.SDKVersion == "" {
		params.SDKVersion = Version
	}

	var ws transport.MessageTransport
	if cfg.Transport != nil {
		ws = cfg.Transport
	} else {
		ws = transport.NewWSClient(cfg.Endpoint, params, l)
	}

	// 2. Manager 构造
	authMgr := manager.NewAuthMgr(http, tokens, bus, l)
	userMgr := manager.NewUserMgr(http, bus, l)
	msgMgr := manager.NewMsgMgr(http, ws, bus, l)
	groupMgr := manager.NewGroupMgr(http, bus, l)
	relMgr := manager.NewRelMgr(http, bus, l)
	convMgr := manager.NewConvMgr(http, bus, l)

	c = &DefaultClient{
		cfg:       cfg,
		http:      http,
		transport: ws,
		tokens:    tokens,
		storage:   stor,
		bus:       bus,
		logger:    l,
		params:    params,
		auth:      authMgr,
		user:      userMgr,
		msg:       msgMgr,
		group:     groupMgr,
		rel:       relMgr,
		conv:      convMgr,
	}

	// 3. 传输层生命周期钩子（通过 MessageTransport interface setter 注入）
	ws.SetOnConnecting(func() {
		bus.Publish(events.EventConnecting, events.Event{
			Type:      events.EventConnecting,
			Timestamp: timex.UnixMilli(),
		})
	})
	ws.SetOnReconnected(func(ctx context.Context) {
		bus.Publish(events.EventReconnect, events.Event{
			Type:      events.EventReconnect,
			Timestamp: timex.UnixMilli(),
		})
		// 重连后：gap 拉取 + 五类实体带游标增量（异步，失败只 warn，下轮重连再补）
		c.mu.RLock()
		owner := c.userID
		loggedIn := c.loggedIn
		c.mu.RUnlock()
		if !loggedIn || owner == "" {
			return
		}
		go func() {
			if err := c.runSyncFlow(context.Background(), owner); err != nil {
				c.logger.Warnf("client: resync after reconnect, owner=%s, error=%v", owner, err.Error())
			}
		}()
	})
	ws.SetOnDisconnected(func() {
		bus.Publish(events.EventDisconnect, events.Event{
			Type:      events.EventDisconnect,
			Timestamp: timex.UnixMilli(),
		})
	})

	// 4. 推送处理器注册
	router := ws.Router()
	router.RegisterPush(transport.WSKickOnlineMsg, c.handleKick)
	router.RegisterPush(transport.WSLogoutMsg, c.handleKick)
	router.RegisterPush(transport.WSSetBackgroundStatus, c.handleBackgroundStatus)
	router.RegisterPush(transport.WSSubUserOnlineStatus, c.handleUserStatusChange)
	router.RegisterPush(transport.WSPushMsg, c.handleMessagePush)

	return c
}

// === 生命周期 ===

const defaultAdminUserID = "imAdmin"

// sdk_meta 表键名
const metaKeyDeviceID = "device_id"

// Login 登录（受信环境模式）：Token 获取 + WS 连接 + 数据同步
//
// 配置了 ClientConfig.AdminSecret 时自动 admin bootstrap
// （GetAdminToken → 挂 admin token → GetUserToken → 切回用户 token），
// 未配置时要求凭证已注入（如 LoginWithToken / 宿主自管 TokenStore）
func (c *DefaultClient) Login(ctx context.Context, userID string, platformID int32) error {
	tokenResp, err := c.acquireUserToken(ctx, userID, platformID)
	if err != nil {
		c.bus.Publish(events.EventLoginFailed, events.Event{
			Type:      events.EventLoginFailed,
			Timestamp: timex.UnixMilli(),
		})
		return fmt.Errorf("login: get user token: %w", err)
	}
	return c.loginFlow(ctx, userID, platformID, tokenResp.Token)
}

// LoginWithToken 登录（终端模式）：使用宿主业务服务端下发的 user token，
// SDK 全程不接触 admin secret
func (c *DefaultClient) LoginWithToken(ctx context.Context, userID string, platformID int32, token string, expireTimeSeconds int64) error {
	if token == "" {
		return fmt.Errorf("login: token is empty")
	}
	_ = c.tokens.Set(userID, platformID, token, expireTimeSeconds)
	return c.loginFlow(ctx, userID, platformID, token)
}

// acquireUserToken 获取用户 token（过期刷新也走此路径，见 NewClient 的 refresh 闭包）
//
//   - 配置 AdminSecret → admin bootstrap：复用/重取 admin token → 挂载 → GetUserToken
//     （服务端 get_user_token 要求调用方具备 admin 身份，见 docs/INTEGRATION_TEST_PLAN.md §4）
//   - 刷新目标为 admin 凭证本身 (adminUserID, AdminPlatformID) → 仅重取 admin token，
//     不为 admin 签发 user token（否则会覆盖 admin token 缓存槽）
//   - 未配置 AdminSecret → 直接 GetUserToken（依赖当前已注入的凭证）
func (c *DefaultClient) acquireUserToken(ctx context.Context, userID string, platformID int32) (*transport.TokenResult, error) {
	if c.cfg.AdminSecret != "" {
		adminUserID := c.cfg.AdminUserID
		if adminUserID == "" {
			adminUserID = defaultAdminUserID
		}
		// admin token 缓存有效则复用，缺失/过期则重取（get_admin_token 为白名单接口，无需凭证）
		var adminResult *transport.TokenResult
		if cached, _ := c.tokens.Get(adminUserID, pbconstant.AdminPlatformID); cached == "" {
			adminResp, err := c.auth.GetAdminToken(ctx, c.cfg.AdminSecret, adminUserID)
			if err != nil {
				return nil, fmt.Errorf("admin bootstrap: %w", err)
			}
			_ = c.tokens.Set(adminUserID, pbconstant.AdminPlatformID, adminResp.Token, adminResp.ExpireTimeSeconds)
			adminResult = adminResp
		}
		// 刷新目标就是 admin 凭证本身（服务端清除 admin token 记录后，HTTP 层会以
		// (admin, AdminPlatformID) 触发 Refresh）：GetAdminToken 即完成刷新，到此返回。
		// 若继续为 admin 走 GetUserToken，admin 的 user token 会写进 admin token
		// 缓存槽，污染后续 bootstrap 的凭证选路
		if isAdminSelf := userID == adminUserID && platformID == pbconstant.AdminPlatformID; isAdminSelf {
			if adminResult != nil {
				return adminResult, nil
			}
			cached, _ := c.tokens.Get(adminUserID, pbconstant.AdminPlatformID)
			return &transport.TokenResult{Token: cached}, nil
		}
		if setter, ok := c.http.(transport.CurrentUserSetter); ok {
			setter.SetCurrentUser(adminUserID, pbconstant.AdminPlatformID)
		}
	}
	// 内部 TokenStore.Set(userID, platformID) + Publish EventLoginSuccess
	resp, err := c.auth.GetUserToken(ctx, userID, platformID)
	// 无论成败都把 HTTP 选路切回目标用户：上面的 admin 挂载只是调用 get_user_token
	// 的临时现场，残留会让重试及后续业务请求以 admin 身份发送。Login 场景由
	// loginFlow 兜底切回，但刷新场景没有兜底，必须在此恢复
	if setter, ok := c.http.(transport.CurrentUserSetter); ok {
		setter.SetCurrentUser(userID, platformID)
	}
	return resp, err
}

// loginFlow token 就绪后的公共登录链路：HTTP 选路 → WS 参数 → 连接 → 同步 → 事件
func (c *DefaultClient) loginFlow(ctx context.Context, userID string, platformID int32, token string) error {
	// 1. HTTP 层 token 选路：TokenStore 按 (userID, platformID) 存取，
	//    后续 HTTP 请求（实体同步等）按当前用户取 token
	if setter, ok := c.http.(transport.CurrentUserSetter); ok {
		setter.SetCurrentUser(userID, platformID)
	}

	// 2. 覆盖 WS 连接参数
	c.params.UserID = userID
	c.params.Token = token
	c.params.PlatformID = convert.ToInt(platformID)
	c.params.OperationID = uuidx.NewOperationID("login")

	// 同步到 WS transport
	c.transport.SetConnectParams(c.params)

	// 3. WS Connect
	if err := c.Connect(ctx); err != nil {
		c.bus.Publish(events.EventLoginFailed, events.Event{
			Type:      events.EventLoginFailed,
			Timestamp: timex.UnixMilli(),
		})
		return fmt.Errorf("login: ws connect: %w", err)
	}

	// 4. 注入 MsgMgr senderID
	c.msg.SetSenderID(userID)

	// 5. 数据同步：消息 Seq gap 拉取 + 五类实体游标增量
	//    全部完成才算登录成功（上层收到 EventLoginSuccess 时本地数据已就绪）
	c.mu.Lock()
	c.userID = userID
	c.platformID = platformID
	c.loggedIn = true
	c.mu.Unlock()
	if err := c.runSyncFlow(ctx, userID); err != nil {
		c.bus.Publish(events.EventLoginFailed, events.Event{
			Type:      events.EventLoginFailed,
			Timestamp: timex.UnixMilli(),
		})
		return fmt.Errorf("login: sync data: %w", err)
	}

	// 6. 完整登录成功事件（Token + WS + SeqSync + EntitySync 全部完成）
	c.bus.Publish(events.EventLoginSuccess, events.Event{
		Type:      events.EventLoginSuccess,
		Timestamp: timex.UnixMilli(),
	})

	return nil
}

// runSyncFlow 执行完整同步流程：消息 Seq gap 拉取 → 五类实体增量
func (c *DefaultClient) runSyncFlow(ctx context.Context, owner string) error {
	seqSync := sdkSync.NewSeqSyncEngine(c.msg, c.storage, owner, c.bus, c.logger)
	entitySync := sdkSync.NewEntitySyncEngine(c.rel, c.group, c.conv, c.storage, owner, c.bus, c.logger)

	if err := seqSync.Sync(ctx); err != nil {
		return fmt.Errorf("seq sync: %w", err)
	}
	if err := entitySync.SyncAll(ctx); err != nil {
		return fmt.Errorf("entity sync: %w", err)
	}
	return nil
}

func (c *DefaultClient) Logout(clearData bool) error {
	c.mu.Lock()
	userID := c.userID
	platformID := c.platformID
	c.loggedIn = false
	c.userID = ""
	c.mu.Unlock()

	// 主动 Disconnect（不会触发 OnDisconnected，因为 actorsExited 里 stillConnected=false）
	_ = c.Disconnect()

	// 清理 TokenStore（会话结束，token 不复用）
	if c.tokens != nil && userID != "" {
		_ = c.tokens.Delete(userID, platformID)
	}

	// 清除 HTTP 层当前用户（下次 Login 重新注入）
	if setter, ok := c.http.(transport.CurrentUserSetter); ok {
		setter.SetCurrentUser("", 0)
	}

	// 本地数据默认保留（owner 隔离，多账号共存）：下次登录游标增量同步，快速恢复。
	// 仅调用方显式要求时清除（共享设备隐私、注销账号），代价是下次登录全量重拉
	if clearData && c.storage != nil && userID != "" {
		_ = c.storage.Clear(context.Background(), userID)
	}

	return nil
}

// Close 释放资源：断开 WS + 关闭底层存储（SQLite 句柄，Windows 下不关闭会锁文件）
func (c *DefaultClient) Close() error {
	_ = c.Disconnect()
	if cl, ok := c.storage.(io.Closer); ok {
		return cl.Close()
	}
	return nil
}

func (c *DefaultClient) Connect(ctx context.Context) error {
	if c.transport.IsConnected() {
		return nil
	}

	if err := c.transport.Connect(ctx); err != nil {
		return err
	}

	// 首次连接成功事件
	c.bus.Publish(events.EventConnect, events.Event{
		Type:      events.EventConnect,
		Timestamp: timex.UnixMilli(),
	})
	return nil
}

func (c *DefaultClient) Disconnect() error {
	if c.connectCancel != nil {
		c.connectCancel()
		c.connectCancel = nil
	}
	return c.transport.Disconnect()
}

func (c *DefaultClient) Status() transport.TransportStatus {
	return c.transport.Status()
}

// === Manager 聚合 ===

func (c *DefaultClient) Auth() manager.AuthMgr   { return c.auth }
func (c *DefaultClient) User() manager.UserMgr   { return c.user }
func (c *DefaultClient) Msg() manager.MsgMgr     { return c.msg }
func (c *DefaultClient) Group() manager.GroupMgr { return c.group }
func (c *DefaultClient) Rel() manager.RelMgr     { return c.rel }
func (c *DefaultClient) Conv() manager.ConvMgr   { return c.conv }

// === 事件订阅 ===

// Subscribe 订阅指定 EventType，返回类型安全的事件 channel
//
// eventbus 内部用 interface{} 载荷，这里做一层 type assert 桥接，
// 让上层业务拿到干净的 <-chan events.Event，不需要自己断言。
func (c *DefaultClient) Subscribe(eventType events.EventType) (<-chan events.Event, eventbus.UnsubscribeFunc) {
	rawCh, unsubscribeRaw := c.bus.Subscribe(eventType)

	typedCh := make(chan events.Event, cap(rawCh))
	go func() {
		defer close(typedCh)
		for raw := range rawCh {
			ev, ok := raw.(events.Event)
			if !ok {
				continue
			}
			select {
			case typedCh <- ev:
			default:
				// 订阅者消费太慢，丢事件 —— 和 eventbus 策略一致
				c.logger.Warnf("client.Subscribe: typed channel full, dropping event type=%v", eventType)
			}
		}
	}()

	return typedCh, func() {
		unsubscribeRaw()
	}
}

// SubscribeAll 订阅全部事件（调试用）
func (c *DefaultClient) SubscribeAll() (<-chan events.Event, eventbus.UnsubscribeFunc) {
	rawCh, unsubscribe := c.bus.Subscribe(eventbus.AllKey)

	typedCh := make(chan events.Event, cap(rawCh))
	go func() {
		defer close(typedCh)
		for raw := range rawCh {
			ev, ok := raw.(events.Event)
			if !ok {
				continue
			}
			select {
			case typedCh <- ev:
			default:
				c.logger.Warnf("client.SubscribeAll: typed channel full, dropping")
			}
		}
	}()
	return typedCh, unsubscribe
}

// === 便捷生命周期 Handler（Subscribe 的语法糖）===

func (c *DefaultClient) OnConnect(h func()) {
	c.onEvent(events.EventConnect, func(events.Event) { h() })
}
func (c *DefaultClient) OnConnecting(h func()) {
	c.onEvent(events.EventConnecting, func(events.Event) { h() })
}
func (c *DefaultClient) OnDisconnect(h func(err error)) {
	c.onEvent(events.EventDisconnect, func(ev events.Event) {
		if err, _ := ev.Payload.(error); err != nil {
			h(err)
		} else {
			h(fmt.Errorf("connection lost"))
		}
	})
}
func (c *DefaultClient) OnReconnect(h func()) {
	c.onEvent(events.EventReconnect, func(events.Event) { h() })
}
func (c *DefaultClient) OnLoginSuccess(h func()) {
	c.onEvent(events.EventLoginSuccess, func(events.Event) { h() })
}
func (c *DefaultClient) OnLoginFailed(h func(err error)) {
	c.onEvent(events.EventLoginFailed, func(ev events.Event) {
		if err, _ := ev.Payload.(error); err != nil {
			h(err)
		} else {
			h(fmt.Errorf("login failed"))
		}
	})
}
func (c *DefaultClient) OnForceLogout(h func()) {
	c.onEvent(events.EventForceLogout, func(events.Event) { h() })
}

func (c *DefaultClient) onEvent(et events.EventType, fn func(events.Event)) {
	ch, unsub := c.Subscribe(et)
	go func() {
		defer unsub()
		for ev := range ch {
			fn(ev)
		}
	}()
}

// === 高频业务回调（Subscribe 语法糖，payload 做好类型断言后回调） ===

// OnNewMessage 收到新消息（EventNewMsg payload: *pbsdk.MsgData → *model.MsgData）
func (c *DefaultClient) OnNewMessage(h func(msg *model.MsgData)) {
	c.onEvent(events.EventNewMsg, func(ev events.Event) {
		if pm, ok := ev.Payload.(*pbsdk.MsgData); ok {
			h(manager.ConvProtoMsgData(pm))
		}
	})
}

// OnConversationChanged 会话变更（会话列表 UI 刷新信号）
func (c *DefaultClient) OnConversationChanged(h func()) {
	c.onEvent(events.EventConversationUpdate, func(events.Event) { h() })
}

// OnMessageRevoked 消息被撤回（EventRevokeMsg payload: *pbsdk.RevokeMsgTips，回调取 ClientMsgID）
func (c *DefaultClient) OnMessageRevoked(h func(clientMsgID string)) {
	c.onEvent(events.EventRevokeMsg, func(ev events.Event) {
		if tip, ok := ev.Payload.(*pbsdk.RevokeMsgTips); ok {
			h(tip.ClientMsgID)
		}
	})
}

// === WS PushHandler 实现 ===

// handleKick 处理踢下线推送（WSKickOnlineMsg + WSLogoutMsg）
func (c *DefaultClient) handleKick(resp *transport.Response) {
	c.mu.Lock()
	c.loggedIn = false
	c.mu.Unlock()

	c.bus.Publish(events.EventForceLogout, events.Event{
		Type:       events.EventForceLogout,
		RawPayload: resp.Data,
		Timestamp:  timex.UnixMilli(),
	})

	// 异步断开，不阻塞 readLoop
	go c.Disconnect()
}

// handleBackgroundStatus 处理后台状态变更推送（WSSetBackgroundStatus=2004）
func (c *DefaultClient) handleBackgroundStatus(resp *transport.Response) {
	c.bus.Publish(events.EventBackgroundStatusChanged, events.Event{
		Type:       events.EventBackgroundStatusChanged,
		RawPayload: resp.Data,
		Timestamp:  timex.UnixMilli(),
	})
}

// handleUserStatusChange 处理订阅的用户在线状态变更推送（WSSubUserOnlineStatus=2005）
func (c *DefaultClient) handleUserStatusChange(resp *transport.Response) {
	tip := &pbsdk.UserStatusChangeTips{}
	var payload interface{}
	if err := proto.Unmarshal(resp.Data, tip); err == nil {
		payload = tip
	}
	c.bus.Publish(events.EventUserStatusChange, events.Event{
		Type:       events.EventUserStatusChange,
		Payload:    payload,
		RawPayload: resp.Data,
		Timestamp:  timex.UnixMilli(),
	})
}

// handleMessagePush 处理消息类推送（WSPushMsg=2001）
//
// 协议事实（对齐服务端 pkg/msgdispatcher.SendNotification / im-rpc msg notification.go）：
//
//	2001 推送的唯一形态是 sdkws.MsgData。普通消息与全部系统通知共用这一个信封：
//	- 普通消息：ContentType ∈ [100, 999]（101 文本 / 102 图片 / 203 自定义通知...）→ EventNewMsg
//	- 系统通知：ContentType ≥ 1000（NotificationBegin），Tips 本体序列化在
//	  Content = JSON(NotificationElem{Detail: JSON(tips)})，按 ContentType 分流到对应事件
//	  （2101 撤回 / 2102 删除 / 2200 已读 / 1201-1210 好友 / 1300-1308 用户会话 / 1501-1520 群组）
//
// 不再做 multi-type probe（proto 字段编号互串，RevokeMsgTips 会被误判成 MsgData）。
func (c *DefaultClient) handleMessagePush(resp *transport.Response) {
	data := resp.Data
	timestamp := timex.UnixMilli()

	msgData := &pbsdk.MsgData{}
	if err := proto.Unmarshal(data, msgData); err != nil {
		c.logger.Warnf("handleMessagePush: unmarshal MsgData failed, reqIdentifier=%d, error=%v",
			resp.ReqIdentifier, err.Error())
		return
	}

	// 系统通知：NotificationElem 两段 JSON 解出 tips，按 ContentType 分流
	if msgData.ContentType >= pbconstant.NotificationBegin {
		c.dispatchNotification(msgData, timestamp)
		return
	}

	// 普通消息 → EventNewMsg
	c.bus.Publish(events.EventNewMsg, events.Event{
		Type:       events.EventNewMsg,
		Payload:    msgData,
		RawPayload: data,
		Timestamp:  timestamp,
	})
}

// dispatchNotification 按 ContentType 分流系统通知
//
// tips 解码策略：核心高频通知解码为具体 proto 类型；其余 payload 为 Detail JSON string，
// RawPayload 始终保留 MsgData 原始 bytes，上层可自行解析。
func (c *DefaultClient) dispatchNotification(msgData *pbsdk.MsgData, timestamp int64) {
	detail := extractNotificationDetail(msgData.Content)
	data := convert.ToBytes(msgData.Content)

	publish := func(et events.EventType, payload interface{}) {
		c.bus.Publish(et, events.Event{
			Type:       et,
			Payload:    payload,
			RawPayload: data,
			Timestamp:  timestamp,
		})
	}
	decodeTip := func(et events.EventType, tip proto.Message) {
		if err := json.Unmarshal([]byte(detail), tip); err != nil {
			c.logger.Warnf("dispatchNotification: unmarshal tips failed, contentType=%d, error=%v",
				msgData.ContentType, err.Error())
		}
		publish(et, tip)
	}

	switch msgData.ContentType {
	// —— 消息类通知（tips 类型确定）——
	case pbconstant.MsgRevokeNotification: // 2101
		decodeTip(events.EventRevokeMsg, &pbsdk.RevokeMsgTips{})
	case pbconstant.DeleteMsgsNotification: // 2102
		decodeTip(events.EventDeleteMsgs, &pbsdk.DeleteMsgsTips{})
	case pbconstant.HasReadReceipt: // 2200
		decodeTip(events.EventMarkAsRead, &pbsdk.MarkAsReadTips{})

	// —— 用户 / 状态 ——
	case pbconstant.UserInfoUpdatedNotification: // 1303
		decodeTip(events.EventUserInfoUpdated, &pbsdk.UserInfoUpdatedTips{})
	case pbconstant.UserStatusChangeNotification: // 1304
		decodeTip(events.EventUserStatusChange, &pbsdk.UserStatusChangeTips{})

	// —— 好友 ——
	case pbconstant.FriendApplicationNotification,
		pbconstant.FriendApplicationApprovedNotification,
		pbconstant.FriendApplicationRejectedNotification: // 1203 / 1201 / 1202
		decodeTip(events.EventFriendApplication, &pbsdk.FriendApplicationTips{})
	case pbconstant.FriendAddedNotification: // 1204
		publish(events.EventFriendAdded, detail)
	case pbconstant.FriendDeletedNotification: // 1205
		publish(events.EventFriendDeleted, detail)
	case pbconstant.FriendRemarkSetNotification,
		pbconstant.FriendInfoUpdatedNotification,
		pbconstant.FriendsInfoUpdateNotification: // 1206 / 1209 / 1210
		publish(events.EventFriendInfoChanged, detail)
	case pbconstant.BlackAddedNotification: // 1207
		publish(events.EventBlackAdded, detail)
	case pbconstant.BlackDeletedNotification: // 1208
		publish(events.EventBlackDeleted, detail)

	// —— 会话 ——
	case pbconstant.ConversationChangeNotification: // 1300
		publish(events.EventConversationUpdate, detail)

	// —— 群组 ——
	case pbconstant.GroupCreatedNotification: // 1501
		decodeTip(events.EventGroupCreated, &pbsdk.GroupCreatedTips{})
	case pbconstant.GroupInfoSetNotification: // 1502
		decodeTip(events.EventGroupInfoSet, &pbsdk.GroupInfoSetTips{})
	case pbconstant.JoinGroupApplicationNotification: // 1503
		publish(events.EventJoinGroupApplication, detail)
	case pbconstant.GroupApplicationAcceptedNotification: // 1505
		publish(events.EventGroupApplicationAccepted, detail)
	case pbconstant.GroupApplicationRejectedNotification: // 1506
		publish(events.EventGroupApplicationRejected, detail)
	case pbconstant.GroupOwnerTransferredNotification: // 1507
		publish(events.EventGroupOwnerTransferred, detail)
	case pbconstant.MemberKickedNotification: // 1508
		publish(events.EventMemberKicked, detail)
	case pbconstant.MemberInvitedNotification: // 1509
		publish(events.EventMemberInvited, detail)
	case pbconstant.MemberEnterNotification: // 1510
		publish(events.EventMemberEnter, detail)
	case pbconstant.MemberQuitNotification: // 1504
		publish(events.EventMemberQuit, detail)
	case pbconstant.GroupDismissedNotification: // 1511
		publish(events.EventGroupDismissed, detail)
	case pbconstant.GroupMemberMutedNotification: // 1512
		publish(events.EventGroupMemberMuted, detail)
	case pbconstant.GroupMemberCancelMutedNotification: // 1513
		publish(events.EventGroupMemberCancelMuted, detail)
	case pbconstant.GroupMutedNotification: // 1514
		publish(events.EventGroupMuted, detail)
	case pbconstant.GroupCancelMutedNotification: // 1515
		publish(events.EventGroupCancelMuted, detail)

	default:
		c.logger.Warnf("dispatchNotification: unmapped contentType=%d, clientMsgID=%s",
			msgData.ContentType, msgData.ClientMsgID)
	}
}

// extractNotificationDetail 解出通知 Detail：Content = JSON(NotificationElem{Detail: JSON(tips)})
func extractNotificationDetail(content string) string {
	elem := &pbsdk.NotificationElem{}
	if err := json.Unmarshal([]byte(content), elem); err != nil {
		// 非 NotificationElem 包装（防御：直接把 Content 当 Detail）
		return content
	}
	return elem.Detail
}
