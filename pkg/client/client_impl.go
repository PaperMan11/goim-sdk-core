package client

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"

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
	"github.com/PaperMan11/goim-sdk-core/pkg/utils/safego"
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
	auth       *manager.DefaultAuthMgr
	user       *manager.DefaultUserMgr
	msg        *manager.DefaultMsgMgr
	group      *manager.DefaultGroupMgr
	rel        *manager.DefaultRelMgr
	conv       *manager.DefaultConvMgr
	third      *manager.DefaultThirdMgr
	fileUpload manager.FileUploader
	local      manager.LocalDataMgr // 本地数据能力门面（只读缓存）

	mu            sync.RWMutex
	userID        string
	platformID    int32
	loggedIn      bool
	connectCancel context.CancelFunc // 连接相关 goroutine 的 cancel

	// 后台数据同步（登录/重连触发，与登录流程解耦；mu 保护）
	syncState  SyncState          // 同步状态（SyncStateNone/Syncing/Synced/Failed）
	syncGen    uint64             // 同步轮次（start/stopBackgroundSync 递增，防旧轮 goroutine 覆盖新状态）
	syncCancel context.CancelFunc // 后台同步 goroutine 的 cancel（Logout/被踢/Close/新一轮同步时取消）

	// 同步引擎（Login / 重连后执行）
	seqSync    *sdkSync.SeqSyncEngine
	entitySync *sdkSync.EntitySyncEngine

	// 通知驱动的定向实体增量同步（去抖定时器 + 窗口内待同步类型累积 + 执行互斥）
	entitySyncMu      sync.Mutex
	entitySyncTimer   *time.Timer
	entitySyncPending map[sdkSync.EntityKind]struct{} // 去抖窗口内累积的待同步实体类型
	syncRunMu         sync.Mutex

	// 会话摘要写入串行化：推送、发送回调与后台 SeqSyncEngine 补拉通知可能并发，
	// 避免读改写互相覆盖。同步引擎通过注入的 refreshSummary 共用这把锁。
	convSummaryMu sync.Mutex
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
	// 优先级：TokenProvider 宿主回调（终端模式）> AdminSecret 自动 bootstrap > 失败。
	// 成功/失败都发布事件：wasm 预取模式没有同步刷新回调，宿主只能靠
	// EventTokenRefreshed / EventTokenRefreshFailed 感知续期结果并补发 token。
	var c *DefaultClient
	fetchToken := func(ctx context.Context, userID string, platformID int32) (string, int64, error) {
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
	refresh := func(ctx context.Context, userID string, platformID int32) (string, int64, error) {
		token, expire, err := fetchToken(ctx, userID, platformID)
		if err != nil {
			bus.Publish(events.EventTokenRefreshFailed, events.Event{
				Type: events.EventTokenRefreshFailed,
				Payload: &model.TokenRefreshFailedData{
					UserID:     userID,
					PlatformID: platformID,
					Error:      err.Error(),
				},
				Timestamp: timex.UnixMilli(),
			})
			return "", 0, err
		}
		bus.Publish(events.EventTokenRefreshed, events.Event{
			Type:      events.EventTokenRefreshed,
			Payload:   &model.TokenRefreshedData{UserID: userID, PlatformID: platformID},
			Timestamp: timex.UnixMilli(),
		})
		return token, expire, nil
	}
	tokens := transport.NewTokenStore(refresh)

	// 存储：注入优先；否则 SessionDataPath 有值 → SQLite；都空 → 内存
	var stor storage.Storage
	if cfg.Storage != nil {
		stor = cfg.Storage
	} else if cfg.SessionDataPath != "" {
		db, err := newSQLiteStorage(cfg.SessionDataPath, l)
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
	thirdMgr := manager.NewThirdMgr(http, l)
	fileUploader := manager.NewFileUploader(thirdMgr, l)

	c = &DefaultClient{
		cfg:        cfg,
		http:       http,
		transport:  ws,
		tokens:     tokens,
		storage:    stor,
		bus:        bus,
		logger:     l,
		params:     params,
		auth:       authMgr,
		user:       userMgr,
		msg:        msgMgr,
		group:      groupMgr,
		rel:        relMgr,
		conv:       convMgr,
		third:      thirdMgr,
		fileUpload: fileUploader,
	}

	// 发送成功回调：维护会话本地摘要（服务端会话 DTO 不含摘要）
	c.msg.SetOnSent(c.applySentConversationSummary)
	// 本人已读上报成功：同步本地消息缓存 IsRead（Local.UnreadCount 依赖它）
	c.msg.SetOnReadMarked(c.applySelfReadMark)

	// 本地数据能力门面（依赖 storage + 运行期当前 owner）
	c.local = manager.NewLocalDataMgr(stor, c.currentOwner)

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
		// 重连后：gap 拉取 + 五类实体带游标增量，走后台同步（进度经
		// EventSyncStart/EventSyncFinish/EventSyncFailed 暴露；失败下轮重连再补）
		owner, loggedIn := c.sessionOwner()
		if !loggedIn || owner == "" {
			return
		}
		c.startBackgroundSync(owner)
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

// loginFlow token 就绪后的公共登录链路：HTTP 选路 → WS 参数 → 连接 → 事件 → 后台同步
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

	// 5. 登录成功：登录语义收敛为「Token + WS 就绪」即返回（对齐微信/Telegram 做法，
	//    避免全量同步阻塞登录、拉长登录耗时）
	c.mu.Lock()
	c.userID = userID
	c.platformID = platformID
	c.loggedIn = true
	c.mu.Unlock()
	c.bus.Publish(events.EventLoginSuccess, events.Event{
		Type:      events.EventLoginSuccess,
		Timestamp: timex.UnixMilli(),
	})

	// 6. 数据同步移入后台：消息 Seq gap 拉取 + 五类实体游标增量。
	//    进度经 EventSyncStart/EventSyncFinish/EventSyncFailed 与 SyncState() 暴露，
	//    失败不再伪装成 EventLoginFailed（连接本身已成功）
	c.startBackgroundSync(userID)

	return nil
}

// runSyncFlow 执行完整同步流程：消息 Seq gap 拉取 → 五类实体增量
//
// 与通知触发的实体同步（triggerEntitySync）互斥：游标读写串行化，
// 避免重连同步与通知同步并发拉取同一增量段
func (c *DefaultClient) runSyncFlow(ctx context.Context, owner string) error {
	c.syncRunMu.Lock()
	defer c.syncRunMu.Unlock()

	seqSync := sdkSync.NewSeqSyncEngine(c.msg, c.storage, owner, c.bus, c.logger, c.refreshSummary)
	if err := seqSync.Sync(ctx); err != nil {
		return fmt.Errorf("seq sync: %w", err)
	}
	if err := c.runEntitySync(ctx, owner); err != nil {
		return fmt.Errorf("entity sync: %w", err)
	}
	// 会话已落库后同步服务端已读游标（未读推导以它为准）；
	// 失败只降级为本地推导，不阻断整轮同步
	if err := c.refreshReadSeqs(ctx, owner); err != nil {
		c.logger.Warnf("sync: refresh read seqs failed, owner=%s, error=%v", owner, err.Error())
	}
	return nil
}

// runEntitySync 五类实体（好友/黑名单/群/群成员/会话）游标增量同步
func (c *DefaultClient) runEntitySync(ctx context.Context, owner string) error {
	entitySync := sdkSync.NewEntitySyncEngine(c.rel, c.group, c.conv, c.storage, owner, c.bus, c.logger)
	return entitySync.SyncAll(ctx)
}

// startBackgroundSync 启动后台数据同步（登录 / 重连后调用）
//
// 同步在独立 goroutine 执行，与登录流程解耦；进度经 EventSyncStart /
// EventSyncFinish / EventSyncFailed 事件与 SyncState() 状态暴露。
// 每轮同步递增 syncGen：新一轮启动或 stopBackgroundSync 取消后，
// 旧轮 goroutine 结束时不覆盖状态、不发事件，静默退出。
func (c *DefaultClient) startBackgroundSync(owner string) {
	c.mu.Lock()
	if c.syncCancel != nil {
		c.syncCancel() // 取消在途同步（如重连时上一轮同步仍在拉取）
	}
	c.syncGen++
	gen := c.syncGen
	ctx, cancel := context.WithCancel(context.Background())
	c.syncCancel = cancel
	c.syncState = SyncStateSyncing
	c.mu.Unlock()

	c.bus.Publish(events.EventSyncStart, events.Event{
		Type:      events.EventSyncStart,
		Timestamp: timex.UnixMilli(),
	})

	go func() {
		// panic 兜底：后台同步 panic 若杀死进程，wasm 下所有排队回调会抛
		// "Go program has already exited"，SDK 整体静默失效
		defer safego.Recover("client.backgroundSync", c.logger.Warnf)
		err := c.runSyncFlow(ctx, owner)

		c.mu.Lock()
		stale := c.syncGen != gen
		if !stale {
			if err != nil {
				c.syncState = SyncStateFailed
			} else {
				c.syncState = SyncStateSynced
			}
		}
		c.mu.Unlock()
		if stale {
			return
		}
		if err != nil {
			c.logger.Warnf("client: background sync failed, owner=%s, error=%v", owner, err.Error())
			c.bus.Publish(events.EventSyncFailed, events.Event{
				Type:      events.EventSyncFailed,
				Payload:   err,
				Timestamp: timex.UnixMilli(),
			})
			return
		}
		c.bus.Publish(events.EventSyncFinish, events.Event{
			Type:      events.EventSyncFinish,
			Timestamp: timex.UnixMilli(),
		})
	}()
}

// stopBackgroundSync 取消在途后台同步并复位状态（Logout / 被踢下线 / Close）
func (c *DefaultClient) stopBackgroundSync() {
	c.mu.Lock()
	if c.syncCancel != nil {
		c.syncCancel()
		c.syncCancel = nil
	}
	c.syncGen++ // 在途同步 goroutine 结束时视为过期，不覆盖状态/不发事件
	c.syncState = SyncStateNone
	c.mu.Unlock()
}

// stopEntitySyncDebounce 停止通知去抖定时器并丢弃 pending 类型（Logout / 被踢 / Close）
//
// 不停止的话，Close 关闭存储后定时器仍会触发同步，读到已关闭的数据库
// （"sql: database is closed"，集成测试 t.Cleanup(Close) 无 Logout 的路径必现）
func (c *DefaultClient) stopEntitySyncDebounce() {
	c.entitySyncMu.Lock()
	defer c.entitySyncMu.Unlock()
	if c.entitySyncTimer != nil {
		c.entitySyncTimer.Stop()
		c.entitySyncTimer = nil
	}
	c.entitySyncPending = nil
}

// SyncState 当前数据同步状态
func (c *DefaultClient) SyncState() SyncState {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.syncState
}

// entitySyncDebounce 通知驱动的实体同步去抖窗口：突发通知合并为一次增量同步
const entitySyncDebounce = 500 * time.Millisecond

// triggerEntitySync 实体类变更通知触发定向增量同步
//
// 通知类型映射到具体实体（好友/黑名单/群/会话），去抖窗口内多条通知累积的
// 类型合并为一次同步，只拉窗口内变更类型的增量，未变更类型不发请求。
// 让本地缓存在连接期间实时跟进，无需等待下次重连。失败仅告警——缓存最终由
// 下次触发或重连同步的游标增量收敛。
func (c *DefaultClient) triggerEntitySync(kinds ...sdkSync.EntityKind) {
	c.entitySyncMu.Lock()
	defer c.entitySyncMu.Unlock()
	if c.entitySyncPending == nil {
		c.entitySyncPending = make(map[sdkSync.EntityKind]struct{}, 4)
	}
	for _, k := range kinds {
		c.entitySyncPending[k] = struct{}{}
	}
	if c.entitySyncTimer != nil {
		c.entitySyncTimer.Stop()
	}
	c.entitySyncTimer = time.AfterFunc(entitySyncDebounce, c.runEntitySyncDebounced)
}

// runEntitySyncDebounced 去抖窗口结束后执行定向实体增量同步（含登出/执行互斥防护）
func (c *DefaultClient) runEntitySyncDebounced() {
	c.entitySyncMu.Lock()
	c.entitySyncTimer = nil
	pending := c.entitySyncPending
	c.entitySyncPending = nil
	c.entitySyncMu.Unlock()

	if len(pending) == 0 {
		return
	}
	owner, loggedIn := c.sessionOwner()
	if !loggedIn || owner == "" {
		return
	}

	kinds := make([]sdkSync.EntityKind, 0, len(pending))
	for k := range pending {
		kinds = append(kinds, k)
	}
	c.syncRunMu.Lock()
	defer c.syncRunMu.Unlock()
	// 拿锁后复查登录态：前面读取登录态与真正执行之间可能已 Logout/Close，
	// 复查与 syncRunMu 握手共同保证不会访问已关闭的存储
	_, loggedIn = c.sessionOwner()
	if !loggedIn {
		return
	}
	// 超时预算在拿到锁之后才开始计：首轮大同步可能持锁很久，
	// 若在排队前创建 ctx，醒来即超时，pending 已清空导致这批变更被丢弃
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	entitySync := sdkSync.NewEntitySyncEngine(c.rel, c.group, c.conv, c.storage, owner, c.bus, c.logger)
	if err := entitySync.SyncKinds(ctx, kinds...); err != nil {
		c.logger.Warnf("client: entity resync after notification, owner=%s, kinds=%v, error=%v",
			owner, kinds, err.Error())
	}
}

func (c *DefaultClient) Logout(clearData bool) error {
	c.mu.Lock()
	userID := c.userID
	platformID := c.platformID
	c.loggedIn = false
	c.userID = ""
	c.mu.Unlock()

	// 取消在途后台同步并复位同步状态；停掉未触发的通知去抖同步
	c.stopBackgroundSync()
	c.stopEntitySyncDebounce()

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

// Close 释放资源：断开 WS + 停止全部同步 + 关闭底层存储（SQLite 句柄，Windows 下不关闭会锁文件）
func (c *DefaultClient) Close() error {
	// 复位登录态：去抖同步的登录态门禁在 Close 后必须关闭
	c.mu.Lock()
	c.loggedIn = false
	c.userID = ""
	c.mu.Unlock()

	// 停掉未触发的通知去抖同步与在途后台同步
	c.stopEntitySyncDebounce()
	c.stopBackgroundSync()
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

func (c *DefaultClient) Auth() manager.AuthMgr              { return c.auth }
func (c *DefaultClient) User() manager.UserMgr              { return c.user }
func (c *DefaultClient) Msg() manager.MsgMgr                { return c.msg }
func (c *DefaultClient) Group() manager.GroupMgr            { return c.group }
func (c *DefaultClient) Rel() manager.RelMgr                { return c.rel }
func (c *DefaultClient) Conv() manager.ConvMgr              { return c.conv }
func (c *DefaultClient) Third() manager.ThirdMgr            { return c.third }
func (c *DefaultClient) FileUploader() manager.FileUploader { return c.fileUpload }

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
func (c *DefaultClient) OnSyncStart(h func()) {
	c.onEvent(events.EventSyncStart, func(events.Event) { h() })
}
func (c *DefaultClient) OnSyncFinish(h func()) {
	c.onEvent(events.EventSyncFinish, func(events.Event) { h() })
}
func (c *DefaultClient) OnSyncFailed(h func(err error)) {
	c.onEvent(events.EventSyncFailed, func(ev events.Event) {
		if err, _ := ev.Payload.(error); err != nil {
			h(err)
		} else {
			h(fmt.Errorf("sync failed"))
		}
	})
}

func (c *DefaultClient) onEvent(et events.EventType, fn func(events.Event)) {
	ch, unsub := c.Subscribe(et)
	go func() {
		defer unsub()
		// fn 链路最终会调到跨语言回调（JS/Native），panic 必须降级为日志
		defer safego.Recover("client.onEvent", c.logger.Warnf)
		for ev := range ch {
			fn(ev)
		}
	}()
}

// === 高频业务回调（Subscribe 语法糖，payload 做好类型断言后回调） ===

// OnNewMessage 收到新消息（EventNewMsg payload: *model.MsgData，含本地 ConversationID）
func (c *DefaultClient) OnNewMessage(h func(msg *model.MsgData)) {
	c.onEvent(events.EventNewMsg, func(ev events.Event) {
		if mm, ok := ev.Payload.(*model.MsgData); ok {
			h(mm)
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
	// 取消在途后台同步并复位同步状态（会话已结束，同步无意义）
	c.stopBackgroundSync()
	c.mu.Lock()
	c.loggedIn = false
	c.mu.Unlock()

	c.bus.Publish(events.EventForceLogout, events.Event{
		Type:       events.EventForceLogout,
		RawPayload: resp.Data,
		Timestamp:  timex.UnixMilli(),
	})

	// 异步断开，不阻塞 readLoop（panic 兜底：readLoop 上下文里 panic 会杀整个进程）
	safego.Go("client.disconnect", c.logger.Warnf, func() { _ = c.Disconnect() })
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

	// 服务端 2001 推送载荷是 PushMessages（会话ID → PullMsgs），非裸 MsgData
	// （msggateway hub_server.go SuperGroupOnlineBatchPushOneMsg：notification 走 NotificationMsgs）
	push := &pbsdk.PushMessages{}
	if err := proto.Unmarshal(data, push); err != nil {
		c.logger.Warnf("handleMessagePush: unmarshal PushMessages failed, reqIdentifier=%d, error=%v",
			resp.ReqIdentifier, err.Error())
		return
	}

	handle := func(convID string, msgData *pbsdk.MsgData) {
		if msgData == nil {
			return
		}
		mm := manager.ConvProtoMsgData(msgData)
		mm.ConversationID = convID
		// 本地持久化：推送消息/通知实时写入消息缓存（与 SeqSyncEngine gap 拉取
		// 入库同一路径，幂等；不推进 seq 游标——游标仍由同步引擎按缺口拉取推进，
		// 避免推送丢失/乱序时跳号漏消息）
		c.persistPushMsg(mm)
		// 系统通知：NotificationElem 两段 JSON 解出 tips，按 ContentType 分流
		if msgData.ContentType >= pbconstant.NotificationBegin {
			c.dispatchNotification(msgData, mm, timestamp)
			return
		}
		// 普通消息 → 更新会话摘要 + EventNewMsg
		// （payload 为已转换的 model，携带推送 map key 得到的 ConversationID）
		c.applyConversationSummary(mm, convID)
		c.bus.Publish(events.EventNewMsg, events.Event{
			Type:       events.EventNewMsg,
			Payload:    mm,
			RawPayload: data,
			Timestamp:  timestamp,
		})
	}
	for convID, pull := range push.Msgs {
		for _, msgData := range pull.GetMsgs() {
			handle(convID, msgData)
		}
	}
	for convID, pull := range push.NotificationMsgs {
		for _, msgData := range pull.GetMsgs() {
			handle(convID, msgData)
		}
	}
}

// notificationConvID 消息类通知（2101/2102/2200）的会话 ID：服务端把通知消息
// 归属到 n_ 前缀的通知会话（信封归属非原会话），tips 里携带的才是原会话 ID；
// tips 缺失时回退信封归属（兼容旧服务端）
func notificationConvID(tipConvID, fallback string) string {
	if tipConvID != "" {
		return tipConvID
	}
	return fallback
}

// dispatchNotification 按 ContentType 分流系统通知
//
// tips 解码策略：核心高频通知解码为具体 proto 类型；其余 payload 为 Detail JSON string，
// RawPayload 始终保留 MsgData 原始 bytes，上层可自行解析。
//
// 本地持久化：
//   - 消息类通知（2101 撤回 / 2102 删除 / 2200 已读）实时应用到本地消息缓存
//   - 实体类通知（好友/黑名单/群/会话）去抖触发实体增量同步（tips 不含完整实体数据，
//     由同步引擎按游标从服务端拉取变更，如 1300 仅携带会话 ID 列表）
func (c *DefaultClient) dispatchNotification(msgData *pbsdk.MsgData, mm *model.MsgData, timestamp int64) {
	detail := extractNotificationDetail(msgData.Content)
	data := convert.ToBytes(msgData.Content)

	publish := func(et events.EventType, payload proto.Message) {
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
	// —— 消息类通知（tips 类型确定；先应用到本地消息缓存再发事件，
	// 保证宿主在事件回调里读到最新缓存）——
	case pbconstant.MsgRevokeNotification: // 2101
		tip := &pbsdk.RevokeMsgTips{}
		if err := json.Unmarshal([]byte(detail), tip); err != nil {
			c.logger.Warnf("dispatchNotification: unmarshal revoke tips failed, error=%v", err.Error())
		}
		convID := notificationConvID(tip.ConversationID, mm.ConversationID)
		if tip.Seq > 0 {
			c.applyMsgCache("mark revoked", convID, func(owner string) error {
				return c.storage.MarkMsgsRevoked(owner, convID, []int64{tip.Seq})
			})
			c.applyConversationSummary(nil, convID)
		}
		publish(events.EventRevokeMsg, tip)
	case pbconstant.DeleteMsgsNotification: // 2102
		tip := &pbsdk.DeleteMsgsTips{}
		if err := json.Unmarshal([]byte(detail), tip); err != nil {
			c.logger.Warnf("dispatchNotification: unmarshal delete tips failed, error=%v", err.Error())
		}
		convID := notificationConvID(tip.ConversationID, mm.ConversationID)
		if len(tip.Seqs) > 0 {
			c.applyMsgCache("delete msgs", convID, func(owner string) error {
				return c.storage.DeleteMsgs(owner, convID, tip.Seqs)
			})
			c.applyConversationSummary(nil, convID)
		}
		publish(events.EventDeleteMsgs, tip)
	case pbconstant.HasReadReceipt: // 2200
		tip := &pbsdk.MarkAsReadTips{}
		if err := json.Unmarshal([]byte(detail), tip); err != nil {
			c.logger.Warnf("dispatchNotification: unmarshal mark-as-read tips failed, error=%v", err.Error())
		}
		convID := notificationConvID(tip.ConversationID, mm.ConversationID)
		// 只有「本人」的已读（其它端/其他设备）才落到本地消息缓存的 IsRead。
		// 对方读了我的消息时，服务端也会广播该 tips，但 HasReadSeq 是对方的
		// 阅读游标；若照此回写，会把我未读的入向消息一并标为已读，
		// 表现为「还没点开该会话，消息就全部已读」。
		if tip.MarkAsReadUserID == c.currentOwner() {
			c.applyMsgCache("mark read", convID, func(owner string) error {
				if len(tip.Seqs) > 0 {
					if err := c.storage.MarkMsgsRead(owner, convID, tip.Seqs); err != nil {
						return err
					}
				} else if err := c.storage.MarkMsgsReadUpTo(owner, convID, tip.HasReadSeq); err != nil {
					return err
				}
				// 已读游标取本端与 tips 的较大值
				readSeq := tip.HasReadSeq
				for _, seq := range tip.Seqs {
					if seq > readSeq {
						readSeq = seq
					}
				}
				return c.saveReadSeq(owner, convID, readSeq)
			})
		}
		publish(events.EventMarkAsRead, tip)

	// —— 用户 / 状态（用户资料无本地缓存，仅事件）——
	case pbconstant.UserInfoUpdatedNotification: // 1303
		decodeTip(events.EventUserInfoUpdated, &pbsdk.UserInfoUpdatedTips{})
	case pbconstant.UserStatusChangeNotification: // 1304
		decodeTip(events.EventUserStatusChange, &pbsdk.UserStatusChangeTips{})

	// —— 好友（好友/黑名单缓存随增量同步实时跟进）——
	case pbconstant.FriendApplicationNotification: // 1203 收到好友申请
		decodeTip(events.EventFriendApplication, &pbsdk.FriendApplicationTips{})
	case pbconstant.FriendApplicationApprovedNotification: // 1201 好友申请已同意
		decodeTip(events.EventFriendApplicationApproved, &pbsdk.FriendApplicationTips{})
		c.triggerEntitySync(sdkSync.KindFriend)
	case pbconstant.FriendApplicationRejectedNotification: // 1202 好友申请已拒绝
		decodeTip(events.EventFriendApplicationRejected, &pbsdk.FriendApplicationTips{})
	case pbconstant.FriendAddedNotification: // 1204
		decodeTip(events.EventFriendAdded, &pbsdk.FriendAddedTips{})
		c.triggerEntitySync(sdkSync.KindFriend)
	case pbconstant.FriendDeletedNotification: // 1205
		decodeTip(events.EventFriendDeleted, &pbsdk.FriendDeletedTips{})
	case pbconstant.FriendRemarkSetNotification,
		pbconstant.FriendInfoUpdatedNotification,
		pbconstant.FriendsInfoUpdateNotification: // 1206 / 1209 / 1210
		decodeTip(events.EventFriendInfoChanged, &pbsdk.FriendInfoChangedTips{})
		c.triggerEntitySync(sdkSync.KindFriend)
	case pbconstant.BlackAddedNotification: // 1207
		decodeTip(events.EventBlackAdded, &pbsdk.BlackAddedTips{})
		c.triggerEntitySync(sdkSync.KindBlack)
	case pbconstant.BlackDeletedNotification: // 1208
		decodeTip(events.EventBlackDeleted, &pbsdk.BlackDeletedTips{})
		c.triggerEntitySync(sdkSync.KindBlack)

	// —— 会话 ——
	case pbconstant.ConversationChangeNotification: // 1300
		decodeTip(events.EventConversationUpdate, &pbsdk.ConversationUpdateTips{})
		c.triggerEntitySync(sdkSync.KindConversation)

	// —— 群组 ——
	case pbconstant.GroupCreatedNotification: // 1501
		decodeTip(events.EventGroupCreated, &pbsdk.GroupCreatedTips{})
		c.triggerEntitySync(sdkSync.KindGroup)
	case pbconstant.GroupInfoSetNotification: // 1502
		decodeTip(events.EventGroupInfoSet, &pbsdk.GroupInfoSetTips{})
		c.triggerEntitySync(sdkSync.KindGroup)
	case pbconstant.JoinGroupApplicationNotification: // 1503
		decodeTip(events.EventJoinGroupApplication, &pbsdk.JoinGroupApplicationTips{})
	case pbconstant.GroupApplicationAcceptedNotification: // 1505
		decodeTip(events.EventGroupApplicationAccepted, &pbsdk.GroupApplicationAcceptedTips{})
		c.triggerEntitySync(sdkSync.KindGroup)
	case pbconstant.GroupApplicationRejectedNotification: // 1506
		decodeTip(events.EventGroupApplicationRejected, &pbsdk.GroupApplicationRejectedTips{})
	case pbconstant.GroupOwnerTransferredNotification: // 1507
		decodeTip(events.EventGroupOwnerTransferred, &pbsdk.GroupOwnerTransferredTips{})
		c.triggerEntitySync(sdkSync.KindGroup)
	case pbconstant.MemberKickedNotification: // 1508
		decodeTip(events.EventMemberKicked, &pbsdk.MemberKickedTips{})
		c.triggerEntitySync(sdkSync.KindGroup)
	case pbconstant.MemberInvitedNotification: // 1509
		decodeTip(events.EventMemberInvited, &pbsdk.MemberInvitedTips{})
		c.triggerEntitySync(sdkSync.KindGroup)
	case pbconstant.MemberEnterNotification: // 1510
		decodeTip(events.EventMemberEnter, &pbsdk.MemberEnterTips{})
		c.triggerEntitySync(sdkSync.KindGroup)
	case pbconstant.MemberQuitNotification: // 1504
		decodeTip(events.EventMemberQuit, &pbsdk.MemberQuitTips{})
		c.triggerEntitySync(sdkSync.KindGroup)
	case pbconstant.GroupDismissedNotification: // 1511
		decodeTip(events.EventGroupDismissed, &pbsdk.GroupDismissedTips{})
		c.triggerEntitySync(sdkSync.KindGroup)
	case pbconstant.GroupMemberMutedNotification: // 1512
		decodeTip(events.EventGroupMemberMuted, &pbsdk.GroupMemberMutedTips{})
		c.triggerEntitySync(sdkSync.KindGroup)
	case pbconstant.GroupMemberCancelMutedNotification: // 1513
		decodeTip(events.EventGroupMemberCancelMuted, &pbsdk.GroupMemberCancelMutedTips{})
		c.triggerEntitySync(sdkSync.KindGroup)
	case pbconstant.GroupMutedNotification: // 1514
		decodeTip(events.EventGroupMuted, &pbsdk.GroupMutedTips{})
		c.triggerEntitySync(sdkSync.KindGroup)
	case pbconstant.GroupCancelMutedNotification: // 1515
		decodeTip(events.EventGroupCancelMuted, &pbsdk.GroupCancelMutedTips{})
		c.triggerEntitySync(sdkSync.KindGroup)

	default:
		c.logger.Warnf("dispatchNotification: unmapped contentType=%d, clientMsgID=%s",
			msgData.ContentType, msgData.ClientMsgID)
	}
}

// persistPushMsg 服务端实时推送的消息/通知写入本地消息缓存（owner 未登录或 seq<=0 跳过，
// 与存储层 UpsertMsgs 约定一致）
func (c *DefaultClient) persistPushMsg(mm *model.MsgData) {
	owner := c.currentOwner()
	if owner == "" || mm == nil || mm.Seq <= 0 || mm.ConversationID == "" {
		return
	}
	// 本端已读是本地状态：实时推送到达的他人消息对本端必然未读。
	// 服务端推送的 IsRead 可能是发送者视角（发送即已读），直接采信会把
	// 新消息 seq 计入 hasReadSeq，导致 Local.UnreadCount 本地推导为 0。
	if mm.SendID != owner {
		mm.IsRead = false
	}
	if err := c.storage.UpsertMsgs(owner, []*model.MsgData{mm}); err != nil {
		c.logger.Warnf("client: persist pushed msg, conv=%s, seq=%d, error=%v",
			mm.ConversationID, mm.Seq, err.Error())
	}
	c.logger.Infof("client: persist pushed msg, conv=%s, msgID=%s, seq=%d", mm.ConversationID, mm.ClientMsgID, mm.Seq)
}

// applySentConversationSummary 发送成功后更新会话摘要（会话 ID 由消息收发双方推导）
//
// 发送消息本地不入消息缓存（seq 由服务端分配），因此摘要直接由发送消息构造；
// 后续会话增量同步 upsert 时会保留该本地摘要，不会被服务端空摘要覆盖。
func (c *DefaultClient) applySentConversationSummary(msg *model.MsgData, resp *model.SendMsgResp) {
	if msg == nil {
		return
	}
	conversationID := model.ChatConversationID(msg.SessionType, msg.SendID, msg.RecvID, msg.GroupID)
	if conversationID == "" {
		return
	}
	out := *msg
	out.ConversationID = conversationID
	if resp != nil {
		out.ServerMsgID = resp.ServerMsgID
		out.SendTime = resp.SendTime
	}
	c.applyConversationSummary(&out, conversationID)
}

// applyConversationSummary 消息变化统一入口：发送 / 接收 / 撤回 / 删除后维护会话摘要，
// 完成后发布 EventConversationUpdate，宿主监听事件重读本地会话即可，无需自己维护摘要。
//
//   - msg != nil（发送成功 / 实时收到）：直接用该消息推进摘要。发送消息本地不入消息缓存
//     （seq 由服务端分配），无法从缓存重算；比当前摘要更早的乱序消息不回退摘要。
//   - msg == nil（撤回 / 删除后）：本地消息缓存已变更，按本地最新一条重算——最新一条被
//     撤回 → 摘要改写为 111 Revoke；被删除 → 回退到上一条（本地已无消息则清空）。
func (c *DefaultClient) applyConversationSummary(msg *model.MsgData, conversationID string) {
	owner := c.currentOwner()
	if owner == "" || conversationID == "" || manager.IsNotificationConversation(conversationID) {
		return
	}

	if msg == nil {
		// 撤回/删除：按本地最新消息重算（refreshSummary 内部加锁）
		c.refreshSummary(owner, conversationID)
		return
	}

	c.convSummaryMu.Lock()
	defer c.convSummaryMu.Unlock()
	c.updateConversationSummary(owner, msg, conversationID)
}

// refreshSummary 会话摘要重算实现（撤回/删除后按本地最新消息回退摘要），
// 同时注入给 SeqSyncEngine——补拉通知路径复用同一实现、同一把写锁。
//
// 会话摘要的事实源是本地消息缓存：最新一条被撤回 → 摘要改写为 111 Revoke；
// 被删除 → 回退到上一条（本地已无消息则清空）。宿主只需监听
// EventConversationUpdate 重新读取本地会话，不需要自己维护摘要。
func (c *DefaultClient) refreshSummary(owner, conversationID string) {
	if owner == "" || conversationID == "" {
		return
	}
	c.convSummaryMu.Lock()
	defer c.convSummaryMu.Unlock()
	c.refreshConversationSummaryLocked(owner, conversationID)
}

// refreshConversationSummaryLocked 重算并落盘会话摘要；调用方需持有 convSummaryMu。
func (c *DefaultClient) refreshConversationSummaryLocked(owner, conversationID string) {
	conv, err := c.storage.GetConversation(owner, conversationID)
	if err != nil {
		c.logger.Warnf("client: refresh conversation summary, get conv error=%v", err.Error())
		return
	}
	if conv == nil {
		return
	}
	latest, err := c.storage.GetLatestMsg(owner, conversationID)
	if err != nil {
		c.logger.Warnf("client: refresh conversation summary, get latest msg error=%v", err.Error())
		return
	}
	conv.MsgInfo = model.SummaryInfoFromMsg(latest)
	conv.LatestMsgSendTime = 0
	if latest != nil {
		conv.LatestMsgSendTime = latest.SendTime
	}
	if err := c.storage.UpsertConversations(owner, []*model.Conversation{conv}); err != nil {
		c.logger.Warnf("client: refresh conversation summary, conv=%s, error=%v", conversationID, err.Error())
		return
	}
	c.bus.Publish(events.EventConversationUpdate, events.Event{
		Type: events.EventConversationUpdate,
		Payload: &events.ConversationUpdateTips{
			UserID:             owner,
			ConversationIDList: []string{conversationID},
		},
		Timestamp: timex.UnixMilli(),
	})
}

// updateConversationSummary 用消息维护本地会话摘要（MsgInfo + latestMsgSendTime）；
// 调用方需持有 convSummaryMu。
func (c *DefaultClient) updateConversationSummary(owner string, msg *model.MsgData, conversationID string) {
	conv, err := c.storage.GetConversation(owner, conversationID)
	if err != nil {
		c.logger.Warnf("client: update conversation summary, get conv error=%v", err.Error())
		return
	}
	if conv == nil {
		conv = &model.Conversation{ConversationID: conversationID}
	}
	// 推送乱序：比当前摘要更早的消息不回退摘要
	if conv.LatestMsgSendTime > 0 && msg.SendTime > 0 && msg.SendTime < conv.LatestMsgSendTime {
		return
	}

	if conv.ConversationType == 0 {
		conv.ConversationType = msg.SessionType
	}
	if msg.SessionType == model.SessionTypeC2C {
		if conv.UserID == "" {
			if msg.SendID == owner {
				conv.UserID = msg.RecvID
			} else {
				conv.UserID = msg.SendID
			}
		}
	} else if conv.GroupID == "" {
		conv.GroupID = msg.GroupID
	}
	conv.MsgInfo = model.MsgInfoFromMsg(msg)
	conv.LatestMsgSendTime = msg.SendTime

	if err := c.storage.UpsertConversations(owner, []*model.Conversation{conv}); err != nil {
		c.logger.Warnf("client: update conversation summary, conv=%s, error=%v", conversationID, err.Error())
		return
	}
	c.bus.Publish(events.EventConversationUpdate, events.Event{
		Type: events.EventConversationUpdate,
		Payload: &events.ConversationUpdateTips{
			UserID:             owner,
			ConversationIDList: []string{conversationID},
		},
		Timestamp: timex.UnixMilli(),
	})
}

// applyMsgCache 消息类通知应用到本地消息缓存
//
// 未登录/无会话上下文跳过；失败仅告警不阻断事件分发——缓存最终由
// SeqSyncEngine 缺口拉取收敛（撤回/删除/已读状态在拉取时由服务端改写）。
func (c *DefaultClient) applyMsgCache(name, conversationID string, fn func(owner string) error) {
	owner := c.currentOwner()
	if owner == "" || conversationID == "" {
		return
	}
	if err := fn(owner); err != nil {
		c.logger.Warnf("client: apply %s to msg cache, conv=%s, error=%v", name, conversationID, err.Error())
	}
}

// applySelfReadMark 本人已读上报成功后回写本地：消息 IsRead + 会话已读游标
//
// 游标是 Local.UnreadCount 的主依据，消息 IsRead 是游标缺失时的离线兜底；
// 两者不同步会出现「读过刷新后又有未读」。
func (c *DefaultClient) applySelfReadMark(conversationID string, hasReadSeq int64, seqs []int64) {
	c.applyMsgCache("self mark read", conversationID, func(owner string) error {
		readSeq := hasReadSeq
		for _, seq := range seqs {
			if seq > readSeq {
				readSeq = seq
			}
		}
		if readSeq <= 0 {
			return nil
		}
		// 已读上限是连续的：≤readSeq 的消息都视为已读，无需再逐条标记 seqs
		if err := c.storage.MarkMsgsReadUpTo(owner, conversationID, readSeq); err != nil {
			return err
		}
		return c.saveReadSeq(owner, conversationID, readSeq)
	})
}

// saveReadSeq 持久化会话已读游标（只前进不回退），供 Local.UnreadCount 使用
func (c *DefaultClient) saveReadSeq(owner, conversationID string, seq int64) error {
	if owner == "" || conversationID == "" || seq <= 0 {
		return nil
	}
	entity := fmt.Sprintf(storage.EntityReadSeqFmt, conversationID)
	_, cur, err := c.storage.GetVersion(owner, entity)
	if err != nil {
		return err
	}
	if uint64(seq) <= cur {
		return nil
	}
	return c.storage.SetVersion(owner, entity, "", uint64(seq))
}

// refreshReadSeqs 同步服务端记录的会话已读游标
//
// 未读数以服务端 hasReadSeq 为基准，换机/重装后本地没有已读标记也能算对；
// 放在实体同步之后执行，保证会话列表已落库。
func (c *DefaultClient) refreshReadSeqs(ctx context.Context, owner string) error {
	conversationIDs, err := c.allConversationIDs(owner)
	if err != nil {
		return fmt.Errorf("refresh read seqs: list conversations: %w", err)
	}
	const batch = 100
	for start := 0; start < len(conversationIDs); start += batch {
		end := start + batch
		if end > len(conversationIDs) {
			end = len(conversationIDs)
		}
		seqs, _, err := c.msg.GetConversationsHasReadAndMaxSeq(ctx, conversationIDs[start:end], false)
		if err != nil {
			return fmt.Errorf("refresh read seqs: %w", err)
		}
		for conversationID, info := range seqs {
			if info == nil || info.HasReadSeq <= 0 {
				continue
			}
			if err := c.saveReadSeq(owner, conversationID, info.HasReadSeq); err != nil {
				return fmt.Errorf("refresh read seqs: save %s: %w", conversationID, err)
			}
		}
	}
	return nil
}

// allConversationIDs 本地会话列表的全部 ID（分页收集）
func (c *DefaultClient) allConversationIDs(owner string) ([]string, error) {
	const pageSize = 200
	out := make([]string, 0, pageSize)
	for offset := int32(0); ; {
		page, err := c.storage.ListConversationsPage(owner, offset, pageSize)
		if err != nil {
			return nil, err
		}
		for _, conv := range page.Items {
			if conv != nil && conv.ConversationID != "" {
				out = append(out, conv.ConversationID)
			}
		}
		offset += pageSize
		if !page.HasMore || int64(offset) >= page.Total {
			return out, nil
		}
	}
}

// Local 本地数据能力门面（只读本地缓存，不发网络请求；设计见 docs/LOCAL_DATA_API.md）
func (c *DefaultClient) Local() manager.LocalDataMgr {
	if c.local != nil {
		return c.local
	}
	// 测试等直接构造 DefaultClient 的场景：按需构造（依赖 storage + 当前 owner）
	return manager.NewLocalDataMgr(c.storage, c.currentOwner)
}

// sessionOwner 当前登录态快照（userID 与 loggedIn 一并读取，避免多次加锁不一致）
func (c *DefaultClient) sessionOwner() (owner string, loggedIn bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.userID, c.loggedIn
}

// currentOwner 当前登录 userID 只读快照（未登录返回空串）
func (c *DefaultClient) currentOwner() string {
	owner, _ := c.sessionOwner()
	return owner
}

// localOwner 当前登录用户（本地查询的数据隔离维度）
func (c *DefaultClient) localOwner() (string, error) {
	owner, _ := c.sessionOwner()
	if owner == "" {
		return "", fmt.Errorf("client: not logged in")
	}
	return owner, nil
}

// GetHistoryMsgs 服务端分页历史消息（降序，最新在前），结果写入本地消息缓存。
//
// 分页：第 1 页传 beforeSeq<=0；之后传上一页的 OldestSeq-1（服务端区间为闭区间）。
func (c *DefaultClient) GetHistoryMsgs(ctx context.Context, conversationID string, beforeSeq int64, count int32) (*model.HistoryMsgsPage, error) {
	page, err := c.msg.PullHistoryMsgs(ctx, conversationID, 1, beforeSeq, count, 1)
	if err != nil {
		return nil, err
	}
	c.cacheFetchedMsgs("history", conversationID, page)
	return page, nil
}

// GetNewerMsgs 服务端分页增量消息（升序），用于补齐比本地更新的消息，结果写入本地缓存。
func (c *DefaultClient) GetNewerMsgs(ctx context.Context, conversationID string, afterSeq int64, count int32) (*model.HistoryMsgsPage, error) {
	begin := afterSeq
	if begin < 0 {
		begin = 0
	}
	page, err := c.msg.PullHistoryMsgs(ctx, conversationID, begin+1, 0, count, 0)
	if err != nil {
		return nil, err
	}
	c.cacheFetchedMsgs("newer", conversationID, page)
	return page, nil
}

// cacheFetchedMsgs 把服务端拉取的消息写入本地缓存（未登录/空页时跳过）
func (c *DefaultClient) cacheFetchedMsgs(name, conversationID string, page *model.HistoryMsgsPage) {
	if page == nil || len(page.Msgs) == 0 {
		return
	}
	owner, err := c.localOwner()
	if err != nil {
		return
	}
	if err := c.storage.UpsertMsgs(owner, page.Msgs); err != nil {
		c.logger.Warnf("client: cache %s msgs of conv=%s, error=%v", name, conversationID, err.Error())
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
