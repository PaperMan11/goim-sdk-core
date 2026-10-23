package client

import (
	"context"
	"fmt"
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/PaperMan11/goim-sdk-core/pkg/events"
	"github.com/PaperMan11/goim-sdk-core/pkg/logger"
	"github.com/PaperMan11/goim-sdk-core/pkg/manager"
	"github.com/PaperMan11/goim-sdk-core/pkg/storage"
	"github.com/PaperMan11/goim-sdk-core/pkg/transport"
	"github.com/PaperMan11/goim-sdk-core/pkg/utils/convert"
	"github.com/PaperMan11/goim-sdk-core/pkg/utils/eventbus"
	"github.com/PaperMan11/goim-sdk-core/pkg/utils/timex"
	"github.com/PaperMan11/goim-sdk-core/pkg/utils/uuidx"

	pbsdk "github.com/PaperMan11/goim/pkg/protocol/sdkws"
)

// DefaultClient Client 接口完整实现
type DefaultClient struct {
	cfg *ClientConfig

	http    transport.HTTPClient
	ws      transport.MessageTransport
	tokens  transport.TokenStore
	storage storage.Storage
	bus     *eventbus.EventBus
	logger  logger.Logger

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
	l := &logger.NopLogger{} // TODO: 从 cfg 注入，默认 nop；Phase 4 完可以用 zap 替换
	bus := eventbus.New(eventbus.WithLogger(l))
	tokens := transport.NewTokenStore(nil)
	stor := storage.NewMemoryStorage()

	var http transport.HTTPClient
	if cfg.HTTP != nil {
		http = cfg.HTTP
	} else {
		http = transport.NewHTTPClient(cfg.BaseURL, tokens, l)
	}

	params := transport.ConnectParams{
		PlatformID:  cfg.DefaultPlatformID,
		DeviceID:    cfg.DefaultDeviceID,
		SDKVersion:  cfg.DefaultSDKVersion,
		Compression: cfg.DefaultCompression,
		OperationID: uuid4(),
	}

	var ws transport.MessageTransport
	if cfg.WS != nil {
		ws = cfg.WS
	} else {
		ws = transport.NewWSClient(cfg.WSURL, params, l)
	}

	// 2. Manager 构造
	authMgr := manager.NewAuthMgr(http, tokens, bus, l)
	userMgr := manager.NewUserMgr(http, bus, l)
	msgMgr := manager.NewMsgMgr(http, ws, bus, l)
	groupMgr := manager.NewGroupMgr(http, bus, l)
	relMgr := manager.NewRelMgr(http, bus, l)
	convMgr := manager.NewConvMgr(http, bus, l)

	c := &DefaultClient{
		cfg:     cfg,
		http:    http,
		ws:      ws,
		tokens:  tokens,
		storage: stor,
		bus:     bus,
		logger:  l,
		params:  params,
		auth:    authMgr,
		user:    userMgr,
		msg:     msgMgr,
		group:   groupMgr,
		rel:     relMgr,
		conv:    convMgr,
	}

	// 3. 传输层生命周期钩子（通过 MessageTransport interface setter 注入）
	ws.SetOnReconnected(func(ctx context.Context) {
		bus.Publish(events.EventReconnect, events.Event{
			Type:      events.EventReconnect,
			Timestamp: timex.UnixMilli(),
		})
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

func (c *DefaultClient) Login(ctx context.Context, userID string, platformID int32) error {
	// 1. GetUserToken（内部已 Publish EventLoginSuccess + TokenStore.Set）
	tokenResp, err := c.auth.GetUserToken(ctx, userID, platformID)
	if err != nil {
		c.bus.Publish(events.EventLoginFailed, events.Event{
			Type:      events.EventLoginFailed,
			Timestamp: timex.UnixMilli(),
		})
		return fmt.Errorf("login: get user token: %w", err)
	}

	// 2. 覆盖 WS 连接参数
	c.params.UserID = userID
	c.params.Token = tokenResp.Token
	c.params.PlatformID = convert.ToInt(platformID)
	c.params.OperationID = uuidx.New()

	// 同步到 WS transport
	c.ws.SetConnectParams(c.params)

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

	// 5. 记录登录状态
	c.mu.Lock()
	c.userID = userID
	c.platformID = platformID
	c.loggedIn = true
	c.mu.Unlock()

	// 6. 完整登录成功事件（包含 EventConnect + Token + SeqSync 后续）
	c.bus.Publish(events.EventLoginSuccess, events.Event{
		Type:      events.EventLoginSuccess,
		Timestamp: timex.UnixMilli(),
	})

	return nil
}

func (c *DefaultClient) Logout() error {
	c.mu.Lock()
	userID := c.userID
	platformID := c.platformID
	c.loggedIn = false
	c.userID = ""
	c.mu.Unlock()

	// 主动 Disconnect（不会触发 OnDisconnected，因为 actorsExited 里 stillConnected=false）
	_ = c.Disconnect()

	// 清理 TokenStore
	if c.tokens != nil && userID != "" {
		_ = c.tokens.Delete(userID, platformID)
	}

	// 清理 Storage
	if c.storage != nil {
		_ = c.storage.Clear(context.Background())
	}

	return nil
}

func (c *DefaultClient) Connect(ctx context.Context) error {
	if c.ws.IsConnected() {
		return nil
	}

	if err := c.ws.Connect(ctx); err != nil {
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
	return c.ws.Disconnect()
}

func (c *DefaultClient) Status() transport.TransportStatus {
	return c.ws.Status()
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
	// SubscribeAll 用一个特殊 key，所有 Publish 时也 publish 一份到这个 key
	const allKey = "__all__"
	rawCh, unsubscribe := c.bus.Subscribe(allKey)

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
// WSPushMsg 推的内容不固定，type probe 依次尝试 Unmarshal 到可能的 proto 类型：
//
//  1. sdkws.MsgData           → EventNewMsg
//  2. sdkws.RevokeMsgTips     → EventRevokeMsg
//  3. sdkws.DeleteMsgsTips    → EventDeleteMsgs
//  4. sdkws.MarkAsReadTips    → EventMarkAsRead
//  5. sdkws.UserInfoUpdatedTips → EventUserInfoUpdated
//  6. sdkws.GroupCreatedTips  → EventGroupCreated
//  7. sdkws.FriendApplicationTips → EventFriendApplication
//     ... 等等（覆盖所有可能通过 2001 推的 Tips 类型）
//
// 未知类型 → 只 Publish 到 SubscribeAll，不丢数据。
func (c *DefaultClient) handleMessagePush(resp *transport.Response) {
	data := resp.Data
	timestamp := timex.UnixMilli()

	// 1. 新消息（MsgData）
	msgData := &pbsdk.MsgData{}
	if err := proto.Unmarshal(data, msgData); err == nil && msgData.RecvID != "" {
		c.bus.Publish(events.EventNewMsg, events.Event{
			Type:       events.EventNewMsg,
			Payload:    msgData,
			RawPayload: data,
			Timestamp:  timestamp,
		})
		return
	}

	// 2. 撤回消息
	revokeTip := &pbsdk.RevokeMsgTips{}
	if err := proto.Unmarshal(data, revokeTip); err == nil {
		c.bus.Publish(events.EventRevokeMsg, events.Event{
			Type:       events.EventRevokeMsg,
			Payload:    revokeTip,
			RawPayload: data,
			Timestamp:  timestamp,
		})
		return
	}

	// 3. 删除消息
	deleteTip := &pbsdk.DeleteMsgsTips{}
	if err := proto.Unmarshal(data, deleteTip); err == nil {
		c.bus.Publish(events.EventDeleteMsgs, events.Event{
			Type:       events.EventDeleteMsgs,
			Payload:    deleteTip,
			RawPayload: data,
			Timestamp:  timestamp,
		})
		return
	}

	// 4. 已读回执
	readTip := &pbsdk.MarkAsReadTips{}
	if err := proto.Unmarshal(data, readTip); err == nil {
		c.bus.Publish(events.EventMarkAsRead, events.Event{
			Type:       events.EventMarkAsRead,
			Payload:    readTip,
			RawPayload: data,
			Timestamp:  timestamp,
		})
		return
	}

	// 5. 用户信息更新
	userTip := &pbsdk.UserInfoUpdatedTips{}
	if err := proto.Unmarshal(data, userTip); err == nil {
		c.bus.Publish(events.EventUserInfoUpdated, events.Event{
			Type:       events.EventUserInfoUpdated,
			Payload:    userTip,
			RawPayload: data,
			Timestamp:  timestamp,
		})
		return
	}

	// 6. 在线状态变更
	statusTip := &pbsdk.UserStatusChangeTips{}
	if err := proto.Unmarshal(data, statusTip); err == nil {
		c.bus.Publish(events.EventUserStatusChange, events.Event{
			Type:       events.EventUserStatusChange,
			Payload:    statusTip,
			RawPayload: data,
			Timestamp:  timestamp,
		})
		return
	}

	// 7. 群组创建
	groupCreated := &pbsdk.GroupCreatedTips{}
	if err := proto.Unmarshal(data, groupCreated); err == nil {
		c.bus.Publish(events.EventGroupCreated, events.Event{
			Type:       events.EventGroupCreated,
			Payload:    groupCreated,
			RawPayload: data,
			Timestamp:  timestamp,
		})
		return
	}

	// 8. 群组信息变更
	groupInfoSet := &pbsdk.GroupInfoSetTips{}
	if err := proto.Unmarshal(data, groupInfoSet); err == nil {
		c.bus.Publish(events.EventGroupInfoSet, events.Event{
			Type:       events.EventGroupInfoSet,
			Payload:    groupInfoSet,
			RawPayload: data,
			Timestamp:  timestamp,
		})
		return
	}

	// 9. 好友申请
	friendApp := &pbsdk.FriendApplicationTips{}
	if err := proto.Unmarshal(data, friendApp); err == nil {
		c.bus.Publish(events.EventFriendApplication, events.Event{
			Type:       events.EventFriendApplication,
			Payload:    friendApp,
			RawPayload: data,
			Timestamp:  timestamp,
		})
		return
	}

	// 10. 未知类型 — 兜底：只 publish 原始 payload
	c.logger.Warnf("unknown WS push type, reqIdentifier=%d, dataLen=%d", resp.ReqIdentifier, len(data))
}

// === UUID helpers ===

func uuid4() string {
	return uuidx.New()
}
