package call

import (
	"context"
	"encoding/json"

	"github.com/PaperMan11/goim-sdk-core/pkg/events"
)

// EventPayload 跨语言导出的事件载荷（gomobile 可绑定：全基本类型）
//
// events.Event.Payload 是 interface{} 无法直接导出，统一序列化为 JSON string。
type EventPayload struct {
	Type      int32  `json:"type"`              // events.EventType 数值
	Timestamp int64  `json:"timestamp"`         // 事件时间戳（毫秒）
	Payload   string `json:"payload,omitempty"` // 具体事件对象的 JSON 序列化（空串表示无载荷）
}

// CallBack 唯一的跨语言回调契约（gomobile 硬约束：Native 只能实现单方法 interface）
//
// Native 侧实现一次即可收全量事件，按 eventType 字符串自行分发：
//
//	type GoimCallback : CallBack {
//	    override fun onEvent(eventType: String, payloadJSON: String) { ... }
//	}
type CallBack interface {
	// OnEvent 全部事件经此分发
	//   eventType: events.EventType 的字符串形式（如 "EventNewMsg"）
	//   payloadJSON: EventPayload 序列化结果
	OnEvent(eventType string, payloadJSON string)
}

// Subscribe 订阅指定事件类型，返回取消函数
//
// onEvent 收到的是 EventPayload 的 JSON string。
// 返回的 func() 由 gomobile 绑定为 Unsubscriber（iOS closure / Android lambda）。
func (d *Dispatcher) Subscribe(eventType int32, onEvent func(eventJSON string)) func() {
	d.mu.RLock()
	c := d.cli
	d.mu.RUnlock()
	if c == nil {
		return func() {}
	}

	ctx, cancel := context.WithCancel(context.Background())
	ch, unsub := c.Subscribe(events.EventType(eventType))

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case ev, ok := <-ch:
				if !ok {
					return
				}
				if bs, err := marshalEvent(ev); err == nil {
					onEvent(string(bs))
				}
			}
		}
	}()

	return func() {
		cancel()
		unsub()
	}
}

// RegisterCallback 注册全量事件回调（Native 单点注册入口）
//
// 内部 = SubscribeAll + goroutine → CallBack.OnEvent。
// 重复注册覆盖旧回调；SetClient / Init 重建 Client 后自动重绑，无需重新注册。
// 传 nil 注销。
func (d *Dispatcher) RegisterCallback(cb CallBack) {
	d.mu.Lock()
	d.cb = cb
	d.mu.Unlock()
	d.bindCallback()
}

// bindCallback 按当前 (cli, cb) 状态建立 / 取消全量事件订阅
func (d *Dispatcher) bindCallback() {
	d.mu.Lock()
	if d.cbCancel != nil {
		d.cbCancel()
		d.cbCancel = nil
	}
	cb, cli := d.cb, d.cli
	if cb == nil || cli == nil {
		d.mu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	d.cbCancel = cancel
	d.mu.Unlock()

	ch, unsub := cli.SubscribeAll()
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
				bs, err := marshalEvent(ev)
				if err != nil {
					continue
				}
				cb.OnEvent(eventTypeName(ev.Type), string(bs))
			}
		}
	}()
}

// marshalEvent events.Event → EventPayload JSON
//
// Payload 优先序列化已反序列化对象；仅有 RawPayload（proto bytes）时无法
// 跨语言还原具体类型，载荷置空（core 层 PushHandler 均已填充 Payload）。
func marshalEvent(ev events.Event) ([]byte, error) {
	p := EventPayload{Type: int32(ev.Type), Timestamp: ev.Timestamp}
	if ev.Payload != nil {
		bs, err := json.Marshal(ev.Payload)
		if err != nil {
			return nil, err
		}
		p.Payload = string(bs)
	}
	return json.Marshal(p)
}

// eventTypeName EventType → 字符串形式（RegisterCallback 的 eventType 参数）
func eventTypeName(t events.EventType) string {
	if int(t) >= 1 && int(t) < len(eventTypeNames) {
		return eventTypeNames[t]
	}
	return "EventUnknown"
}

// eventTypeNames 与 events.EventType 枚举一一对应（index 0 占位）
var eventTypeNames = []string{
	"",
	"EventNewMsg",                    // 1
	"EventRevokeMsg",                 // 2
	"EventDeleteMsgs",                // 3
	"EventMarkAsRead",                // 4
	"EventConversationUpdate",        // 5
	"EventConversationDelete",        // 6
	"EventFriendApplication",         // 7
	"EventFriendApplicationApproved", // 8
	"EventFriendApplicationRejected", // 9
	"EventFriendAdded",               // 10
	"EventFriendDeleted",             // 11
	"EventFriendInfoChanged",         // 12
	"EventBlackAdded",                // 13
	"EventBlackDeleted",              // 14
	"EventGroupCreated",              // 15
	"EventGroupInfoSet",              // 16
	"EventJoinGroupApplication",      // 17
	"EventGroupApplicationAccepted",  // 18
	"EventGroupApplicationRejected",  // 19
	"EventMemberEnter",               // 20
	"EventMemberInvited",             // 21
	"EventMemberQuit",                // 22
	"EventMemberKicked",              // 23
	"EventGroupOwnerTransferred",     // 24
	"EventGroupDismissed",            // 25
	"EventGroupMemberMuted",          // 26
	"EventGroupMemberCancelMuted",    // 27
	"EventGroupMuted",                // 28
	"EventGroupCancelMuted",          // 29
	"EventUserInfoUpdated",           // 30
	"EventUserStatusChange",          // 31
	"EventForceLogout",               // 32
	"EventBackgroundStatusChanged",   // 33
	"EventConnect",                   // 34
	"EventConnecting",                // 35
	"EventDisconnect",                // 36
	"EventReconnect",                 // 37
	"EventLoginSuccess",              // 38
	"EventLoginFailed",               // 39
	"EventTokenRefreshed",            // 40
	"EventSyncStart",                 // 41
	"EventSyncFinish",                // 42
	"EventSyncFailed",                // 43
	"EventSeqSynced",                 // 44
	"EventFriendListSynced",          // 45
	"EventBlackListSynced",           // 46
	"EventGroupListSynced",           // 47
	"EventGroupMemberSynced",         // 48
	"EventConversationSynced",        // 49
	"EventMsgSent",                   // 50
	"EventMsgRevoked",                // 51
	"EventGroupMemberInvited",        // 52
	"EventConversationCreated",       // 53
}
