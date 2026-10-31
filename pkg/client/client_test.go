package client

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/PaperMan11/goim-sdk-core/pkg/events"
	"github.com/PaperMan11/goim-sdk-core/pkg/logger"
	"github.com/PaperMan11/goim-sdk-core/pkg/model"
	"github.com/PaperMan11/goim-sdk-core/pkg/transport"

	pbconstant "github.com/PaperMan11/goim/pkg/protocol/constant"
	pbsdk "github.com/PaperMan11/goim/pkg/protocol/sdkws"
)

// mockTransport 测试用 transport，仅记录钩子 + 提供真实 MessageRouter
type mockTransport struct {
	router         *transport.MessageRouter
	params         transport.ConnectParams
	connected      bool
	onConnecting   func()
	onReconnected  func(ctx context.Context)
	onDisconnected func()
}

func newMockTransport() *mockTransport {
	return &mockTransport{router: transport.NewMessageRouter(&logger.NopLogger{})}
}

func (m *mockTransport) Connect(ctx context.Context) error { m.connected = true; return nil }
func (m *mockTransport) Disconnect() error                 { m.connected = false; return nil }
func (m *mockTransport) IsConnected() bool                 { return m.connected }
func (m *mockTransport) Status() transport.TransportStatus {
	if m.connected {
		return transport.TransportStatusConnected
	}
	return transport.TransportStatusDisconnected
}
func (m *mockTransport) SendRequest(ctx context.Context, req *transport.Request) error {
	return nil
}
func (m *mockTransport) SendRequestAndWait(ctx context.Context, req *transport.Request) (*transport.Response, error) {
	return nil, nil
}
func (m *mockTransport) Router() *transport.MessageRouter { return m.router }
func (m *mockTransport) SetConnectParams(p transport.ConnectParams) {
	m.params = p
}
func (m *mockTransport) SetOnConnecting(fn func())                     { m.onConnecting = fn }
func (m *mockTransport) SetOnReconnected(fn func(ctx context.Context)) { m.onReconnected = fn }
func (m *mockTransport) SetOnDisconnected(fn func())                   { m.onDisconnected = fn }

// waitForSubscribe 等待异步回调（回调里 close/写入 done），超时 fail
func waitForSubscribe(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("callback not triggered within 2s")
	}
}

func newTestClient(t *testing.T) (*DefaultClient, *mockTransport) {
	t.Helper()
	m := newMockTransport()
	cli := NewClient(&ClientConfig{Transport: m})
	return cli.(*DefaultClient), m
}

func TestOnConnecting(t *testing.T) {
	cli, m := newTestClient(t)

	done := make(chan struct{})
	cli.OnConnecting(func() { close(done) })

	if m.onConnecting == nil {
		t.Fatal("SetOnConnecting hook not registered by NewClient")
	}
	m.onConnecting() // 模拟 transport 进入 Connecting 状态

	waitForSubscribe(t, done)
}

func TestOnConnect(t *testing.T) {
	cli, _ := newTestClient(t)

	done := make(chan struct{})
	cli.OnConnect(func() { close(done) })

	if err := cli.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	waitForSubscribe(t, done)
}

func TestOnNewMessage(t *testing.T) {
	cli, m := newTestClient(t)

	done := make(chan *model.MsgData, 1)
	cli.OnNewMessage(func(msg *model.MsgData) { done <- msg })

	// 模拟服务端 2001 推送：PushMessages 包装（会话ID → PullMsgs）
	data, err := proto.Marshal(pushEnvelope(&pbsdk.MsgData{
		RecvID:      "me",
		SendID:      "alice",
		Content:     "hello",
		ContentType: 101,
	}))
	if err != nil {
		t.Fatal(err)
	}
	m.router.Route(&transport.Response{ReqIdentifier: transport.WSPushMsg, Data: data})

	select {
	case msg := <-done:
		if msg.Content != "hello" || msg.SendID != "alice" {
			t.Fatalf("unexpected msg: %+v", msg)
		}
		if msg.ContentType != 101 { // proto.ContentType → model.ContentType 直通
			t.Fatalf("ContentType = %d, want 101", msg.ContentType)
		}
		if msg.ConversationID != "si_c2c_test" { // 推送 map key → 本地 ConversationID
			t.Fatalf("ConversationID = %q, want si_c2c_test", msg.ConversationID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("OnNewMessage not triggered within 2s")
	}
}

func TestOnMessageRevoked(t *testing.T) {
	cli, m := newTestClient(t)

	done := make(chan string, 1)
	cli.OnMessageRevoked(func(clientMsgID string) { done <- clientMsgID })

	// 服务端真实形态：2101 通知的 Content = JSON(NotificationElem{Detail: JSON(RevokeMsgTips)})
	detail, err := json.Marshal(&pbsdk.RevokeMsgTips{ClientMsgID: "cm-123"})
	if err != nil {
		t.Fatal(err)
	}
	content, err := json.Marshal(&pbsdk.NotificationElem{Detail: string(detail)})
	if err != nil {
		t.Fatal(err)
	}
	data, err := proto.Marshal(pushEnvelope(&pbsdk.MsgData{
		ContentType: pbconstant.MsgRevokeNotification,
		Content:     string(content),
	}))
	if err != nil {
		t.Fatal(err)
	}
	m.router.Route(&transport.Response{ReqIdentifier: transport.WSPushMsg, Data: data})

	select {
	case id := <-done:
		if id != "cm-123" {
			t.Fatalf("clientMsgID = %q, want cm-123", id)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("OnMessageRevoked not triggered within 2s")
	}
}

func TestNotificationUnmapped(t *testing.T) {
	// 未映射的 ContentType（如信令 1600+）不应 panic，也不应误发 EventNewMsg
	cli, m := newTestClient(t)

	normalCh, unsub := cli.Subscribe(events.EventNewMsg)
	defer unsub()

	detail, err := json.Marshal(&pbsdk.RevokeMsgTips{ClientMsgID: "unmapped-x"})
	if err != nil {
		t.Fatal(err)
	}
	content, err := json.Marshal(&pbsdk.NotificationElem{Detail: string(detail)})
	if err != nil {
		t.Fatal(err)
	}
	data, err := proto.Marshal(pushEnvelope(&pbsdk.MsgData{
		ContentType: 1666, // 未映射通知
		Content:     string(content),
	}))
	if err != nil {
		t.Fatal(err)
	}
	m.router.Route(&transport.Response{ReqIdentifier: transport.WSPushMsg, Data: data})

	select {
	case <-normalCh:
		t.Fatal("unmapped notification must not be published as EventNewMsg")
	case <-time.After(300 * time.Millisecond):
	}
}

// pushEnvelope 按服务端 msggateway 真实形态包装 2001 推送载荷
// （PushMessages{Msgs: {convID → PullMsgs{Msgs: [...]}}}）
func pushEnvelope(msg *pbsdk.MsgData) *pbsdk.PushMessages {
	return &pbsdk.PushMessages{
		Msgs: map[string]*pbsdk.PullMsgs{
			"si_c2c_test": {Msgs: []*pbsdk.MsgData{msg}},
		},
	}
}

// loginTestClient 将 client 置为已登录态（推送持久化依赖 owner）
func loginTestClient(cli *DefaultClient, owner string) {
	cli.mu.Lock()
	cli.userID = owner
	cli.loggedIn = true
	cli.mu.Unlock()
}

// seedMsg 预置消息到本地缓存（模拟此前 gap 拉取入库）
func seedMsg(t *testing.T, cli *DefaultClient, owner, convID string, seq int64, content string) {
	t.Helper()
	if err := cli.storage.UpsertMsgs(owner, []*model.MsgData{{
		ClientMsgID:    fmt.Sprintf("cm-%d", seq),
		ConversationID: convID,
		Seq:            seq,
		SendID:         "alice",
		RecvID:         owner,
		Content:        content,
		ContentType:    101,
	}}); err != nil {
		t.Fatal(err)
	}
}

// notificationMsg 构造系统通知推送（Content = JSON(NotificationElem{Detail: JSON(tips)})）
func notificationMsg(t *testing.T, contentType int32, tips interface{}, seq int64) *pbsdk.MsgData {
	t.Helper()
	detail, err := json.Marshal(tips)
	if err != nil {
		t.Fatal(err)
	}
	content, err := json.Marshal(&pbsdk.NotificationElem{Detail: string(detail)})
	if err != nil {
		t.Fatal(err)
	}
	return &pbsdk.MsgData{ContentType: contentType, Content: string(content), Seq: seq}
}

// TestPushMessagePersisted 实时推送的普通消息应写入本地消息缓存（seq 游标不动，由同步引擎推进）
func TestPushMessagePersisted(t *testing.T) {
	cli, m := newTestClient(t)
	loginTestClient(cli, "me")

	done := make(chan *model.MsgData, 1)
	cli.OnNewMessage(func(msg *model.MsgData) { done <- msg })

	data, err := proto.Marshal(pushEnvelope(&pbsdk.MsgData{
		RecvID: "me", SendID: "alice", Content: "hello", ContentType: 101, Seq: 5, ClientMsgID: "cm-5",
	}))
	if err != nil {
		t.Fatal(err)
	}
	m.router.Route(&transport.Response{ReqIdentifier: transport.WSPushMsg, Data: data})

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("OnNewMessage not triggered within 2s")
	}

	msgs, err := cli.storage.ListMsgsByConversation("me", "si_c2c_test")
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 1 || msgs[0].Seq != 5 || msgs[0].Content != "hello" {
		t.Fatalf("pushed msg not persisted: %+v", msgs)
	}
	// seq 游标不得被推送推进（推送可能丢失，缺口由同步引擎拉取补齐）
	maxSeqs, _, err := cli.storage.GetMaxSeqs("me")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := maxSeqs["si_c2c_test"]; ok {
		t.Fatalf("push must not advance seq cursor, got maxSeq=%d", maxSeqs["si_c2c_test"])
	}
}

// TestPushRevokeAppliesToCache 2101 撤回通知应实时改写本地缓存的消息状态
func TestPushRevokeAppliesToCache(t *testing.T) {
	cli, m := newTestClient(t)
	loginTestClient(cli, "me")
	seedMsg(t, cli, "me", "si_c2c_test", 3, "to-revoke")

	done := make(chan string, 1)
	cli.OnMessageRevoked(func(clientMsgID string) { done <- clientMsgID })

	data, err := proto.Marshal(pushEnvelope(notificationMsg(t,
		pbconstant.MsgRevokeNotification,
		&pbsdk.RevokeMsgTips{ClientMsgID: "cm-3", Seq: 3, ConversationID: "si_c2c_test", RevokerUserID: "alice"},
		0)))
	if err != nil {
		t.Fatal(err)
	}
	m.router.Route(&transport.Response{ReqIdentifier: transport.WSPushMsg, Data: data})

	select {
	case id := <-done:
		if id != "cm-3" {
			t.Fatalf("clientMsgID = %q, want cm-3", id)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("OnMessageRevoked not triggered within 2s")
	}

	msgs, err := cli.storage.ListMsgsByConversation("me", "si_c2c_test")
	if err != nil {
		t.Fatal(err)
	}
	for _, msg := range msgs {
		if msg.Seq == 3 && msg.Status != model.MsgStatusRevoked {
			t.Fatalf("seq=3 status=%d, want MsgStatusRevoked(%d)", msg.Status, model.MsgStatusRevoked)
		}
	}
}

// TestPushDeleteAppliesToCache 2102 删除通知应实时删除本地缓存的消息
func TestPushDeleteAppliesToCache(t *testing.T) {
	cli, m := newTestClient(t)
	loginTestClient(cli, "me")
	seedMsg(t, cli, "me", "si_c2c_test", 1, "m1")
	seedMsg(t, cli, "me", "si_c2c_test", 2, "m2")

	done := make(chan struct{}, 1)
	ch, unsub := cli.Subscribe(events.EventDeleteMsgs)
	defer unsub()
	go func() {
		for range ch {
			done <- struct{}{}
		}
	}()

	data, err := proto.Marshal(pushEnvelope(notificationMsg(t,
		pbconstant.DeleteMsgsNotification,
		&pbsdk.DeleteMsgsTips{UserID: "me", ConversationID: "si_c2c_test", Seqs: []int64{1, 2}},
		0)))
	if err != nil {
		t.Fatal(err)
	}
	m.router.Route(&transport.Response{ReqIdentifier: transport.WSPushMsg, Data: data})

	waitForSubscribe(t, done)

	msgs, err := cli.storage.ListMsgsByConversation("me", "si_c2c_test")
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 0 {
		t.Fatalf("deleted msgs still in cache: %+v", msgs)
	}
}

// TestPushMarkAsReadAppliesToCache 2200 已读回执应实时标记本地缓存（seqs 列表 + hasReadSeq 兜底）
func TestPushMarkAsReadAppliesToCache(t *testing.T) {
	cli, m := newTestClient(t)
	loginTestClient(cli, "me")
	seedMsg(t, cli, "me", "si_c2c_test", 1, "m1")
	seedMsg(t, cli, "me", "si_c2c_test", 2, "m2")
	seedMsg(t, cli, "me", "si_c2c_test", 3, "m3")

	done := make(chan struct{}, 2)
	ch, unsub := cli.Subscribe(events.EventMarkAsRead)
	defer unsub()
	go func() {
		for range ch {
			done <- struct{}{}
		}
	}()

	// 第一条：仅标记 seq=2 已读
	data, err := proto.Marshal(pushEnvelope(notificationMsg(t,
		pbconstant.HasReadReceipt,
		&pbsdk.MarkAsReadTips{MarkAsReadUserID: "alice", ConversationID: "si_c2c_test", Seqs: []int64{2}},
		0)))
	if err != nil {
		t.Fatal(err)
	}
	m.router.Route(&transport.Response{ReqIdentifier: transport.WSPushMsg, Data: data})
	waitForSubscribe(t, done)

	// 第二条：seqs 为空 + hasReadSeq=3 → 标记 ≤3 全部已读
	data, err = proto.Marshal(pushEnvelope(notificationMsg(t,
		pbconstant.HasReadReceipt,
		&pbsdk.MarkAsReadTips{MarkAsReadUserID: "alice", ConversationID: "si_c2c_test", HasReadSeq: 3},
		0)))
	if err != nil {
		t.Fatal(err)
	}
	m.router.Route(&transport.Response{ReqIdentifier: transport.WSPushMsg, Data: data})
	waitForSubscribe(t, done)

	msgs, err := cli.storage.ListMsgsByConversation("me", "si_c2c_test")
	if err != nil {
		t.Fatal(err)
	}
	bySeq := map[int64]*model.MsgData{}
	for _, msg := range msgs {
		bySeq[msg.Seq] = msg
	}
	for _, seq := range []int64{1, 2, 3} {
		if msg := bySeq[seq]; msg == nil || !msg.IsRead {
			t.Fatalf("seq=%d should be marked read, got %+v", seq, msg)
		}
	}
}

// TestPushNotificationTriggersEntitySync 实体类通知（1204 好友添加）应去抖触发
// 实体增量同步，把变更实时拉入本地缓存（无需等待重连）
func TestPushNotificationTriggersEntitySync(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		write := func(data interface{}) {
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"code": 0, "msg": "", "data": data})
		}
		switch r.URL.Path {
		case "/relation/get_incremental_friends":
			write(map[string]interface{}{
				"versionID": "fv1", "version": 1,
				"insert": []map[string]interface{}{
					{"ownerUserID": "u1", "friendUserID": "bob", "createTime": 1},
				},
			})
		case "/relation/get_incremental_blacks",
			"/group/get_incremental_join_groups",
			"/conversation/get_incremental_conversations":
			write(map[string]interface{}{"versionID": "v1", "version": 1})
		default:
			write(map[string]interface{}{})
		}
	}))
	defer srv.Close()

	m := newMockTransport()
	cli := NewClient(&ClientConfig{BaseURL: srv.URL, Transport: m}).(*DefaultClient)
	loginTestClient(cli, "u1")

	synced := make(chan struct{}, 1)
	ch, unsub := cli.Subscribe(events.EventFriendListSynced)
	defer unsub()
	go func() {
		for range ch {
			synced <- struct{}{}
		}
	}()

	// 1204 好友添加通知（tips 无完整实体数据，靠触发同步拉取）
	data, err := proto.Marshal(pushEnvelope(notificationMsg(t,
		pbconstant.FriendAddedNotification,
		&pbsdk.FromToUserID{FromUserID: "alice", ToUserID: "u1"},
		9)))
	if err != nil {
		t.Fatal(err)
	}
	m.router.Route(&transport.Response{ReqIdentifier: transport.WSPushMsg, Data: data})

	// 去抖窗口（500ms）结束后触发同步 → EventFriendListSynced
	waitForSubscribe(t, synced)

	friends, err := cli.storage.ListFriends("u1")
	if err != nil {
		t.Fatal(err)
	}
	if len(friends) != 1 || friends[0].FriendUserID != "bob" {
		t.Fatalf("friend cache not updated by notification-triggered sync: %+v", friends)
	}
}

// TestAcquireUserTokenStoresToken 登录链路 token 存取验证（排查"登录成功后未保存 token"报告）：
//  1. AdminSecret bootstrap：get_user_token 请求携带 admin token（R7）
//  2. 响应 token/expireTimeSeconds 正确解析（服务端字段名 token/expireTimeSeconds 已比对 pb.go）
//  3. ★ token 写入 TokenStore（user + admin 两个键都可读回，且未过期）
//  4. SetCurrentUser 后业务请求携带用户 token 而非 admin token（R1）
func TestAcquireUserTokenStoresToken(t *testing.T) {
	var gotUserTokenHeader string // 业务接口收到的 token 头
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/auth/get_admin_token":
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"code": 0, "msg": "",
				"data": map[string]interface{}{"token": "admin-tok", "expireTimeSeconds": 3600},
			})
		case "/auth/get_user_token":
			// bootstrap 时当前用户是 admin → 必须带 admin token
			if got := r.Header.Get("token"); got != "admin-tok" {
				t.Errorf("get_user_token should carry admin token, got %q", got)
			}
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"code": 0, "msg": "",
				"data": map[string]interface{}{"token": "user-tok", "expireTimeSeconds": 3600},
			})
		default:
			gotUserTokenHeader = r.Header.Get("token")
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"code": 0, "msg": ""})
		}
	}))
	defer srv.Close()

	m := newMockTransport()
	cli := NewClient(&ClientConfig{
		BaseURL:     srv.URL,
		Transport:   m,
		AdminSecret: "test-secret",
		Logging:     &logger.Config{Level: logger.LevelDebug},
	}).(*DefaultClient)

	// 1. bootstrap 获取用户 token（与 Login 首步一致）
	resp, err := cli.acquireUserToken(context.Background(), "u1", 1)
	if err != nil {
		t.Fatalf("acquireUserToken: %v", err)
	}
	if resp.Token != "user-tok" || resp.ExpireTimeSeconds != 3600 {
		t.Fatalf("resp = %+v, want token=user-tok expire=3600", resp)
	}

	// 2. ★ 核心断言：user token 已写入 TokenStore 且可读回
	tok, err := cli.tokens.Get("u1", 1)
	if err != nil || tok != "user-tok" {
		t.Fatalf("token store missing user token: get(u1,1) = %q, %v", tok, err)
	}
	// admin token 同样缓存（复用路径依赖）
	atok, err := cli.tokens.Get(defaultAdminUserID, pbconstant.AdminPlatformID)
	if err != nil || atok != "admin-tok" {
		t.Fatalf("token store missing admin token: get = %q, %v", atok, err)
	}

	// 3. loginFlow 的选路注入后，业务请求应携带用户 token（而非残留的 admin token）
	if setter, ok := cli.http.(transport.CurrentUserSetter); ok {
		setter.SetCurrentUser("u1", 1)
	}
	if err := cli.http.Post(context.Background(), "/user/get_users_info", nil, nil); err != nil {
		t.Fatalf("business post: %v", err)
	}
	if gotUserTokenHeader != "user-tok" {
		t.Fatalf("business request token header = %q, want user-tok (R1)", gotUserTokenHeader)
	}
}

// TestRefreshRecoversFromServerSideInvalidation 服务端短 TTL 场景回归：
// admin token 记录已被服务端清除（1507）但本地缓存"未过期"时，刷新链必须
// 强制重取 admin token 恢复，而不是复用旧缓存无限递归（无 Delete 修复时本测试会栈溢出）。
func TestRefreshRecoversFromServerSideInvalidation(t *testing.T) {
	var adminCalls, userCalls atomic.Int32
	var lastBizToken atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/auth/get_admin_token":
			n := adminCalls.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"code": 0, "msg": "",
				"data": map[string]interface{}{"token": fmt.Sprintf("admin-tok-%d", n), "expireTimeSeconds": 3600},
			})
		case "/auth/get_user_token":
			userCalls.Add(1)
			if got := r.Header.Get("token"); got == "admin-tok-1" {
				// 模拟：admin-tok-1 的服务端记录已被短 TTL 清除
				_ = json.NewEncoder(w).Encode(map[string]interface{}{"code": 1507, "msg": "token does not exist"})
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"code": 0, "msg": "",
				"data": map[string]interface{}{"token": "user-tok-2", "expireTimeSeconds": 3600},
			})
		default:
			lastBizToken.Store(r.Header.Get("token"))
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"code": 0, "msg": ""})
		}
	}))
	defer srv.Close()

	cli := NewClient(&ClientConfig{
		BaseURL:     srv.URL,
		Transport:   newMockTransport(),
		AdminSecret: "test-secret",
	}).(*DefaultClient)

	// 首次获取用户 token：携带 admin-tok-1 → 1507 → Refresh(admin) → 闭包 Delete
	// → 重取 admin-tok-2 → GetUserToken 成功 → 恢复
	resp, err := cli.acquireUserToken(context.Background(), "u1", 1)
	if err != nil {
		t.Fatalf("acquireUserToken should recover via forced re-bootstrap, err=%v", err)
	}
	if resp.Token != "user-tok-2" {
		t.Fatalf("token = %q, want user-tok-2", resp.Token)
	}
	if n := adminCalls.Load(); n != 2 {
		t.Errorf("get_admin_token called %d times, want 2 (stale + re-fetched)", n)
	}
	// user token 已保存（首次登录保存链路）
	if tok, _ := cli.tokens.Get("u1", 1); tok != "user-tok-2" {
		t.Errorf("stored user token = %q, want user-tok-2", tok)
	}
	// admin token 已更新为新值
	if tok, _ := cli.tokens.Get(defaultAdminUserID, pbconstant.AdminPlatformID); tok != "admin-tok-2" {
		t.Errorf("stored admin token = %q, want admin-tok-2", tok)
	}
	// 获取完成后 HTTP 选路必须回到目标用户，而非残留 admin 挂载
	// （残留会导致重试及后续业务请求以 admin 身份发送）
	if err := cli.http.Post(context.Background(), "/user/get_users_info", nil, nil); err != nil {
		t.Fatalf("business post after acquire: %v", err)
	}
	if got, _ := lastBizToken.Load().(string); got != "user-tok-2" {
		t.Errorf("post-acquire business token = %q, want user-tok-2 (curUser must be restored)", got)
	}
}
