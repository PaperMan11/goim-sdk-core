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

	// 模拟服务端 2001 推送 MsgData（handleMessagePush probe 命中 RecvID != "" → EventNewMsg）
	data, err := proto.Marshal(&pbsdk.MsgData{
		RecvID:      "me",
		SendID:      "alice",
		Content:     "hello",
		ContentType: 101,
	})
	if err != nil {
		t.Fatal(err)
	}
	m.router.Route(&transport.Response{ReqIdentifier: transport.WSPushMsg, Data: data})

	select {
	case msg := <-done:
		if msg.Content != "hello" || msg.SendID != "alice" {
			t.Fatalf("unexpected msg: %+v", msg)
		}
		if msg.MsgType != 101 { // proto.ContentType → model.MsgType 转换正确性
			t.Fatalf("MsgType = %d, want 101", msg.MsgType)
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
	data, err := proto.Marshal(&pbsdk.MsgData{
		ContentType: pbconstant.MsgRevokeNotification,
		Content:     string(content),
	})
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
	data, err := proto.Marshal(&pbsdk.MsgData{
		ContentType: 1666, // 未映射通知
		Content:     string(content),
	})
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
