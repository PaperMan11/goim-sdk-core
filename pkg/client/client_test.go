package client

import (
	"context"
	"encoding/json"
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
