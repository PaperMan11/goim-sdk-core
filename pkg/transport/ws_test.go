package transport

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/PaperMan11/goim-sdk-core/pkg/logger"
	"github.com/gorilla/websocket"
)

// newTestWSServer 起一个测试用 WS 服务端，handler 拿到升级后的 conn 自行处理
func newTestWSServer(t *testing.T, handler func(conn *websocket.Conn, r *http.Request)) (*httptest.Server, string) {
	t.Helper()

	upgrader := websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade failed: %v", err)
			return
		}
		handler(conn, r)
	}))

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws"
	return srv, wsURL
}

func TestEncodeQuery(t *testing.T) {
	p := ConnectParams{
		UserID:       "u1",
		Token:        "tok",
		PlatformID:   2,
		DeviceID:     "dev-1",
		OperationID:  "op-1",
		SDKType:      GoSDK,
		SDKVersion:   "1.0.0",
		Compression:  "none",
		SendResponse: true,
		IsBackground: false,
	}

	q := encodeQuery(p)
	checks := map[string]string{
		"userID":       "u1",
		"token":        "tok",
		"platformID":   "2",
		"deviceID":     "dev-1",
		"operationID":  "op-1",
		"sdkType":      "go",
		"sdkVersion":   "1.0.0",
		"compression":  "none",
		"sendResponse": "true",
		"isBackground": "false",
	}
	for k, want := range checks {
		if got := q.Get(k); got != want {
			t.Errorf("query[%s] = %q, want %q", k, got, want)
		}
	}
}

func TestNewWSClient_Defaults(t *testing.T) {
	w := NewWSClient("ws://example.com/ws", ConnectParams{}, &logger.NopLogger{})

	if w.params.SDKType != GoSDK {
		t.Errorf("SDKType = %q, want go", w.params.SDKType)
	}
	if w.params.Compression != "none" {
		t.Errorf("Compression = %q, want none", w.params.Compression)
	}
	if w.params.OperationID == "" {
		t.Error("OperationID should be auto-generated")
	}
	if w.IsConnected() {
		t.Error("should not be connected before Connect")
	}
}

// 端到端：连接 → 校验认证 query → SendRequestAndWait 收到 echo 响应
func TestWSClient_ConnectAndRequestResponse(t *testing.T) {
	srv, wsURL := newTestWSServer(t, func(conn *websocket.Conn, r *http.Request) {
		defer conn.Close()

		// 校验认证参数
		if r.URL.Query().Get("userID") != "u1" {
			t.Errorf("userID = %q", r.URL.Query().Get("userID"))
		}
		if r.URL.Query().Get("token") != "tok-1" {
			t.Errorf("token = %q", r.URL.Query().Get("token"))
		}

		for {
			_, raw, err := conn.ReadMessage()
			if err != nil {
				return
			}
			// 客户端 compression=none / encoder=json → raw 即 JSON
			var req Request
			if err := json.Unmarshal(raw, &req); err != nil {
				t.Errorf("unmarshal request: %v", err)
				return
			}
			resp := Response{
				ReqIdentifier: req.ReqIdentifier,
				OperationID:   req.OperationID,
				Data:          []byte(`{"echo":true}`),
			}
			out, _ := json.Marshal(resp)
			if err := conn.WriteMessage(websocket.BinaryMessage, out); err != nil {
				return
			}
		}
	})
	defer srv.Close()

	c := NewWSClient(wsURL, ConnectParams{
		UserID: "u1",
		Token:  "tok-1",
	}, &logger.NopLogger{})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := c.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	defer c.Disconnect()

	if !c.IsConnected() {
		t.Fatal("should be connected")
	}

	resp, err := c.SendRequestAndWait(ctx, &Request{
		ReqIdentifier: WSSendMsg,
		Data:          []byte(`{"hello":"world"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(resp.Data), "echo") {
		t.Fatalf("resp.Data = %s", resp.Data)
	}
	if resp.OperationID == "" {
		t.Fatal("OperationID should be echoed back")
	}
}

// 服务端主动推送 → 注册的 push handler 收到
func TestWSClient_PushDispatch(t *testing.T) {
	srv, wsURL := newTestWSServer(t, func(conn *websocket.Conn, r *http.Request) {
		defer conn.Close()

		// 连接建立后主动推一条 WSPushMsg
		resp := Response{
			ReqIdentifier: WSPushMsg,
			Data:          []byte(`{"msg":"new-message"}`),
		}
		out, _ := json.Marshal(resp)
		_ = conn.WriteMessage(websocket.BinaryMessage, out)

		// 保持连接直到客户端断开
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	})
	defer srv.Close()

	c := NewWSClient(wsURL, ConnectParams{UserID: "u1"}, &logger.NopLogger{})

	got := make(chan *Response, 1)
	c.Router().RegisterPush(WSPushMsg, func(resp *Response) {
		got <- resp
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := c.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	defer c.Disconnect()

	select {
	case resp := <-got:
		if resp.ReqIdentifier != WSPushMsg {
			t.Fatalf("ReqIdentifier = %d", resp.ReqIdentifier)
		}
		if !strings.Contains(string(resp.Data), "new-message") {
			t.Fatalf("Data = %s", resp.Data)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("push not received")
	}
}

// 未连接时发送 → notConnectedError
func TestWSClient_SendNotConnected(t *testing.T) {
	c := NewWSClient("ws://127.0.0.1:1/ws", ConnectParams{}, &logger.NopLogger{})

	err := c.SendRequest(context.Background(), &Request{ReqIdentifier: WSSendMsg})
	if err == nil {
		t.Fatal("expected not connected error")
	}
	if !strings.Contains(err.Error(), "not connected") {
		t.Fatalf("err = %v", err)
	}
}

// 断开后发送 → notConnectedError
func TestWSClient_DisconnectThenSend(t *testing.T) {
	srv, wsURL := newTestWSServer(t, func(conn *websocket.Conn, r *http.Request) {
		defer conn.Close()
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	})
	defer srv.Close()

	c := NewWSClient(wsURL, ConnectParams{}, &logger.NopLogger{})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := c.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	if err := c.Disconnect(); err != nil {
		t.Fatal(err)
	}

	if c.IsConnected() {
		t.Fatal("should be disconnected")
	}

	err := c.SendRequest(context.Background(), &Request{ReqIdentifier: WSSendMsg})
	if err == nil || !strings.Contains(err.Error(), "not connected") {
		t.Fatalf("err = %v, want not connected", err)
	}
}

// 断开后应触发重连（服务端先断 → 客户端自动重拨）
func TestWSClient_AutoReconnect(t *testing.T) {
	var connCount int32

	srv, wsURL := newTestWSServer(t, func(conn *websocket.Conn, r *http.Request) {
		atomic.AddInt32(&connCount, 1)
		defer conn.Close()
		// 第一次连接：立刻从服务端断开，触发客户端重连
		if atomic.LoadInt32(&connCount) == 1 {
			return
		}
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	})
	defer srv.Close()

	reconnected := make(chan struct{}, 1)
	c := NewWSClient(wsURL, ConnectParams{}, &logger.NopLogger{})
	c.OnReconnected = func(ctx context.Context) {
		select {
		case reconnected <- struct{}{}:
		default:
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := c.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	defer c.Disconnect()

	// 等第一次连接被服务端断开 + 重连成功
	select {
	case <-reconnected:
	case <-time.After(5 * time.Second):
		t.Fatal("auto reconnect did not happen")
	}

	if atomic.LoadInt32(&connCount) < 2 {
		t.Fatalf("connCount = %d, want >= 2", connCount)
	}
}

// SendRequestAndWait 在连接断开时应返回 not connected（而非卡到超时）
func TestWSClient_PendingWokenOnDisconnect(t *testing.T) {
	srv, wsURL := newTestWSServer(t, func(conn *websocket.Conn, r *http.Request) {
		defer conn.Close()
		// 只读不回 → 客户端 pending 挂起，等断开
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	})
	defer srv.Close()

	c := NewWSClient(wsURL, ConnectParams{}, &logger.NopLogger{})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := c.Connect(ctx); err != nil {
		t.Fatal(err)
	}

	errCh := make(chan error, 1)
	go func() {
		_, err := c.SendRequestAndWait(context.Background(), &Request{
			ReqIdentifier: WSSendMsg,
			Data:          []byte(`{}`),
		})
		errCh <- err
	}()

	// 等请求确实发出去了再断开
	time.Sleep(200 * time.Millisecond)
	_ = c.Disconnect()

	select {
	case err := <-errCh:
		if err == nil || !strings.Contains(err.Error(), "not connected") {
			t.Fatalf("err = %v, want not connected", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("SendRequestAndWait not woken up by disconnect")
	}
}
