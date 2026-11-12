package call

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/PaperMan11/goim-sdk-core/pkg/client"
	"github.com/PaperMan11/goim-sdk-core/pkg/events"
	"github.com/PaperMan11/goim-sdk-core/pkg/logger"
	"github.com/PaperMan11/goim-sdk-core/pkg/model"
	"github.com/PaperMan11/goim-sdk-core/pkg/storage"
)

// newTestDispatcher 构建内存存储 + 静默日志的 Client 并注入 Dispatcher
func newTestDispatcher(t *testing.T) *Dispatcher {
	t.Helper()
	c := client.NewClient(&client.ClientConfig{
		BaseURL:  "http://127.0.0.1:1/api/v1", // 不可达，仅用于构造
		Endpoint: "ws://127.0.0.1:1/ws",
		Logger:   &logger.NopLogger{},
		Storage:  storage.NewMemoryStorage(),
	})
	if c == nil {
		t.Fatal("NewClient returned nil")
	}
	d := NewDispatcher()
	d.SetClient(c)
	t.Cleanup(func() {
		d.Close()
		_ = c.Close()
	})
	return d
}

// === 方法分发表 ===

func TestMethodTableCoverage(t *testing.T) {
	d := newTestDispatcher(t)

	var names []string
	if err := json.Unmarshal([]byte(d.Methods()), &names); err != nil {
		t.Fatalf("Methods not JSON: %v", err)
	}
	set := make(map[string]bool, len(names))
	for _, n := range names {
		set[n] = true
	}

	// 生命周期方法
	for _, m := range []string{"Login", "LoginWithToken", "Logout", "Connect", "Disconnect", "Status", "SyncState"} {
		if !set[m] {
			t.Errorf("missing lifecycle method %q", m)
		}
	}
	// Manager 代表方法
	for _, m := range []string{
		"MsgMgr.SendMsg", "MsgMgr.NewestSeq", "MsgMgr.PullMsgBySeq",
		"UserMgr.GetUsersInfo", "GroupMgr.CreateGroup", "RelMgr.AddFriend",
		"ConvMgr.GetAllConversations", "ThirdMgr.PartSize", "AuthMgr.ParseToken",
		"FileUploader.Abort",
	} {
		if !set[m] {
			t.Errorf("missing manager method %q", m)
		}
	}
	// io.Reader 参数无法跨语言 → 不应注册
	if set["FileUploader.Upload"] {
		t.Error("FileUploader.Upload should not be registered (io.Reader param)")
	}
}

// === 同步调用：只读状态方法（登录前合法，走白名单） ===

func TestSyncCallStatus(t *testing.T) {
	d := newTestDispatcher(t)

	result, err := d.SyncCall("Status", "")
	if err != nil {
		t.Fatalf("Status failed: %v", err)
	}
	if result != "0" { // TransportStatusDisconnected
		t.Errorf("Status = %s, want 0", result)
	}
}

// === 错误归一化 ===

func TestInvokeNotInitialized(t *testing.T) {
	d := NewDispatcher() // 未 SetClient
	_, err := d.SyncCall("Status", "")
	assertCode(t, err, ErrCodeNotInitialized)
}

func TestInvokeUnknownMethod(t *testing.T) {
	d := newTestDispatcher(t)
	_, err := d.SyncCall("MsgMgr.NoSuchMethod", "[]")
	assertCode(t, err, ErrCodeUnknownMethod)
}

func TestInvokeNotLoggedIn(t *testing.T) {
	d := newTestDispatcher(t)
	// 非白名单方法 + 未登录
	_, err := d.SyncCall("UserMgr.GetUsersInfo", `[["u1"]]`)
	assertCode(t, err, ErrCodeNotLoggedIn)
}

func TestInvokeAuthBypass(t *testing.T) {
	d := newTestDispatcher(t)
	// ParseToken 在白名单内 → 不应该是 NotLoggedIn（会因网络不可达失败，但错误码不同）
	_, err := d.SyncCall("AuthMgr.ParseToken", `["fake-token"]`)
	if err == nil {
		t.Fatal("expect error (server unreachable)")
	}
	assertNotCode(t, err, ErrCodeNotLoggedIn)
}

func TestInvokeInvalidArgs(t *testing.T) {
	d := newTestDispatcher(t)

	// 非 JSON 数组
	_, err := d.SyncCall("Status", `{"a":1}`)
	assertCode(t, err, ErrCodeInvalidArgs)

	// 参数个数不匹配
	_, err = d.SyncCall("Status", `[1]`)
	assertCode(t, err, ErrCodeInvalidArgs)

	// 参数类型不匹配
	_, err = d.SyncCall("Login", `[123, "not-a-number"]`)
	assertCode(t, err, ErrCodeInvalidArgs)
}

func TestNormalizeError(t *testing.T) {
	code, msg := NormalizeError(nil)
	if code != model.SuccessCode || msg != "" {
		t.Errorf("nil error → (%d, %q)", code, msg)
	}

	code, msg = NormalizeError(model.NewSDKError(model.ErrCodeTokenExpiredError, "token expired"))
	if code != 1501 || msg != "token expired" {
		t.Errorf("SDKError → (%d, %q)", code, msg)
	}

	code, msg = NormalizeError(errors.New("boom"))
	if code != model.ErrCodeInternalError || msg != "boom" {
		t.Errorf("plain error → (%d, %q)", code, msg)
	}
}

// === panic 不穿透 ===

func TestPanicRecover(t *testing.T) {
	d := newTestDispatcher(t)
	_, err := d.withMiddleware("Fake.Method", func() (string, error) {
		panic("simulated panic")
	})
	assertCode(t, err, model.ErrCodeInternalError)
	if !strings.Contains(err.Error(), "simulated panic") {
		t.Errorf("panic msg lost: %v", err)
	}
}

// === 异步调用 ===

func TestCallAsync(t *testing.T) {
	d := newTestDispatcher(t)

	type result struct {
		json string
		code int32
		msg  string
	}
	done := make(chan result, 1)
	d.Call("Status", "",
		func(s string) { done <- result{json: s} },
		func(c int32, m string) { done <- result{code: c, msg: m} })

	select {
	case r := <-done:
		if r.json != "0" {
			t.Errorf("async Status = %+v, want json=0", r)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Call timeout")
	}
}

func TestCallAsyncError(t *testing.T) {
	d := newTestDispatcher(t)

	done := make(chan int32, 1)
	d.Call("UserMgr.GetUsersInfo", `[["u1"]]`,
		func(string) { t.Error("should not succeed") },
		func(c int32, m string) { done <- c })

	select {
	case code := <-done:
		if code != ErrCodeNotLoggedIn {
			t.Errorf("code = %d, want %d", code, ErrCodeNotLoggedIn)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Call timeout")
	}
}

// === 事件桥接 ===

type captureCallback struct {
	ch chan [2]string
}

func (c *captureCallback) OnEvent(eventType, payloadJSON string) {
	select {
	case c.ch <- [2]string{eventType, payloadJSON}:
	default:
	}
}

func TestRegisterCallback(t *testing.T) {
	d := newTestDispatcher(t)
	cb := &captureCallback{ch: make(chan [2]string, 4)}
	d.RegisterCallback(cb)

	// 触发事件：Login 失败（服务器不可达）会发布 EventLoginFailed
	_, _ = d.SyncCall("Login", `["u1", 1]`)

	select {
	case ev := <-cb.ch:
		if ev[0] != "EventLoginFailed" {
			t.Errorf("eventType = %q, want EventLoginFailed", ev[0])
		}
		var p EventPayload
		if err := json.Unmarshal([]byte(ev[1]), &p); err != nil {
			t.Fatalf("payload not EventPayload JSON: %v", err)
		}
		if p.Type != int32(events.EventLoginFailed) {
			t.Errorf("payload.Type = %d, want %d", p.Type, events.EventLoginFailed)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no event received")
	}
}

func TestSubscribe(t *testing.T) {
	d := newTestDispatcher(t)

	got := make(chan string, 1)
	unsub := d.Subscribe(int32(events.EventLoginFailed), func(eventJSON string) {
		select {
		case got <- eventJSON:
		default:
		}
	})
	defer unsub()

	_, _ = d.SyncCall("Login", `["u1", 1]`)

	select {
	case js := <-got:
		var p EventPayload
		if err := json.Unmarshal([]byte(js), &p); err != nil {
			t.Fatalf("bad event JSON: %v", err)
		}
		if p.Type != int32(events.EventLoginFailed) {
			t.Errorf("type = %d", p.Type)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no event received")
	}
}

func TestEventTypeName(t *testing.T) {
	cases := map[events.EventType]string{
		events.EventNewMsg:       "EventNewMsg",
		events.EventConnecting:   "EventConnecting",
		events.EventLoginSuccess: "EventLoginSuccess",
		events.EventType(999):    "EventUnknown",
	}
	for et, want := range cases {
		if got := eventTypeName(et); got != want {
			t.Errorf("eventTypeName(%d) = %q, want %q", et, got, want)
		}
	}
	// 全枚举覆盖：1..53 不得出现 EventUnknown
	for i := events.EventType(1); i <= events.EventConversationCreated; i++ {
		if eventTypeName(i) == "EventUnknown" {
			t.Errorf("eventTypeName(%d) uncovered", i)
		}
	}
}

// === TokenProvider 桥 ===

type fakeTokenBridge struct {
	result string
	err    error
}

func (f *fakeTokenBridge) FetchToken(userID string, platformID int32) (string, error) {
	return f.result, f.err
}

func TestWrapTokenProvider(t *testing.T) {
	fn := wrapTokenProvider(&fakeTokenBridge{result: `{"token":"tok123","expireTimeSeconds":3600}`})
	token, expire, err := fn(t.Context(), "u1", 1)
	if err != nil || token != "tok123" || expire != 3600 {
		t.Errorf("got (%q, %d, %v)", token, expire, err)
	}

	// 桥返回错误
	_, _, err = wrapTokenProvider(&fakeTokenBridge{err: errors.New("native fail")})(t.Context(), "u1", 1)
	if err == nil || !strings.Contains(err.Error(), "native fail") {
		t.Errorf("bridge error not propagated: %v", err)
	}

	// 非法 JSON
	_, _, err = wrapTokenProvider(&fakeTokenBridge{result: `not-json`})(t.Context(), "u1", 1)
	if err == nil {
		t.Error("expect decode error")
	}

	// 空 token
	_, _, err = wrapTokenProvider(&fakeTokenBridge{result: `{"token":""}`})(t.Context(), "u1", 1)
	if err == nil {
		t.Error("expect empty token error")
	}
}

func TestUpdateTokenPrefetch(t *testing.T) {
	d := NewDispatcher()
	d.UpdateToken("u1", 1, "tok-abc", 7200)

	fn := d.resolveTokenProvider(false)
	token, expire, err := fn(t.Context(), "u1", 1)
	if err != nil || token != "tok-abc" || expire != 7200 {
		t.Errorf("prefetch provider got (%q, %d, %v)", token, expire, err)
	}

	// 账号不匹配 → 错误
	_, _, err = fn(t.Context(), "u2", 1)
	if err == nil {
		t.Error("expect mismatch error")
	}

	// AdminSecret 模式 → nil（走 admin bootstrap）
	if fn := d.resolveTokenProvider(true); fn != nil {
		t.Error("admin mode should return nil provider")
	}

	// 桥优先于缓存
	d.SetTokenProvider(&fakeTokenBridge{result: `{"token":"tok123","expireTimeSeconds":60}`})
	token, _, err = d.resolveTokenProvider(true)(t.Context(), "u1", 1)
	if err != nil || token != "tok123" {
		t.Errorf("bridge priority: got (%q, %v)", token, err)
	}
}

// TestTokenProviderDynamicAfterInit 验证终端模式下 SetTokenProvider 在
// Init（resolveTokenProvider）之后调用仍能生效——provider 是动态读取的。
func TestTokenProviderDynamicAfterInit(t *testing.T) {
	d := NewDispatcher()

	// 模拟 Init 时已注入的 provider（无 bridge、无 admin → 动态 provider）
	fn := d.resolveTokenProvider(false)

	// 初始：无 bridge、无缓存 → 刷新失败
	_, _, err := fn(t.Context(), "u1", 1)
	if err == nil {
		t.Error("expect error when no provider and no cache")
	}

	// Init 之后才注册 bridge
	d.SetTokenProvider(&fakeTokenBridge{result: `{"token":"dynamic-tok","expireTimeSeconds":1800}`})

	// 同一个 fn（已注入 ClientConfig）现在应走 bridge
	token, expire, err := fn(t.Context(), "u1", 1)
	if err != nil {
		t.Fatalf("dynamic bridge call failed: %v", err)
	}
	if token != "dynamic-tok" || expire != 1800 {
		t.Errorf("got (%q, %d), want dynamic-tok/1800", token, expire)
	}

	// 切换为预取缓存：注销 bridge，UpdateToken 注入
	d.SetTokenProvider(nil)
	d.UpdateToken("u1", 1, "prefetch-tok", 900)
	token, _, err = fn(t.Context(), "u1", 1)
	if err != nil || token != "prefetch-tok" {
		t.Errorf("prefetch fallback got (%q, %v), want prefetch-tok", token, err)
	}
}

// === 登录态跟踪 ===

func TestLoggedInTracking(t *testing.T) {
	d := newTestDispatcher(t)
	if d.isLoggedIn() {
		t.Error("should not be logged in initially")
	}

	// 登录失败（服务器不可达）→ 仍未登录
	_, err := d.SyncCall("Login", `["u1", 1]`)
	if err == nil {
		t.Fatal("expect login failure (server unreachable)")
	}
	if d.isLoggedIn() {
		t.Error("failed login should not set loggedIn")
	}
}

func assertCode(t *testing.T, err error, want int32) {
	t.Helper()
	if err == nil {
		t.Fatalf("expect error code %d, got nil", want)
	}
	code, _ := NormalizeError(err)
	if code != want {
		t.Fatalf("error code = %d, want %d (err: %v)", code, want, err)
	}
}

func assertNotCode(t *testing.T, err error, notWant int32) {
	t.Helper()
	code, _ := NormalizeError(err)
	if code == notWant {
		t.Fatalf("error code should not be %d (err: %v)", notWant, err)
	}
}
