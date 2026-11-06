package integration

// E5 异常与重连（docs/INTEGRATION_TEST_PLAN.md §3 E5）
//
// 断连模拟方式：本地 TCP 代理（wsProxy）——SDK 的 Endpoint 指向代理，代理原样转发
// 到真实网关。测试通过代理侧控制连接（killCurrent 掐断现连 = 服务端 kill/断网；
// setStall 黑洞新拨号 = 断网窗口），无需操作服务端组件即可确定性构造异常。
// 代理对 WS 帧与心跳完全透明（原始字节转发）。
//
// E5.7 服务端 5xx 用本地 httptest 存根验证 SDK HTTP 层重试（5xx 指数退避、
// 最终错误含 operationID），连接拒绝路径等价于 im-api 宕机。

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PaperMan11/goim-sdk-core/pkg/client"
	"github.com/PaperMan11/goim-sdk-core/pkg/events"
	"github.com/PaperMan11/goim-sdk-core/pkg/logger"
	"github.com/PaperMan11/goim-sdk-core/pkg/model"
	"github.com/PaperMan11/goim-sdk-core/pkg/storage"
	"github.com/PaperMan11/goim-sdk-core/pkg/transport"
)

const reconnectTimeout = 60 * time.Second // 重连退避 1s 起步翻倍，留足窗口

// —— wsProxy：本地 TCP 代理 ——

type wsProxy struct {
	ln       net.Listener
	upstream string // 真实网关 host:port
	logf     func(format string, args ...any)

	mu   sync.Mutex
	live map[net.Conn]net.Conn // client conn -> upstream conn
	held []net.Conn            // stall 模式下挂住的新连接
	wall bool                  // stall 开关
}

func startWSProxy(t *testing.T, upstream string) *wsProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err, "proxy listen")
	p := &wsProxy{ln: ln, upstream: upstream, live: map[net.Conn]net.Conn{}, logf: t.Logf}
	go p.acceptLoop()
	t.Cleanup(p.closeAll)
	return p
}

// endpoint SDK Endpoint（原始 TCP 转发，但路径需与网关路由一致：/ws）
func (p *wsProxy) endpoint() string { return "ws://" + p.ln.Addr().String() + "/ws" }

func (p *wsProxy) acceptLoop() {
	for {
		conn, err := p.ln.Accept()
		if err != nil {
			return
		}
		go p.handle(conn)
	}
}

func (p *wsProxy) handle(cli net.Conn) {
	p.mu.Lock()
	stall := p.wall
	if stall {
		p.held = append(p.held, cli)
	}
	p.mu.Unlock()
	if stall { // 黑洞：挂住连接不转发（模拟断网/网关无响应）
		return
	}
	up, err := net.Dial("tcp", p.upstream)
	if err != nil {
		p.logf("[wsProxy] upstream dial %s failed: %v", p.upstream, err)
		_ = cli.Close()
		return
	}
	p.mu.Lock()
	p.live[cli] = up
	p.mu.Unlock()
	go func() { _, _ = io.Copy(up, cli); p.teardown(cli) }()
	go func() { _, _ = io.Copy(cli, up); p.teardown(cli) }()
}

func (p *wsProxy) teardown(cli net.Conn) {
	p.mu.Lock()
	up := p.live[cli]
	delete(p.live, cli)
	p.mu.Unlock()
	_ = cli.Close()
	if up != nil {
		_ = up.Close()
	}
}

// killCurrent 掐断现有连接（两端同断，等价服务端 kill / 网络中断）
func (p *wsProxy) killCurrent() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for cli, up := range p.live {
		_ = cli.Close()
		_ = up.Close()
		delete(p.live, cli)
	}
}

// setStall 切换黑洞模式；解除时释放挂住的连接，让重连拨号走通
func (p *wsProxy) setStall(v bool) {
	p.mu.Lock()
	p.wall = v
	held := p.held
	p.held = nil
	p.mu.Unlock()
	if !v {
		for _, c := range held {
			_ = c.Close()
		}
	}
}

func (p *wsProxy) closeAll() {
	_ = p.ln.Close()
	p.mu.Lock()
	defer p.mu.Unlock()
	for cli, up := range p.live {
		_ = cli.Close()
		_ = up.Close()
	}
	for _, c := range p.held {
		_ = c.Close()
	}
}

// wsUpstreamAddr 从 envWSURL 提取真实网关 host:port（丢弃 scheme 与 /ws 路径）
func wsUpstreamAddr() string {
	u, err := url.Parse(envWSURL)
	if err != nil || u.Host == "" {
		panic("invalid envWSURL: " + envWSURL) // 测试基建常量，解析失败直接暴露
	}
	return u.Host
}

// —— 自定义 Endpoint / 平台的 Client 构造 ——

// newClientOn 同 newTestClientWithStorage，但 Endpoint / platformID 可指定
//（E5 断连模拟需指向本地代理；E5.3 多端互踢需 platformID=2）
func newClientOn(t *testing.T, endpoint string, plat int32, tags ...string) (client.Client, *eventCollector, storage.Storage) {
	t.Helper()
	requireEnv(t)

	l := testLogger()
	if len(tags) > 0 && tags[0] != "" {
		l = taggedLogger(tags[0])
	}
	db, err := storage.NewSQLiteStorage(filepath.Join(t.TempDir(), "sdk.db"), l)
	require.NoError(t, err, "open sqlite storage")

	c := client.NewClient(&client.ClientConfig{
		BaseURL:           envAPIURL,
		Endpoint:          endpoint,
		DefaultPlatformID: plat,
		Logging:           &logger.Config{Level: envLogLevel, Format: logger.FormatConsole},
		Storage:           db,
		AdminSecret:       envSecret,
		AdminUserID:       adminUserID,
		Logger:            l,
	})
	require.NotNil(t, c, "client init")

	t.Cleanup(func() { _ = c.Close() })
	ec := collectEvents(c, tags...)
	t.Cleanup(ec.close)
	return c, ec, db
}

// TestE5_ServerKillAutoReconnectResync E5.1：连接被掐断 → 自动重连 → 后台 resync
func TestE5_ServerKillAutoReconnectResync(t *testing.T) {
	proxy := startWSProxy(t, wsUpstreamAddr())
	ca, eca, _ := newClientOn(t, proxy.endpoint(), platformID, "A")
	u1, u2 := uniqueUser("e5a"), uniqueUser("e5b")
	registerUsers(t, u1, u2)
	loginAndSync(t, ca, eca, u1)

	proxy.killCurrent() // 服务端主动断连的等价刺激

	eca.wait(t, events.EventDisconnect, eventTimeout)
	eca.wait(t, events.EventReconnect, reconnectTimeout)
	eca.wait(t, events.EventSyncStart, eventTimeout)
	eca.wait(t, events.EventSyncFinish, eventTimeout)
	require.Equal(t, client.SyncStateSynced, ca.SyncState(), "resync should complete after reconnect")
	require.Equal(t, transport.TransportStatusConnected, ca.Status())
}

// TestE5_OfflineWindowBackfill E5.2：断连期间 B 发 5 条 → A 重连后 gap 补齐
func TestE5_OfflineWindowBackfill(t *testing.T) {
	proxy := startWSProxy(t, wsUpstreamAddr())
	ca, eca, storA := newClientOn(t, proxy.endpoint(), platformID, "A")
	cb, ecb, _ := newClientOn(t, envWSURL, platformID, "B")
	u1, u2 := uniqueUser("e5a"), uniqueUser("e5b")
	registerUsers(t, u1, u2)
	loginAndSync(t, ca, eca, u1)
	loginAndSync(t, cb, ecb, u2)

	// 断 A 连接并黑洞重连拨号，构造确定性的离线窗口
	proxy.setStall(true)
	proxy.killCurrent()
	eca.wait(t, events.EventDisconnect, eventTimeout)

	// 断连期间 B 发 5 条（B 在线，服务端为离线的 A 排队）
	ctx := context.Background()
	want := make(map[string]bool, 5)
	for i := 1; i <= 5; i++ {
		m := newTextMsg(u2, u1, "", model.SessionTypeC2C, "e5-2 offline window")
		resp, err := cb.Msg().SendMsg(ctx, m)
		require.NoError(t, err, "send msg %d during A offline", i)
		want[resp.ClientMsgID] = true
	}

	// 网络恢复 → A 自动重连 → resync gap 补齐
	proxy.setStall(false)
	eca.wait(t, events.EventReconnect, reconnectTimeout)
	eca.wait(t, events.EventSyncFinish, eventTimeout)

	convID := waitConvID(t, ca, u1, u2)
	msgs, err := storA.ListMsgsByConversation(u1, convID)
	require.NoError(t, err, "list A msgs after resync")
	got := make(map[string]bool, len(msgs))
	for _, m := range msgs {
		got[m.ClientMsgID] = true
	}
	for id := range want {
		require.True(t, got[id], "offline window msg %s should be backfilled into A local store", id)
	}
}

// TestE5_MultiDeviceKick E5.3：多端登录与踢下线（2002 → ForceLogout）
//
// 部署服务端登录策略为 allow_multi（每用户 10 连接、每平台 3 连接）：
// 跨平台登录不互踢（多端共存）；同平台连接超过上限（3）时服务端踢掉最旧连接
//（conn_manager.go checkAllowMultiLoginAsync）。此处用第 4 个同平台连接触发
// 2002 踢下线，验证 SDK 的被踢路径：ForceLogout 事件 + 断连 + 同步状态复位 +
// 不自动重连。
func TestE5_MultiDeviceKick(t *testing.T) {
	const perPlatformLimit = 3 // auth.yaml MaxConnPerUserPerPlatform

	u1 := uniqueUser("e5k")
	registerUsers(t, u1)

	// 4 个同平台客户端依次登录，第 4 个触发服务端踢最旧连接
	users := make([]client.Client, 0, perPlatformLimit+1)
	collectors := make([]*eventCollector, 0, perPlatformLimit+1)
	for i := 0; i < perPlatformLimit+1; i++ {
		tag := fmt.Sprintf("A%d", i+1)
		c, ec, _ := newClientOn(t, envWSURL, platformID, tag)
		users = append(users, c)
		collectors = append(collectors, ec)
		loginAndSync(t, c, ec, u1)
	}

	// 最旧连接（第 1 个）被踢：2002 推送 → ForceLogout + 断连
	eca := collectors[0]
	eca.wait(t, events.EventForceLogout, eventTimeout)
	eca.wait(t, events.EventDisconnect, eventTimeout)
	require.Equal(t, client.SyncStateNone, users[0].SyncState(), "kicked client sync state should reset to None")

	// 被踢端不应自动重连（token 未失效，但 SDK 侧应视为主动登出而非断网）
	time.Sleep(6 * time.Second)
	for _, ev := range eca.snapshot() {
		require.NotEqual(t, events.EventReconnect, ev.Type, "kicked client must not auto-reconnect")
	}
	require.Equal(t, transport.TransportStatusDisconnected, users[0].Status())

	// 其余连接不受影响
	for i := 1; i <= perPlatformLimit; i++ {
		require.Equal(t, transport.TransportStatusConnected, users[i].Status(), "client A%d should stay online", i+1)
		require.Equal(t, client.SyncStateSynced, users[i].SyncState(), "client A%d sync state intact", i+1)
	}
}

// TestE5_HeartbeatKeepAlive E5.4：静置 3 分钟无操作，30s ping 维持连接不断
func TestE5_HeartbeatKeepAlive(t *testing.T) {
	proxy := startWSProxy(t, wsUpstreamAddr())
	ca, eca, _ := newClientOn(t, proxy.endpoint(), platformID, "A")
	u1 := uniqueUser("e5h")
	registerUsers(t, u1)
	loginAndSync(t, ca, eca, u1)

	// 静置 3 分钟：轮询状态，任何掉线立即失败（快速暴露心跳缺陷）
	deadline := time.Now().Add(3 * time.Minute)
	for time.Now().Before(deadline) {
		require.Equal(t, transport.TransportStatusConnected, ca.Status(),
			"connection dropped during 3min idle (heartbeat broken)")
		time.Sleep(5 * time.Second)
	}

	// 空闲窗口内无断连事件（drain 收集器通道检查）
	for {
		select {
		case ev := <-eca.ch:
			eca.record(ev)
			require.NotEqual(t, events.EventDisconnect, ev.Type, "unexpected disconnect during idle")
			require.NotEqual(t, events.EventReconnect, ev.Type, "unexpected reconnect during idle")
			continue
		default:
		}
		break
	}
	require.Equal(t, transport.TransportStatusConnected, ca.Status())
}

// TestE5_NetworkBreakRecovery E5.5：断网 30s → 恢复，状态流转与 goroutine 无泄漏
func TestE5_NetworkBreakRecovery(t *testing.T) {
	proxy := startWSProxy(t, wsUpstreamAddr())
	ca, eca, _ := newClientOn(t, proxy.endpoint(), platformID, "A")
	u1 := uniqueUser("e5n")
	registerUsers(t, u1)
	loginAndSync(t, ca, eca, u1)
	baseGoroutines := runtime.NumGoroutine()

	// 断网：掐断现连 + 黑洞新拨号，持续 30s
	proxy.setStall(true)
	proxy.killCurrent()
	eca.wait(t, events.EventDisconnect, eventTimeout)
	time.Sleep(30 * time.Second)
	require.NotEqual(t, transport.TransportStatusConnected, ca.Status(),
		"status should not be Connected during network break")

	// 网络恢复：释放黑洞 → 重连成功 → resync
	proxy.setStall(false)
	eca.wait(t, events.EventReconnect, reconnectTimeout)
	eca.wait(t, events.EventSyncFinish, eventTimeout)
	require.Equal(t, transport.TransportStatusConnected, ca.Status())

	// goroutine 无持续泄漏（允许少量波动，收敛窗口内回落）
	require.Eventually(t, func() bool {
		return runtime.NumGoroutine() <= baseGoroutines+15
	}, 30*time.Second, time.Second, "goroutine count should settle after recovery (base=%d, now=%d)",
		baseGoroutines, runtime.NumGoroutine())
}

// TestE5_SendDuringReconnectWindow E5.6：重连窗口内 SendMsg 立即报 notConnected，不挂死
func TestE5_SendDuringReconnectWindow(t *testing.T) {
	proxy := startWSProxy(t, wsUpstreamAddr())
	ca, eca, _ := newClientOn(t, proxy.endpoint(), platformID, "A")
	cb, ecb, _ := newClientOn(t, envWSURL, platformID, "B")
	u1, u2 := uniqueUser("e5a"), uniqueUser("e5b")
	registerUsers(t, u1, u2)
	loginAndSync(t, ca, eca, u1)
	loginAndSync(t, cb, ecb, u2)

	proxy.setStall(true)
	proxy.killCurrent()
	eca.wait(t, events.EventDisconnect, eventTimeout)

	// 重连窗口内发送：5s ctx 兜底——若 SDK 挂死则表现为 DeadlineExceeded（判失败）
	sctx, scancel := context.WithTimeout(context.Background(), 5*time.Second)
	m := newTextMsg(u1, u2, "", model.SessionTypeC2C, "e5-6 during reconnect")
	_, err := ca.Msg().SendMsg(sctx, m)
	scancel()
	require.Error(t, err, "SendMsg during reconnect window should fail")
	require.NotErrorIs(t, err, context.DeadlineExceeded, "SendMsg hung until ctx deadline (not woken up)")
	require.Contains(t, err.Error(), "not connected", "expect notConnected class error, got: %v", err)

	// 恢复后发送恢复正常
	proxy.setStall(false)
	eca.wait(t, events.EventReconnect, reconnectTimeout)
	eca.wait(t, events.EventSyncFinish, eventTimeout)
	m2 := newTextMsg(u1, u2, "", model.SessionTypeC2C, "e5-6 after recovery")
	resp, err := ca.Msg().SendMsg(context.Background(), m2)
	require.NoError(t, err, "SendMsg after recovery")
	require.NotEmpty(t, resp.ServerMsgID, "send after recovery should ack")
}

// TestE5_HTTPRetryOn5xx E5.7：HTTP 层 5xx 指数退避重试 / 连接拒绝重试耗尽后错误含 operationID
func TestE5_HTTPRetryOn5xx(t *testing.T) {
	ctx := context.Background()

	// ① 5xx 两次后成功：重试 3 次内恢复（服务端瞬时故障）
	var attempts int32
	okSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if atomic.AddInt32(&attempts, 1) < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte(`{"code":0,"msg":"","data":{"token":"stub-token","expireTimeSeconds":3600}}`))
	}))
	t.Cleanup(okSrv.Close)
	hc := transport.NewHTTPClient(okSrv.URL, transport.NewTokenStore(nil), testLogger())
	var tokenResp struct {
		Token             string `json:"token"`
		ExpireTimeSeconds int64  `json:"expireTimeSeconds"`
	}
	require.NoError(t, hc.Post(ctx, "/auth/get_admin_token", map[string]string{"secret": "x", "adminUserID": "a"}, &tokenResp),
		"5xx twice then 200 should succeed via retry")
	require.Equal(t, int32(3), atomic.LoadInt32(&attempts), "should take 3 attempts")
	require.Equal(t, "stub-token", tokenResp.Token)

	// ② 持续 5xx：耗尽 3 次尝试后失败，错误含 operationID
	var failAttempts int32
	failSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&failAttempts, 1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(failSrv.Close)
	hcFail := transport.NewHTTPClient(failSrv.URL, transport.NewTokenStore(nil), testLogger())
	err := hcFail.Post(ctx, "/auth/get_admin_token", map[string]string{"secret": "x", "adminUserID": "a"}, &tokenResp)
	require.Error(t, err, "persistent 5xx should fail after retries")
	require.Equal(t, int32(3), atomic.LoadInt32(&failAttempts), "exactly 3 attempts (maxRetry)")
	require.Contains(t, err.Error(), "operationID=", "final error should contain operationID: %v", err)

	// ③ 连接拒绝（等价 im-api 宕机）：网络层错误重试耗尽，错误含 operationID
	hcDown := transport.NewHTTPClient("http://127.0.0.1:1/api/v1", transport.NewTokenStore(nil), testLogger())
	err = hcDown.Post(ctx, "/auth/get_admin_token", map[string]string{"secret": "x", "adminUserID": "a"}, &tokenResp)
	require.Error(t, err, "connection refused should fail after retries")
	require.Contains(t, err.Error(), "after 3 attempts", "final error should mention attempts: %v", err)
	require.Contains(t, err.Error(), "operationID=", "final error should contain operationID: %v", err)

	// ④ 4xx 不重试：404 立即失败（R11 防护保持）
	var notFoundAttempts int32
	nfSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&notFoundAttempts, 1)
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(nfSrv.Close)
	hcNF := transport.NewHTTPClient(nfSrv.URL, transport.NewTokenStore(nil), testLogger())
	err = hcNF.Post(ctx, "/auth/get_admin_token", map[string]string{"secret": "x", "adminUserID": "a"}, &tokenResp)
	require.Error(t, err)
	require.Equal(t, int32(1), atomic.LoadInt32(&notFoundAttempts), "4xx should not retry")
}
