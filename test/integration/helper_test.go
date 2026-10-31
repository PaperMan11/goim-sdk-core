package integration

// 公共测试基建：SDK Client 构造、admin 注册、事件收集与等待
// （docs/INTEGRATION_TEST_PLAN.md §2.2 公共基建）

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PaperMan11/goim-sdk-core/pkg/client"
	"github.com/PaperMan11/goim-sdk-core/pkg/events"
	"github.com/PaperMan11/goim-sdk-core/pkg/logger"
	"github.com/PaperMan11/goim-sdk-core/pkg/manager"
	"github.com/PaperMan11/goim-sdk-core/pkg/model"
	"github.com/PaperMan11/goim-sdk-core/pkg/transport"
	"github.com/PaperMan11/goim-sdk-core/pkg/utils/eventbus"
)

const (
	platformID  = 1
	adminUserID = "imAdmin"

	loginTimeout  = 30 * time.Second // Login（Token + WS 就绪即返回，同步在后台）
	eventTimeout  = 20 * time.Second
	evWaitTimeout = 10 * time.Second
)

// testLogger 集成测试 SDK 日志（console debug，便于排查对接问题）
func testLogger() logger.Logger {
	return logger.New(&logger.Config{Level: envLogLevel, Format: logger.FormatConsole})
}

// —— 账号 ——

var userSeq int64

// uniqueUser 带时间戳的账号，避免历史数据干扰
func uniqueUser(name string) string {
	n := atomic.AddInt64(&userSeq, 1)
	return fmt.Sprintf("it_%s_%s_%d", time.Now().Format("0102_150405"), name, n)
}

// registerUsers 以 admin 身份注册测试用户（独立 admin HTTP client，
// admin token 按 (adminUserID, platformID) 存取
func registerUsers(t *testing.T, userIDs ...string) {
	t.Helper()
	requireEnv(t)

	l := testLogger()
	adminTokens := transport.NewTokenStore(nil)
	adminHTTP := transport.NewHTTPClient(envAPIURL, adminTokens, l)

	authMgr := manager.NewAuthMgr(adminHTTP, adminTokens, nil, l)
	tokenResp, err := authMgr.GetAdminToken(context.Background(), envSecret, adminUserID)
	require.NoError(t, err, "E0.1: get admin token")
	require.NotEmpty(t, tokenResp.Token)

	require.NoError(t, adminTokens.Set(adminUserID, platformID, tokenResp.Token, tokenResp.ExpireTimeSeconds))
	adminHTTP.SetCurrentUser(adminUserID, platformID)

	users := make([]*model.UserInfo, 0, len(userIDs))
	for _, id := range userIDs {
		users = append(users, &model.UserInfo{UserID: id, Nickname: id, CreateTime: time.Now().UnixMilli()})
	}
	require.NoError(t, manager.NewUserMgr(adminHTTP, nil, l).Register(context.Background(), users),
		"E0.2: register users %v", userIDs)
}

// —— 事件收集 ——

// eventCollector 事件收集器：先订阅后触发，避免事件丢失（方案风险 R6）
type eventCollector struct {
	mu     sync.Mutex
	got    []events.Event
	ch     <-chan events.Event
	cancel eventbus.UnsubscribeFunc
}

// collectEvents 订阅全部事件（调试级收集，wait 按 type 过滤）
func collectEvents(c client.Client) *eventCollector {
	ch, cancel := c.SubscribeAll()
	return &eventCollector{ch: ch, cancel: cancel}
}

func (ec *eventCollector) close() {
	if ec.cancel != nil {
		ec.cancel()
	}
}

func (ec *eventCollector) record(ev events.Event) {
	ec.mu.Lock()
	ec.got = append(ec.got, ev)
	ec.mu.Unlock()
}

func (ec *eventCollector) snapshot() []events.Event {
	ec.mu.Lock()
	defer ec.mu.Unlock()
	out := make([]events.Event, len(ec.got))
	copy(out, ec.got)
	return out
}

// wait 等待指定事件（超时 Fatal 并 dump 已收事件）
func (ec *eventCollector) wait(t *testing.T, typ events.EventType, timeout time.Duration) events.Event {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case ev, ok := <-ec.ch:
			if !ok {
				t.Fatalf("event channel closed while waiting for %v; got: %s", typ, eventTypes(ec.snapshot()))
			}
			ec.record(ev)
			if ev.Type == typ {
				return ev
			}
		case <-deadline:
			t.Fatalf("timeout (%v) waiting event %v; got %d events: %s",
				timeout, typ, len(ec.snapshot()), eventTypes(ec.snapshot()))
		}
	}
}

func eventTypes(evs []events.Event) string {
	types := make([]string, 0, len(evs))
	for _, ev := range evs {
		types = append(types, fmt.Sprintf("%v@%d", ev.Type, ev.Timestamp))
	}
	return fmt.Sprint(types)
}

// —— SDK Client ——

// newTestClient 构造接入真实服务端的 SDK Client（SQLite 存储 + debug 日志）
func newTestClient(t *testing.T) (client.Client, *eventCollector) {
	t.Helper()
	requireEnv(t)

	c := client.NewClient(&client.ClientConfig{
		BaseURL:           envAPIURL,
		Endpoint:          envWSURL,
		DefaultPlatformID: platformID,
		Logging:           &logger.Config{Level: envLogLevel, Format: logger.FormatConsole},
		SessionDataPath:   filepath.Join(t.TempDir(), "sdk.db"),
		// 受信环境模式：Login 自动 admin bootstrap（get_user_token 需 admin 身份）
		AdminSecret: envSecret,
		AdminUserID: adminUserID,
		Logger:      testLogger(),
	})
	require.NotNil(t, c, "client init (check sqlite path)")

	// 释放 SQLite 句柄（Windows 下不关闭会导致 TempDir 清理失败）
	t.Cleanup(func() { _ = c.Close() })

	ec := collectEvents(c)
	t.Cleanup(ec.close)
	return c, ec
}
