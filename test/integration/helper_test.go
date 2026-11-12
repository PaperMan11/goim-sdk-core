//go:build !(js && wasm)

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
	"github.com/PaperMan11/goim-sdk-core/pkg/storage"
	"github.com/PaperMan11/goim-sdk-core/pkg/transport"
	"github.com/PaperMan11/goim-sdk-core/pkg/utils/eventbus"
)

const (
	platformID  = 1
	adminUserID = "imAdmin"

	loginTimeout  = 30 * time.Second // Login（Token + WS 就绪即返回，同步在后台）
	eventTimeout  = 10 * time.Second
	evWaitTimeout = 10 * time.Second

	// syncConvergeTimeout 离线窗口收敛预算：服务端发送链路异步持久化（1003 ack
	// 先于数据可查，见测试计划 R12），登录同步可能跑在提交之前，需重登重拉兜底
	syncConvergeTimeout = 90 * time.Second
)

// testLogger 集成测试 SDK 日志（console debug，便于排查对接问题）
func testLogger() logger.Logger {
	return logger.New(&logger.Config{Level: envLogLevel, Format: logger.FormatConsole})
}

// taggedLogger 调试埋点：给 SDK 日志加 it-<tag> 标签，多客户端日志可区分归属
// （如 it-A / it-B），定位推送链路问题时使用。SDK 侧能力见 logger.WithTag——
// zap 底层走原生 Named，console 输出中标签为级别后的独立名称列（JSON 为 "logger" 字段）
func taggedLogger(tag string) logger.Logger {
	return logger.WithTag(testLogger(), "it-"+tag)
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

	l := taggedLogger("admin")
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
// tag 可选（如 "A"/"B"）：wait 超时信息与事件到达日志带 [it-<tag>] 标签，
// 多客户端测试时日志可区分归属
type eventCollector struct {
	tag    string
	mu     sync.Mutex
	got    []events.Event
	ch     <-chan events.Event
	cancel eventbus.UnsubscribeFunc
}

// collectEvents 订阅全部事件（调试级收集，wait 按 type 过滤）
// tags 可选：传入 "A"/"B" 时事件到达逐条打点、超时信息带 [it-<tag>] 前缀
func collectEvents(c client.Client, tags ...string) *eventCollector {
	ch, cancel := c.SubscribeAll()
	tag := ""
	if len(tags) > 0 {
		tag = tags[0]
	}
	return &eventCollector{tag: tag, ch: ch, cancel: cancel}
}

// prefix 日志前缀（无 tag 时返回空串）
func (ec *eventCollector) prefix() string {
	if ec.tag == "" {
		return ""
	}
	return "[it-" + ec.tag + "] "
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
				t.Fatalf("%sevent channel closed while waiting for %v; got: %s",
					ec.prefix(), typ, eventTypes(ec.snapshot()))
			}
			ec.record(ev)
			if ec.tag != "" {
				t.Logf("[it-%s] event %v", ec.tag, ev.Type)
			}
			if ev.Type == typ {
				return ev
			}
		case <-deadline:
			t.Fatalf("%stimeout (%v) waiting event %v; got %d events: %s",
				ec.prefix(), timeout, typ, len(ec.snapshot()), eventTypes(ec.snapshot()))
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
// tags 可选：传入 "A"/"B" 等标签时 SDK 日志带 [it-<tag>] 前缀，便于区分多客户端
func newTestClient(t *testing.T, tags ...string) (client.Client, *eventCollector) {
	t.Helper()
	requireEnv(t)

	l := testLogger()
	if len(tags) > 0 && tags[0] != "" {
		l = taggedLogger(tags[0])
	}
	c := client.NewClient(&client.ClientConfig{
		BaseURL:           envAPIURL,
		Endpoint:          envWSURL,
		DefaultPlatformID: platformID,
		Logging:           &logger.Config{Level: envLogLevel, Format: logger.FormatConsole},
		SessionDataPath:   filepath.Join(t.TempDir(), "sdk.db"),
		// 受信环境模式：Login 自动 admin bootstrap（get_user_token 需 admin 身份）
		AdminSecret: envSecret,
		AdminUserID: adminUserID,
		Logger:      l,
	})
	require.NotNil(t, c, "client init (check sqlite path)")

	// 释放 SQLite 句柄（Windows 下不关闭会导致 TempDir 清理失败）
	t.Cleanup(func() { _ = c.Close() })

	ec := collectEvents(c, tags...)
	t.Cleanup(ec.close)
	return c, ec
}

// newTestClientWithStorage 同 newTestClient，但由测试侧先建 SQLite 并经
// ClientConfig.Storage 注入（SDK 复用同一连接，避免双连接文件锁），返回存储句柄
// 供用例直接断言本地落库内容（E2.6 历史落库 / E2.7 双路径落库 / E2.8 推送落库）。
// Client.Close() 经 io.Closer 释放句柄，Windows 下 TempDir 清理不受影响。
// tags 可选：传入 "A"/"B" 等标签时 SDK 日志带 [it-<tag>] 前缀。
func newTestClientWithStorage(t *testing.T, tags ...string) (client.Client, *eventCollector, storage.Storage) {
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
		Endpoint:          envWSURL,
		DefaultPlatformID: platformID,
		Logging:           &logger.Config{Level: envLogLevel, Format: logger.FormatConsole},
		Storage:           db,
		// 受信环境模式：Login 自动 admin bootstrap（get_user_token 需 admin 身份）
		AdminSecret: envSecret,
		AdminUserID: adminUserID,
		Logger:      l,
	})
	require.NotNil(t, c, "client init")

	t.Cleanup(func() { _ = c.Close() })

	ec := collectEvents(c, tags...)
	t.Cleanup(ec.close)
	return c, ec, db
}

// waitOfflineSyncConverged 等待离线窗口消息收敛到 owner 的本地库（不齐则重登重拉）。
// （服务端方案 A write-through 上线后 E2.7 已改为 ack 后首轮严格断言，不再使用本
// helper；保留供 E4/E6 离线场景复用——那些用例验证 SDK 同步行为，容忍轮询）
//
// 背景（R12）：服务端发送链路异步持久化，1003 ack 返回时消息可能尚未可查——
// 发送后立即重登，登录同步的 gap 拉取会跑在持久化提交之前而漏掉最新消息，
// 且 SDK 无法从 NewestSeq 感知滞后（服务端游标同样滞后）。
// 策略：每轮检查本地库 → 不齐则等一小段提交时间 → 登出重登重新触发 gap 拉取，
// 直到 want 里的 clientMsgID 全部落库或超时 Fatal。
// （首轮检查假定调用方已完成一次「重登 + EventSyncFinish」）
func waitOfflineSyncConverged(t *testing.T, cb client.Client, ecb *eventCollector, storB storage.Storage,
	ownerUserID, convID string, want map[string]bool) {
	t.Helper()

	deadline := time.Now().Add(syncConvergeTimeout)
	for round := 0; ; round++ {
		msgs, err := storB.ListMsgsByConversation(ownerUserID, convID)
		require.NoError(t, err, "list msgs while waiting offline sync converge")

		got := make(map[string]bool, len(msgs))
		for _, m := range msgs {
			got[m.ClientMsgID] = true
		}
		missing := make([]string, 0, len(want))
		for id := range want {
			if !got[id] {
				missing = append(missing, id)
			}
		}
		if len(missing) == 0 {
			if round > 0 {
				t.Logf("[it-sync] offline window for %s (conv=%s) converged after %d relogin round(s)",
					ownerUserID, convID, round)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("[it-sync] offline window for %s (conv=%s) not converged within %v: %d/%d msgs missing (%v)",
				ownerUserID, convID, syncConvergeTimeout, len(missing), len(want), missing)
		}

		// 给服务端异步持久化管道提交时间，再重登触发新一轮 gap 拉取
		time.Sleep(3 * time.Second)
		require.NoError(t, cb.Logout(false), "logout for resync round %d", round+1)
		lctx, lcancel := context.WithTimeout(context.Background(), loginTimeout)
		require.NoError(t, cb.Login(lctx, ownerUserID, platformID), "relogin for resync round %d", round+1)
		ecb.wait(t, events.EventLoginSuccess, eventTimeout)
		ecb.wait(t, events.EventSyncFinish, eventTimeout) // 本轮 gap 拉取完成
		lcancel()
	}
}
