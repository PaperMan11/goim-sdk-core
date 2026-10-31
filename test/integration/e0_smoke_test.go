package integration

// E0 冒烟 —— 对接第一步，全部后续阶段的前置（docs/INTEGRATION_TEST_PLAN.md §3 E0）
// 覆盖：E0.1 admin token / E0.2 用户注册 / E0.5 Login 全链路（含 R1 选路回归）

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/PaperMan11/goim-sdk-core/pkg/client"
	"github.com/PaperMan11/goim-sdk-core/pkg/events"
	"github.com/PaperMan11/goim-sdk-core/pkg/logger"
	"github.com/PaperMan11/goim-sdk-core/pkg/transport"
)

// TestE0_AdminToken E0.1 服务端连通 + secret 正确性
func TestE0_AdminToken(t *testing.T) {
	requireEnv(t)

	registerUsers(t, uniqueUser("smoke")) // 内部完成 GetAdminToken + Register
}

// TestE0_RegisterUsers E0.2 用户注册 + 查询回读
func TestE0_RegisterUsers(t *testing.T) {
	requireEnv(t)

	users := []string{uniqueUser("u1"), uniqueUser("u2"), uniqueUser("u3")}
	registerUsers(t, users...)
	t.Logf("registered users: %v", users)
}

// TestE0_LoginFullFlow E0.5 Login 全链路：GetUserToken → WS Connect 即成功返回
// （数据同步移入后台，以 SyncStart → SyncFinish 收尾，见 E1.7 解耦契约）
// 同时回归 R1 修复：登录后 HTTP 请求按 (userID, platformID) 带上 token
func TestE0_LoginFullFlow(t *testing.T) {
	requireEnv(t)

	userID := uniqueUser("u1")
	registerUsers(t, userID)

	c, ec := newTestClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), loginTimeout)
	defer cancel()

	// Login：Token + WS 就绪即返回（不等同步）
	require.NoError(t, c.Login(ctx, userID, platformID))

	// 传输层状态与事件顺序：Connecting → Connect → LoginSuccess
	assert.Equal(t, transport.TransportStatusConnected, c.Status())
	ec.wait(t, events.EventConnect, eventTimeout)
	ec.wait(t, events.EventLoginSuccess, eventTimeout)

	// 解耦契约：登录返回时同步进行中，后台同步完成后收敛 Synced
	assert.Equal(t, client.SyncStateSyncing, c.SyncState())
	ec.wait(t, events.EventSyncStart, eventTimeout)
	ec.wait(t, events.EventSyncFinish, eventTimeout)
	assert.Equal(t, client.SyncStateSynced, c.SyncState())

	// R1 回归：已登录 client 发起需鉴权 HTTP 请求，token 应正确注入
	infos, err := c.User().GetUsersInfo(ctx, []string{userID})
	require.NoError(t, err, "R1 regression: authenticated HTTP after login")
	t.Logf("GetUsersInfo: %+v", infos)

	// Logout 断连 + 取消在途同步（状态复位 None）
	require.NoError(t, c.Logout(false))
	assert.Equal(t, transport.TransportStatusDisconnected, c.Status())
	assert.Equal(t, client.SyncStateNone, c.SyncState())
}

// TestE0_LoginFailedWrongAdmin E1.2 错误管理员凭证登录 → EventLoginFailed
//
// 登录失败的可靠触发：不在服务端管理员列表的 AdminUserID
// （GetAdminToken 首步即 requireUserIsAdmin 校验拒绝，失败发生在 WS 拨号之前）
func TestE0_LoginFailedWrongAdmin(t *testing.T) {
	requireEnv(t)

	c := client.NewClient(&client.ClientConfig{
		BaseURL:           envAPIURL,
		Endpoint:          envWSURL,
		DefaultPlatformID: platformID,
		Logging:           &logger.Config{Level: envLogLevel, Format: logger.FormatConsole},
		SessionDataPath:   filepath.Join(t.TempDir(), "sdk.db"),
		AdminSecret:       "no-secret-required",
		AdminUserID:       adminUserID,
		Logger:            testLogger(),
	})
	require.NotNil(t, c, "client init (check sqlite path)")
	t.Cleanup(func() { _ = c.Close() })

	ec := collectEvents(c)
	t.Cleanup(ec.close)

	ctx, cancel := context.WithTimeout(context.Background(), loginTimeout)
	defer cancel()

	err := c.Login(ctx, uniqueUser("nouser"), platformID)
	require.Error(t, err, "login with non-admin AdminUserID should fail")
	ec.wait(t, events.EventLoginFailed, eventTimeout)
	assert.Equal(t, transport.TransportStatusDisconnected, c.Status())
}
