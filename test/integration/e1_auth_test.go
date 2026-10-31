package integration

// E1 认证与连接（docs/INTEGRATION_TEST_PLAN.md §3 E1）
// 覆盖：E1.1 错误 secret / E1.3 重复 Login / E1.4 未登录直连 / E1.5 token 过期 /
// E1.6 Logout / E1.7 登录与同步解耦契约
//
// 实测背景（E0 阶段）：
//   - R8：get_user_token 不校验用户注册态，"未注册登录失败"不成立（用例已移至错误凭证场景）
//   - R9：GetAdminToken 已恢复 secret 校验（常量时间比较），E1.1 可用

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/PaperMan11/goim-sdk-core/pkg/client"
	"github.com/PaperMan11/goim-sdk-core/pkg/events"
	"github.com/PaperMan11/goim-sdk-core/pkg/manager"
	"github.com/PaperMan11/goim-sdk-core/pkg/transport"
)

// TestE1_AdminTokenWrongSecret E1.1 错误 secret → GetAdminToken 拒绝（R9 修复回归）
func TestE1_AdminTokenWrongSecret(t *testing.T) {
	requireEnv(t)

	l := testLogger()
	adminTokens := transport.NewTokenStore(nil)
	adminHTTP := transport.NewHTTPClient(envAPIURL, adminTokens, l)
	authMgr := manager.NewAuthMgr(adminHTTP, adminTokens, nil, l)

	_, err := authMgr.GetAdminToken(context.Background(), "wrong-secret-"+uniqueUser("s"), adminUserID)
	require.Error(t, err, "get admin token with wrong secret should fail (R9 regression)")
	t.Logf("wrong secret rejected: %v", err)
}

// TestE1_RepeatedLogin E1.3 已登录状态下重复 Login → 状态一致、接口可用、游标不脏
func TestE1_RepeatedLogin(t *testing.T) {
	requireEnv(t)

	userID := uniqueUser("u1")
	registerUsers(t, userID)

	c, ec := newTestClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), loginTimeout)
	defer cancel()

	require.NoError(t, c.Login(ctx, userID, platformID))
	ec.wait(t, events.EventLoginSuccess, eventTimeout)

	// 重复 Login（同账号同平台）
	require.NoError(t, c.Login(ctx, userID, platformID), "repeated login should not fail")
	assert.Equal(t, transport.TransportStatusConnected, c.Status())

	// E1.3 解耦语义：重复登录取消旧轮后台同步、重新拉起，最终收敛 Synced
	ec.wait(t, events.EventSyncFinish, eventTimeout)
	assert.Equal(t, client.SyncStateSynced, c.SyncState())

	// 登录态可用：鉴权 HTTP 正常返回（游标/存储未被二次登录破坏的间接验证）
	infos, err := c.User().GetUsersInfo(ctx, []string{userID})
	require.NoError(t, err, "authenticated HTTP after repeated login")
	require.NotEmpty(t, infos)
	assert.Equal(t, userID, infos[0].UserID)
}

// TestE1_ConnectWithoutLogin E1.4 未登录直接 Connect（DefaultPlatformID 兜底，token 为空）
//
// 按 §3 E1.4 约定：服务端明确接受或拒绝都算有效结论，记录实际行为即可。
func TestE1_ConnectWithoutLogin(t *testing.T) {
	requireEnv(t)

	c, ec := newTestClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err := c.Connect(ctx)
	if err != nil {
		// 服务端拒绝：网关验 token 失败，连接建立失败 → SDK 上报错误
		t.Logf("E1.4 conclusion: server REJECTS unauthenticated WS connect, err=%v", err)
		assert.NotEqual(t, transport.TransportStatusConnected, c.Status())
		return
	}
	// 服务端接受：说明网关不强制 token（记录结论，业务层必须自行保证先登录）
	t.Logf("E1.4 conclusion: server ACCEPTS unauthenticated WS connect (status=%v)", c.Status())
	assert.Equal(t, transport.TransportStatusConnected, c.Status())
	ec.wait(t, events.EventConnect, eventTimeout)
	_ = c.Disconnect()
}

// TestE1_TokenExpiredRefresh E1.5 token 过期触发刷新/重试（需服务端配合短有效期）
//
// 前置：服务端把 token 有效期调到超短（如 60s），并设置环境变量 GOIM_SHORT_TTL=1。
// 未配置时 Skip（正常有效期无法在测试窗口内自然过期）。
func TestE1_TokenExpiredRefresh(t *testing.T) {
	requireEnv(t)
	if envShortTTL != "1" {
		t.Skip("requires server-side short token TTL; set GOIM_SHORT_TTL=1 after reconfiguring server")
	}

	userID := uniqueUser("u1")
	registerUsers(t, userID)

	c, _ := newTestClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	require.NoError(t, c.Login(ctx, userID, platformID))

	// 等待 token 自然过期（60s TTL + 余量），然后发起鉴权 HTTP 请求
	// 预期：SDK 触发 Refresh（AdminSecret 模式重新 bootstrap）→ 请求最终成功
	time.Sleep(90 * time.Second)
	infos, err := c.User().GetUsersInfo(ctx, []string{userID})
	require.NoError(t, err, "request after token expiry should succeed via refresh path (R2 regression)")
	require.NotEmpty(t, infos)
}

// TestE1_LogoutKeepAndClear E1.6 Logout(false) 保留数据可重登 / Logout(true) 清数据可重登
func TestE1_LogoutKeepAndClear(t *testing.T) {
	requireEnv(t)

	userID := uniqueUser("u1")
	registerUsers(t, userID)

	c, ec := newTestClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), loginTimeout)
	defer cancel()

	require.NoError(t, c.Login(ctx, userID, platformID))
	ec.wait(t, events.EventLoginSuccess, eventTimeout)

	// Logout(false)：断连 + 保留本地数据 + 取消在途后台同步（E1.6 解耦语义：状态复位 None）
	// 结论（E1.6 实测）：主动 Disconnect 不发布 EventDisconnect（transport 仅对被动断开
	// 发事件，见 Client 接口注释）——登出是宿主主动调用的，用返回值 + Status() 断言即可
	require.NoError(t, c.Logout(false))
	assert.Equal(t, transport.TransportStatusDisconnected, c.Status())
	assert.Equal(t, client.SyncStateNone, c.SyncState())

	// 重新登录（数据保留，走增量同步）；等后台同步完成再清数据
	require.NoError(t, c.Login(ctx, userID, platformID))
	ec.wait(t, events.EventLoginSuccess, eventTimeout)
	ec.wait(t, events.EventSyncFinish, eventTimeout)

	// Logout(true)：断连 + 清除本地数据（下次登录走 full 同步，E6.3 深入验证）
	require.NoError(t, c.Logout(true))
	assert.Equal(t, transport.TransportStatusDisconnected, c.Status())
}

// TestE1_LoginSyncDecoupling E1.7 登录与同步解耦契约：Login 立即返回（不等数据就绪），
// 后台同步以 EventSyncStart → EventSyncFinish 事件序列收尾，SyncState 流转 Syncing → Synced
func TestE1_LoginSyncDecoupling(t *testing.T) {
	requireEnv(t)

	userID := uniqueUser("u1")
	registerUsers(t, userID)

	c, ec := newTestClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), loginTimeout)
	defer cancel()

	require.NoError(t, c.Login(ctx, userID, platformID))

	// 登录返回时刻：连接已就绪、同步在后台进行中（未阻塞等待数据）
	assert.Equal(t, transport.TransportStatusConnected, c.Status())
	assert.Equal(t, client.SyncStateSyncing, c.SyncState())

	// 后台同步事件序列与状态收敛
	ec.wait(t, events.EventSyncStart, eventTimeout)
	ec.wait(t, events.EventSyncFinish, eventTimeout)
	assert.Equal(t, client.SyncStateSynced, c.SyncState())
}
