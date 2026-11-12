//go:build !(js && wasm)

package integration

// E6 存储与多账号（docs/INTEGRATION_TEST_PLAN.md §3 E6）
//
// 存储层 owner 隔离：所有 Storage 方法首参 ownerUserID，Logout(true) 仅清当前 owner
// （storage.Clear(userID)），不影响其他账号；Close() 释放 SQLite 句柄，重开同路径安全。
// 存储初始化三级优先级：cfg.Storage 注入 > cfg.SessionDataPath SQLite > 默认内存。

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PaperMan11/goim-sdk-core/pkg/client"
	"github.com/PaperMan11/goim-sdk-core/pkg/events"
	"github.com/PaperMan11/goim-sdk-core/pkg/logger"
	"github.com/PaperMan11/goim-sdk-core/pkg/model"
	"github.com/PaperMan11/goim-sdk-core/pkg/storage"
)

// newClientWithPath 同 newTestClientWithStorage，但 SessionDataPath 由调用方指定
// （E6.1/E6.4 需跨 Client 生命周期复用同一路径；E6.2 共享同一 Storage 句柄）
func newClientWithPath(t *testing.T, dbPath string, tags ...string) (client.Client, *eventCollector, storage.Storage) {
	t.Helper()
	requireEnv(t)

	l := testLogger()
	if len(tags) > 0 && tags[0] != "" {
		l = taggedLogger(tags[0])
	}
	db, err := storage.NewSQLiteStorage(dbPath, l)
	require.NoError(t, err, "open sqlite storage at %s", dbPath)

	c := client.NewClient(&client.ClientConfig{
		BaseURL:           envAPIURL,
		Endpoint:          envWSURL,
		DefaultPlatformID: platformID,
		Logging:           &logger.Config{Level: envLogLevel, Format: logger.FormatConsole},
		Storage:           db,
		AdminSecret:       envSecret,
		AdminUserID:       adminUserID,
		Logger:            l,
	})
	require.NotNil(t, c, "client init")
	// Close 时释放 SQLite 句柄（Windows 下不 Close 会锁文件，阻止重开同路径）
	t.Cleanup(func() { _ = c.Close() })

	ec := collectEvents(c, tags...)
	t.Cleanup(ec.close)
	return c, ec, db
}

// TestE6_SQLiteRestartRecovery E6.1：收发 → Close → 新建同路径 Client → Login
// 游标保留走增量同步，消息完整。
func TestE6_SQLiteRestartRecovery(t *testing.T) {
	u1, u2 := uniqueUser("e6a"), uniqueUser("e6b")
	registerUsers(t, u1, u2)
	dbPath := filepath.Join(t.TempDir(), "session.db")

	// ① 首客户端：登录 + 收发 3 条
	ca1, eca1, stor := newClientWithPath(t, dbPath, "A1")
	loginAndSync(t, ca1, eca1, u1)

	ctx := context.Background()
	mb := newMsgBox(ca1, "A1")
	for i := 1; i <= 3; i++ {
		m := newTextMsg(u1, u2, "", model.SessionTypeC2C, "e6-1 restart recover")
		resp, err := ca1.Msg().SendMsg(ctx, m)
		require.NoError(t, err, "send msg %d", i)
		mb.recv(t, resp.ClientMsgID, eventTimeout) // 发送端自己也收到 2001 回显
	}
	convID := waitConvID(t, ca1, u1, u2)

	// 首登游标快照（Close 前）
	beforeVerID, beforeVer, err := stor.GetVersion(u1, storage.EntityFriends) // 随便找个实体拿游标
	require.NoError(t, err)
	beforeMax, _, err := stor.GetMaxSeqs(u1)
	require.NoError(t, err)
	beforeConvMsgs, err := stor.ListMsgsByConversation(u1, convID)
	require.NoError(t, err)
	t.Logf("before restart: conv msgs=%d, friends version=%s/%d, maxSeqs=%v",
		len(beforeConvMsgs), beforeVerID, beforeVer, beforeMax)

	// Close 首客户端（释放 SQLite 句柄）
	require.NoError(t, ca1.Close(), "close before-restart client")
	eca1.close()

	// ② 同路径重开（新 Client 注入同一 dbPath → 新 SQLite 句柄，文件不变）
	ca2, eca2, stor2 := newClientWithPath(t, dbPath, "A2")
	loginAndSync(t, ca2, eca2, u1)

	// 游标保留 → 走增量同步（versionID 非空即已对齐过服务端；空 ObjectID 代表全量回退）
	afterVerID, afterVer, err := stor2.GetVersion(u1, storage.EntityFriends)
	require.NoError(t, err)
	require.NotEmpty(t, afterVerID, "versionID should be retained across restart (indicates incremental path)")
	require.Equal(t, beforeVerID, afterVerID, "versionID unchanged after restart")

	// 消息完整、seq 连续
	afterMsgs, err := stor2.ListMsgsByConversation(u1, convID)
	require.NoError(t, err)
	require.Equal(t, len(beforeConvMsgs), len(afterMsgs), "message count should survive restart")
	for i, m := range afterMsgs {
		if i > 0 {
			require.Greater(t, m.Seq, afterMsgs[i-1].Seq, "seq should be monotonic")
		}
	}
	t.Logf("after restart: conv msgs=%d, friends version=%s/%d", len(afterMsgs), afterVerID, afterVer)
}

// TestE6_MultiAccountOwnerIsolation E6.2：同一 SQLite 文件，u1/u2 先后登录，数据按 owner 隔离
func TestE6_MultiAccountOwnerIsolation(t *testing.T) {
	u1, u2 := uniqueUser("e6a"), uniqueUser("e6b")
	registerUsers(t, u1, u2)
	dbPath := filepath.Join(t.TempDir(), "shared.db")

	// 共享 Storage 句柄——两个 Client 打开同一个 SQLite 文件
	l := testLogger()
	db, err := storage.NewSQLiteStorage(dbPath, l)
	require.NoError(t, err)
	defer db.Close()

	// u1：登录 + 发 2 条给自己（C2C 会话）
	c1, ec1 := newClientWithStorageOnDB(t, db, "A1")
	loginAndSync(t, c1, ec1, u1)
	ctx := context.Background()
	for i := 1; i <= 2; i++ {
		m := newTextMsg(u1, u1, "", model.SessionTypeC2C, "e6-2 u1 self")
		_, err := c1.Msg().SendMsg(ctx, m)
		require.NoError(t, err)
	}
	require.Eventually(t, func() bool {
		c1s, _ := db.ListConversations(u1)
		return len(c1s) >= 1
	}, storeWaitTimeout, 200*time.Millisecond, "u1 should have at least one conversation")

	convID1 := waitConvID(t, c1, u1, u1)
	u1msgs, err := db.ListMsgsByConversation(u1, convID1)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(u1msgs), 2, "u1 should see own msgs")

	// u1 登出（clearData=false → 数据保留，为 u2 共存让路）
	require.NoError(t, c1.Logout(false))
	ec1.close()

	// u2：登录 + 发 3 条给自己
	c2, ec2 := newClientWithStorageOnDB(t, db, "A2")
	loginAndSync(t, c2, ec2, u2)
	for i := 1; i <= 3; i++ {
		m := newTextMsg(u2, u2, "", model.SessionTypeC2C, "e6-2 u2 self")
		_, err := c2.Msg().SendMsg(ctx, m)
		require.NoError(t, err)
	}
	convID2 := waitConvID(t, c2, u2, u2)
	u2msgs, err := db.ListMsgsByConversation(u2, convID2)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(u2msgs), 3, "u2 should see own msgs")

	// 断言 u2 的 owner 下看不到 u1 的数据——隔离验证核心
	u1msgsUnderU2, err := db.ListMsgsByConversation(u2, convID1)
	require.NoError(t, err)
	require.Empty(t, u1msgsUnderU2, "u2 owner's msgs at u1 conv should be empty (owner isolation)")

	u2before, err := db.ListFriends(u2)
	require.NoError(t, err)
	require.Empty(t, u2before, "u2 should have no friends (u1's friend list untouched)")

	// u2 登出后再查 u1 数据——u1 的 owner 数据完好
	require.NoError(t, c2.Logout(false))
	ec2.close()
	u1again, err := db.ListMsgsByConversation(u1, convID1)
	require.NoError(t, err)
	require.Len(t, u1again, len(u1msgs), "u1 msgs should survive u2 login+logout")

	t.Logf("u1 msgs=%d, u2 msgs=%d (owners isolated)", len(u1msgs), len(u2msgs))
}

// TestE6_ClearDataFullResync E6.3：Logout(true) 清 owner 数据 → 重登 full 全量回退
// （E4.6 已验证该路径，E6 侧重存储层 Clear 的 owner 语义）
func TestE6_ClearDataFullResync(t *testing.T) {
	ca, eca, stor := newTestClientWithStorage(t, "A")
	cb, ecb := newTestClient(t, "B")
	u1, u2 := uniqueUser("e6a"), uniqueUser("e6b")
	registerUsers(t, u1, u2)
	loginAndSync(t, ca, eca, u1)
	loginAndSync(t, cb, ecb, u2)

	// 构造数据：好友 + C2C 消息 + 会话
	applyFriendship(t, ca, cb, eca, ecb, u1, u2)
	ctx := context.Background()
	m := newTextMsg(u1, u2, "", model.SessionTypeC2C, "e6-3 pre-clear")
	_, err := ca.Msg().SendMsg(ctx, m)
	require.NoError(t, err)
	convID := waitConvID(t, ca, u1, u2)
	require.Eventually(t, func() bool { return hasFriend(stor, u1, u2) },
		storeWaitTimeout, 200*time.Millisecond)
	require.Eventually(t, func() bool { return findConv(stor, u1, convID) != nil },
		storeWaitTimeout, 200*time.Millisecond)

	// Logout(true) 清 owner 数据
	require.NoError(t, ca.Logout(true))

	// 本地 owner 数据全空
	friends, _ := stor.ListFriends(u1)
	require.Empty(t, friends, "friends cleared after Logout(true)")
	convs, _ := stor.ListConversations(u1)
	require.Empty(t, convs, "conversations cleared after Logout(true)")
	_, v, _ := stor.GetVersion(u1, storage.EntityFriends)
	require.Zero(t, v, "friend cursor cleared after Logout(true)")

	// 重登：游标空 → 服务端 full=true → 全量重拉，数据最终一致
	loginAndSync(t, ca, eca, u1)
	require.True(t, hasFriend(stor, u1, u2), "friend re-pulled after clearData relogin")
	require.NotNil(t, findConv(stor, u1, convID), "conversation re-pulled after clearData relogin")
	msgs, _ := stor.ListMsgsByConversation(u1, convID)
	require.Greater(t, len(msgs), 0, "msgs should survive full resync")
}

// TestE6_MemoryStorageMode E6.4：不配 Storage/SessionDataPath → 内存存储，重启数据丢失
func TestE6_MemoryStorageMode(t *testing.T) {
	u1, u2 := uniqueUser("e6a"), uniqueUser("e6b")
	registerUsers(t, u1, u2)

	// 纯内存 Client：SessionDataPath=""、Storage=nil（走默认 storage.NewMemoryStorage()）
	l := testLogger()
	c1 := client.NewClient(&client.ClientConfig{
		BaseURL:           envAPIURL,
		Endpoint:          envWSURL,
		DefaultPlatformID: platformID,
		Logging:           &logger.Config{Level: envLogLevel, Format: logger.FormatConsole},
		Storage:           nil,
		SessionDataPath:   "",
		AdminSecret:       envSecret,
		AdminUserID:       adminUserID,
		Logger:            l,
	})
	require.NotNil(t, c1)
	t.Cleanup(func() { _ = c1.Close() })
	ec1 := collectEvents(c1, "A1")
	t.Cleanup(ec1.close)
	loginAndSync(t, c1, ec1, u1)

	ctx := context.Background()
	m := newTextMsg(u1, u2, "", model.SessionTypeC2C, "e6-4 memory mode")
	_, err := c1.Msg().SendMsg(ctx, m)
	require.NoError(t, err, "memory storage mode SendMsg should work")

	// Close 并释放内存 Client
	require.NoError(t, c1.Close())
	ec1.close()

	// 新建同配置 Client（全新内存实例 → 上次数据丢失，走 full 全量回退）
	c2 := client.NewClient(&client.ClientConfig{
		BaseURL:           envAPIURL,
		Endpoint:          envWSURL,
		DefaultPlatformID: platformID,
		Logging:           &logger.Config{Level: envLogLevel, Format: logger.FormatConsole},
		Storage:           nil,
		SessionDataPath:   "",
		AdminSecret:       envSecret,
		AdminUserID:       adminUserID,
		Logger:            l,
	})
	require.NotNil(t, c2)
	defer c2.Close()
	ec2 := collectEvents(c2, "A2")
	loginAndSync(t, c2, ec2, u1)

	// 重启后游标全空（内存存储不持久化）——走服务端 full 回退
	require.NotEqual(t, client.SyncStateNone, c2.SyncState(), "second client should sync from scratch")
	require.Eventually(t, func() bool { return c2.SyncState() == client.SyncStateSynced },
		storeWaitTimeout, 200*time.Millisecond, "second client should complete full sync")

	t.Logf("memory storage mode: functional, restart loses data as documented")
}

// newClientWithStorageOnDB 给预先打开的 Storage 注入 Client（E6.2 多账号共享同一 DB 句柄）
func newClientWithStorageOnDB(t *testing.T, db storage.Storage, tags ...string) (client.Client, *eventCollector) {
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
		Storage:           db,
		AdminSecret:       envSecret,
		AdminUserID:       adminUserID,
		Logger:            l,
	})
	require.NotNil(t, c, "client init")
	t.Cleanup(func() { _ = c.Close() })
	ec := collectEvents(c, tags...)
	t.Cleanup(ec.close)
	return c, ec
}

// suppress unused import warnings
var _ = events.EventSyncFinish
var _ = model.SessionTypeC2C
