package integration

// E2 消息链路（docs/INTEGRATION_TEST_PLAN.md §3 E2，核心）
// 覆盖：E2.1 单聊收发 / E2.2 群聊收发 / E2.3 离线 SeqSync 补齐 / E2.4 NewestSeq /
// E2.5 PullMsgBySeq / E2.6 历史消息落库 / E2.7 在线推送与离线 sync 双路径持久化 /
// E2.8 推送消息实时落库且不推进游标

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/PaperMan11/goim-sdk-core/pkg/client"
	"github.com/PaperMan11/goim-sdk-core/pkg/events"
	"github.com/PaperMan11/goim-sdk-core/pkg/model"
	"github.com/PaperMan11/goim-sdk-core/pkg/utils/uuidx"
)

const contentTypeText = 101 // 文本消息（对齐服务端 constant.ContentTypeText）

// msgBox B 端消息接收通道（OnNewMessage 回调 → channel，先订阅后触发防 R6 丢失）
type msgBox struct {
	ch chan *model.MsgData

	mu      sync.Mutex
	pending []*model.MsgData // recv 等待期间乱序到达的非目标消息暂存区（不丢弃）
}

func newMsgBox(c client.Client, tags ...string) *msgBox {
	tag := "box"
	if len(tags) > 0 && tags[0] != "" {
		tag = tags[0]
	}
	mb := &msgBox{ch: make(chan *model.MsgData, 64)}
	c.OnNewMessage(func(msg *model.MsgData) {
		// 调试埋点：确认 SDK 推送事件确实触发了本回调（区分「服务端没推到 B」
		// 与「SDK 事件链路断了」两种故障）
		fmt.Printf("[it-box-%s] OnNewMessage: clientMsgID=%s seq=%d conv=%s\n",
			tag, msg.ClientMsgID, msg.Seq, msg.ConversationID)
		select {
		case mb.ch <- msg:
		default: // 通道满丢弃计数场景仅存在于极端压测，集成测试 buffer 足够
			fmt.Printf("[it-box-%s] WARN channel full, dropping clientMsgID=%s\n", tag, msg.ClientMsgID)
		}
	})
	return mb
}

// take 从暂存区取指定 clientMsgID 的消息（此前 recv 等待期间乱序到达的）
func (mb *msgBox) take(clientMsgID string) *model.MsgData {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	for i, m := range mb.pending {
		if m.ClientMsgID == clientMsgID {
			mb.pending = append(mb.pending[:i], mb.pending[i+1:]...)
			return m
		}
	}
	return nil
}

// stash 非目标消息入暂存区（推送可能乱序/多条并发到达，后续 recv 会按需取用）
func (mb *msgBox) stash(m *model.MsgData) {
	mb.mu.Lock()
	mb.pending = append(mb.pending, m)
	mb.mu.Unlock()
}

// recv 等待收到指定 clientMsgID 的消息（超时 Fatal）。
// 非目标消息进暂存区而非丢弃——推送到达顺序不保证（实测 seq 乱序到达），
// 直接丢弃会让后续 recv 永远等不到已被消费的消息。
func (mb *msgBox) recv(t *testing.T, clientMsgID string, timeout time.Duration) *model.MsgData {
	t.Helper()
	if m := mb.take(clientMsgID); m != nil {
		return m
	}
	deadline := time.After(timeout)
	for {
		select {
		case msg := <-mb.ch:
			if msg.ClientMsgID == clientMsgID {
				return msg
			}
			mb.stash(msg)
		case <-deadline:
			t.Fatalf("timeout (%v) waiting msg clientMsgID=%s", timeout, clientMsgID)
		}
	}
}

// newTextMsg 构造文本消息（ClientMsgID 由 SDK 侧生成——服务端不代填）
func newTextMsg(sendID, recvID, groupID string, sessionType int32, content string) *model.MsgData {
	return &model.MsgData{
		ClientMsgID:      uuidx.New(),
		SendID:           sendID,
		RecvID:           recvID,
		GroupID:          groupID,
		SenderPlatformID: platformID,
		SessionType:      sessionType,
		MsgFrom:          model.MsgFromUser,
		ContentType:      contentTypeText,
		Content:          content,
		CreateTime:       time.Now().UnixMilli(),
		SendTime:         time.Now().UnixMilli(),
	}
}

// loginClient 注册 + 登录的常用组合，返回事件收集器
func loginClient(t *testing.T, userID string) (client.Client, *eventCollector) {
	t.Helper()
	registerUsers(t, userID)
	c, ec := newTestClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), loginTimeout)
	t.Cleanup(cancel)
	require.NoError(t, c.Login(ctx, userID, platformID), "login %s", userID)
	ec.wait(t, events.EventLoginSuccess, eventTimeout)
	return c, ec
}

var _ = loginClient // 备用组合 fixture（E3+ 使用）

// TestE2_SingleChatSend E2.1 单聊收发（WS 1003 → 2001 推送），同时覆盖 E2.6 服务端持久化断言
func TestE2_SingleChatSend(t *testing.T) {
	requireEnv(t)

	u1, u2 := uniqueUser("a"), uniqueUser("b")
	registerUsers(t, u1, u2)

	ca, eca := newTestClient(t, "A")
	cb, ecb := newTestClient(t, "B")
	ctx, cancel := context.WithTimeout(context.Background(), 2*loginTimeout)
	defer cancel()

	require.NoError(t, ca.Login(ctx, u1, platformID))
	eca.wait(t, events.EventLoginSuccess, eventTimeout)
	require.NoError(t, cb.Login(ctx, u2, platformID))
	ecb.wait(t, events.EventLoginSuccess, eventTimeout)

	// B 先订阅（防 R6）
	mb := newMsgBox(cb, "B")

	content := "hello-from-a-" + time.Now().Format("150405")
	msg := newTextMsg(u1, u2, "", model.SessionTypeC2C, content)

	// A 发送（WS 1003 同步等响应）
	resp, err := ca.Msg().SendMsg(ctx, msg)
	require.NoError(t, err, "SendMsg 1003")
	require.NotNil(t, resp)
	// E2.6（服务端持久化维度）：ServerMsgID / ClientMsgID 回填
	assert.NotEmpty(t, resp.ClientMsgID, "clientMsgID echoed")
	assert.NotEmpty(t, resp.ServerMsgID, "serverMsgID assigned")
	assert.Equal(t, msg.ClientMsgID, resp.ClientMsgID)
	t.Logf("[it-A] sent: clientMsgID=%s serverMsgID=%s sendTime=%d", resp.ClientMsgID, resp.ServerMsgID, resp.SendTime)

	// B 实时收到（WS 2001 推送），payload 字段逐项比对
	got := mb.recv(t, msg.ClientMsgID, eventTimeout)
	assert.Equal(t, u1, got.SendID)
	assert.Equal(t, u2, got.RecvID)
	assert.Equal(t, content, got.Content)
	assert.Equal(t, int32(model.SessionTypeC2C), got.SessionType)
	// assert.NotZero(t, got.Seq, "offline seq assigned by server")
}

// TestE2_GroupChatSend E2.2 群聊收发（A 建群 → 群发 → B 在线实时收到）
func TestE2_GroupChatSend(t *testing.T) {
	requireEnv(t)

	u1, u2 := uniqueUser("a"), uniqueUser("b")
	registerUsers(t, u1, u2)

	ca, eca := newTestClient(t, "A")
	cb, ecb := newTestClient(t, "B")
	ctx, cancel := context.WithTimeout(context.Background(), 2*loginTimeout)
	defer cancel()

	require.NoError(t, ca.Login(ctx, u1, platformID))
	eca.wait(t, events.EventLoginSuccess, eventTimeout)
	require.NoError(t, cb.Login(ctx, u2, platformID))
	ecb.wait(t, events.EventLoginSuccess, eventTimeout)

	mb := newMsgBox(cb, "B")

	// A 建群（owner=A，成员 B）
	group, err := ca.Group().CreateGroup(ctx,
		&model.GroupInfo{GroupName: "e2-group-" + time.Now().Format("150405")},
		[]string{u2}, nil, u1, false)
	require.NoError(t, err, "create group")
	require.NotEmpty(t, group.GroupID)
	t.Logf("[it-A] group created: %s", group.GroupID)

	// A 群发
	content := "group-hello-" + time.Now().Format("150405")
	msg := newTextMsg(u1, group.GroupID, group.GroupID, model.SessionTypeGroup, content)
	resp, err := ca.Msg().SendMsg(ctx, msg)
	require.NoError(t, err, "group SendMsg")
	require.NotEmpty(t, resp.ServerMsgID)

	// B 实时收到，群字段正确
	got := mb.recv(t, msg.ClientMsgID, eventTimeout)
	assert.Equal(t, group.GroupID, got.RecvID, "group msg recvID should be groupID")
	assert.Equal(t, int32(model.SessionTypeGroup), got.SessionType)
	assert.Equal(t, content, got.Content)
}

// TestE2_OfflineSeqSync E2.3 离线消息 SeqSync 补齐 + E2.4 NewestSeq + E2.5 PullMsgBySeq
//
// 流程：A/B 在线预热 1 条（拿 conversationID 与基准 seq）→ B 登出 → A 发 10 条 →
// B 重登 → 等 EventSyncFinish（后台 SeqSync 补齐完成）→ NewestSeq 前进 10（E2.4）→
// PullMsgBySeq 拉回 10 条比对 clientMsgID（E2.5）。
func TestE2_OfflineSeqSync(t *testing.T) {
	requireEnv(t)

	u1, u2 := uniqueUser("a"), uniqueUser("b")
	registerUsers(t, u1, u2)

	ca, eca := newTestClient(t, "A")
	cb, ecb := newTestClient(t, "B")
	ctx, cancel := context.WithTimeout(context.Background(), 3*loginTimeout)
	defer cancel()

	require.NoError(t, ca.Login(ctx, u1, platformID))
	eca.wait(t, events.EventLoginSuccess, eventTimeout)
	require.NoError(t, cb.Login(ctx, u2, platformID))
	ecb.wait(t, events.EventLoginSuccess, eventTimeout)

	// 预热：A→B 1 条，产生会话
	warm := newTextMsg(u1, u2, "", model.SessionTypeC2C, "warmup")
	_, err := ca.Msg().SendMsg(ctx, warm)
	require.NoError(t, err, "warmup send")

	// B 拿到会话与基准 seq（E2.4 基线）
	convID := waitConvID(t, cb, u2, u1)
	baseSeqs, _, err := cb.Msg().NewestSeq(ctx)
	require.NoError(t, err, "NewestSeq baseline")
	baseSeq := baseSeqs[convID]
	t.Logf("[it-B] baseline: conv=%s baseSeq=%d", convID, baseSeq)

	// B 登出（保留数据）
	require.NoError(t, cb.Logout(false))

	// A 离线窗口发 10 条
	sent := make([]string, 0, 10)
	for i := 0; i < 10; i++ {
		m := newTextMsg(u1, u2, "", model.SessionTypeC2C, "offline-"+time.Now().Format("150405")+"-"+uuidx.New())
		resp, err := ca.Msg().SendMsg(ctx, m)
		require.NoError(t, err, "offline window send %d", i)
		sent = append(sent, resp.ClientMsgID)
	}

	// B 重登 → SeqSync 在后台执行（解耦契约），等 EventSyncFinish 补齐完成再断言
	require.NoError(t, cb.Login(ctx, u2, platformID))
	ecb.wait(t, events.EventLoginSuccess, eventTimeout)
	ecb.wait(t, events.EventSyncFinish, eventTimeout)

	// E2.4：NewestSeq 前进 10
	afterSeqs, _, err := cb.Msg().NewestSeq(ctx)
	require.NoError(t, err, "NewestSeq after resync")
	assert.Equal(t, baseSeq+10, afterSeqs[convID], "maxSeq should advance by exactly 10")

	// E2.5：PullMsgBySeq 拉回离线窗口，clientMsgID 集合一致
	res, err := cb.Msg().PullMsgBySeq(ctx, u2, []*model.SeqRange{{
		ConversationID: convID,
		Begin:          baseSeq + 1,
		End:            baseSeq + 10,
		Num:            10,
		Order:          model.PullOrderAsc,
	}}, model.PullOrderAsc)
	require.NoError(t, err, "PullMsgBySeq")
	pr, ok := res[convID]
	require.True(t, ok, "pull result should contain conv %s", convID)
	require.Len(t, pr.Msgs, 10, "offline window msgs")
	gotIDs := make(map[string]bool, 10)
	for _, m := range pr.Msgs {
		gotIDs[m.ClientMsgID] = true
	}
	for _, id := range sent {
		assert.True(t, gotIDs[id], "missing offline msg %s", id)
	}
}

// waitConvID 轮询等待 B 侧出现与 peer 的 C2C 会话（会话由预热消息异步创建）
func waitConvID(t *testing.T, c client.Client, ownerUserID, peerUserID string) string {
	t.Helper()
	deadline := time.Now().Add(eventTimeout)
	for time.Now().Before(deadline) {
		convs, err := c.Conv().GetAllConversations(context.Background(), ownerUserID)
		if err == nil {
			for _, cv := range convs {
				// C2C 会话：ConversationType=1，UserID 为对端 userID
				if cv.ConversationType == model.SessionTypeC2C && cv.UserID == peerUserID {
					return cv.ConversationID
				}
			}
		}
		time.Sleep(300 * time.Millisecond)
	}
	t.Fatalf("conversation with %s not found for %s within %v", peerUserID, ownerUserID, eventTimeout)
	return ""
}

// TestE2_HistoryLocalStorage E2.6 历史消息落库：B 在线收发 N 条后直接查其 SQLite，
// 消息与 seq 完整（连续、升序）、可按会话查询、无关会话不串数据
func TestE2_HistoryLocalStorage(t *testing.T) {
	requireEnv(t)

	u1, u2 := uniqueUser("a"), uniqueUser("b")
	registerUsers(t, u1, u2)

	ca, eca := newTestClient(t, "A")
	cb, ecb, storB := newTestClientWithStorage(t, "B")
	ctx, cancel := context.WithTimeout(context.Background(), 2*loginTimeout)
	defer cancel()

	require.NoError(t, ca.Login(ctx, u1, platformID))
	eca.wait(t, events.EventLoginSuccess, eventTimeout)
	require.NoError(t, cb.Login(ctx, u2, platformID))
	ecb.wait(t, events.EventLoginSuccess, eventTimeout)

	// 先订阅后触发（R6）：预热与后续消息都经 2001 推送 → persistPushMsg 实时落库
	mb := newMsgBox(cb, "B")

	// 预热 1 条建立会话
	warm := newTextMsg(u1, u2, "", model.SessionTypeC2C, "warmup")
	_, err := ca.Msg().SendMsg(ctx, warm)
	require.NoError(t, err, "warmup send")
	convID := waitConvID(t, cb, u2, u1)

	// A 连发 5 条，B 逐条实时收（OnNewMessage 前落库已完成，事件即落库完成信号）
	const n = 5
	sent := map[string]string{warm.ClientMsgID: warm.Content}
	for i := 0; i < n; i++ {
		m := newTextMsg(u1, u2, "", model.SessionTypeC2C, fmt.Sprintf("history-%d", i))
		_, err := ca.Msg().SendMsg(ctx, m)
		require.NoError(t, err, "send %d", i)
		sent[m.ClientMsgID] = m.Content
	}
	for id, content := range sent {
		got := mb.recv(t, id, eventTimeout)
		assert.Equal(t, content, got.Content, "push content of %s", id)
	}

	// B 的 SQLite：按会话查询，warmup + 5 条齐全
	msgs, err := storB.ListMsgsByConversation(u2, convID)
	require.NoError(t, err, "list msgs from sqlite")
	require.Len(t, msgs, n+1, "warmup + %d history msgs in local sqlite", n)

	byID := make(map[string]*model.MsgData, len(msgs))
	for _, m := range msgs {
		require.Positive(t, m.Seq, "server-assigned seq persisted")
		require.Equal(t, convID, m.ConversationID, "msg belongs to the conversation")
		byID[m.ClientMsgID] = m
	}
	for id, content := range sent {
		m, ok := byID[id]
		require.True(t, ok, "msg %s missing in sqlite", id)
		assert.Equal(t, content, m.Content, "content of %s", id)
	}
	// seq 连续递增（单会话单发送方窗口内每条 seq +1，与 E2.3/E2.4 断言同一假设）
	for i := 1; i < len(msgs); i++ {
		require.Equal(t, msgs[i-1].Seq+1, msgs[i].Seq, "seq consecutive at idx %d", i)
	}

	// 可按会话查询隔离：无关会话不串数据
	other, err := storB.ListMsgsByConversation(u2, "c2c_not_exist_"+uuidx.New())
	require.NoError(t, err, "list unrelated conversation")
	assert.Empty(t, other, "unrelated conversation should have no msgs")
}

// TestE2_PersistenceBothPaths E2.7 消息持久化参数：在线（gateway 2001 推送）与
// 离线（重登后 SeqSync gap 拉取）两条路径均可达，同一批 clientMsgID 在本地库汇合
func TestE2_PersistenceBothPaths(t *testing.T) {
	requireEnv(t)

	u1, u2 := uniqueUser("a"), uniqueUser("b")
	registerUsers(t, u1, u2)

	ca, eca := newTestClient(t, "A")
	cb, ecb, storB := newTestClientWithStorage(t, "B")
	ctx, cancel := context.WithTimeout(context.Background(), 3*loginTimeout)
	defer cancel()

	require.NoError(t, ca.Login(ctx, u1, platformID))
	eca.wait(t, events.EventLoginSuccess, eventTimeout)
	require.NoError(t, cb.Login(ctx, u2, platformID))
	ecb.wait(t, events.EventLoginSuccess, eventTimeout)

	mb := newMsgBox(cb, "B")

	// 预热建立会话
	warm := newTextMsg(u1, u2, "", model.SessionTypeC2C, "warmup")
	_, err := ca.Msg().SendMsg(ctx, warm)
	require.NoError(t, err, "warmup send")
	convID := waitConvID(t, cb, u2, u1)

	// 路径一（在线推送）：A 发 3 条，B 实时收
	const nOnline = 3
	online := make(map[string]string, nOnline)
	for i := 0; i < nOnline; i++ {
		m := newTextMsg(u1, u2, "", model.SessionTypeC2C, fmt.Sprintf("online-%d", i))
		_, err := ca.Msg().SendMsg(ctx, m)
		require.NoError(t, err, "online send %d", i)
		online[m.ClientMsgID] = m.Content
	}
	liveIDs := make(map[string]bool, nOnline)
	for id, content := range online {
		got := mb.recv(t, id, eventTimeout)
		liveIDs[id] = true
		assert.Equal(t, content, got.Content, "live push content of %s", id)
	}

	// 路径二（离线 sync）：B 登出 → A 发 3 条 → B 重登走 gap 拉取。
	// 服务端方案 A（write-through）后 1003 ack 即提交信号，发送完直接重登
	require.NoError(t, cb.Logout(false))
	const nOffline = 3
	offline := make(map[string]string, nOffline)
	offlineIDs := make(map[string]bool, nOffline)
	for i := 0; i < nOffline; i++ {
		m := newTextMsg(u1, u2, "", model.SessionTypeC2C, fmt.Sprintf("offline-%d", i))
		_, err := ca.Msg().SendMsg(ctx, m)
		require.NoError(t, err, "offline send %d", i)
		offline[m.ClientMsgID] = m.Content
		offlineIDs[m.ClientMsgID] = true
	}

	require.NoError(t, cb.Login(ctx, u2, platformID))
	ecb.wait(t, events.EventLoginSuccess, eventTimeout)
	ecb.wait(t, events.EventSyncFinish, eventTimeout) // 后台 SeqSync 补齐完成

	// 方案 A（write-through）回归哨兵：ack 返回时服务端已完成 seq 分配+落库+
	// 建会话+游标推进（goim docs/msg-send-sync-commit.md），B 首登 gap 拉取必达——
	// 首轮同步即须全部到位，不做重登重拉兜底（服务端若退回异步持久化管道，
	// 此处立刻红）
	firstSync, err := storB.ListMsgsByConversation(u2, convID)
	require.NoError(t, err, "list msgs after first sync")
	firstGot := make(map[string]bool, len(firstSync))
	for _, m := range firstSync {
		firstGot[m.ClientMsgID] = true
	}
	missing := make([]string, 0, len(offlineIDs))
	for id := range offlineIDs {
		if !firstGot[id] {
			missing = append(missing, id)
		}
	}
	require.Empty(t, missing, "offline msgs missing after first sync: write-through contract broken (R12 regression?)")

	// 两条路径的消息按同一 clientMsgID 在 B 的 SQLite 汇合，内容一致
	msgs, err := storB.ListMsgsByConversation(u2, convID)
	require.NoError(t, err, "list msgs after resync")
	byID := make(map[string]*model.MsgData, len(msgs))
	for _, m := range msgs {
		byID[m.ClientMsgID] = m
	}
	all := make(map[string]string, nOnline+nOffline+1)
	for id, c := range online {
		all[id] = c
	}
	for id, c := range offline {
		all[id] = c
	}
	all[warm.ClientMsgID] = warm.Content
	for id, content := range all {
		m, ok := byID[id]
		require.True(t, ok, "msg %s missing after sync (both paths converge)", id)
		assert.Equal(t, content, m.Content, "content of %s", id)
	}
	// 离线窗口内的消息不可能经实时推送到达（登出状态），佐证其走 sync 路径
	for id := range offline {
		assert.False(t, liveIDs[id], "offline msg %s must not arrive via live push", id)
	}
}

// TestE2_PushPersistNoCursorAdvance E2.8 推送消息落库（2001 实时持久化）：
// B 在线收到 OnNewMessage 时消息已在 SQLite（clientMsgID/内容一致），
// 且 maxSeqs 游标未被推送推进——游标仅由 gap 拉取推进，防推送丢失/乱序漏消息
func TestE2_PushPersistNoCursorAdvance(t *testing.T) {
	requireEnv(t)

	u1, u2 := uniqueUser("a"), uniqueUser("b")
	registerUsers(t, u1, u2)

	ca, eca := newTestClient(t, "A")
	cb, ecb, storB := newTestClientWithStorage(t, "B")
	ctx, cancel := context.WithTimeout(context.Background(), 3*loginTimeout)
	defer cancel()

	// A 先登录，向未登录的 B 发预热消息（服务端持久化）；B 首登 gap 拉取建立游标基线
	require.NoError(t, ca.Login(ctx, u1, platformID))
	eca.wait(t, events.EventLoginSuccess, eventTimeout)

	warm := newTextMsg(u1, u2, "", model.SessionTypeC2C, "warmup-offline")
	_, err := ca.Msg().SendMsg(ctx, warm)
	require.NoError(t, err, "warmup send to offline B")

	require.NoError(t, cb.Login(ctx, u2, platformID))
	ecb.wait(t, events.EventLoginSuccess, eventTimeout)
	ecb.wait(t, events.EventSyncFinish, eventTimeout) // 首登同步完成：游标 + 预热消息落库

	mb := newMsgBox(cb, "B") // 先订阅后触发（R6）
	convID := waitConvID(t, cb, u2, u1)

	maxSeqs, _, err := storB.GetMaxSeqs(u2)
	require.NoError(t, err, "get maxSeqs baseline")
	baseSeq := maxSeqs[convID]
	require.Positive(t, baseSeq, "cursor established by login sync for conv %s", convID)
	t.Logf("[it-B] baseline: conv=%s maxSeq=%d", convID, baseSeq)

	// B 在线，A 发 1 条 → 2001 推送
	content := "push-persist-" + time.Now().Format("150405")
	msg := newTextMsg(u1, u2, "", model.SessionTypeC2C, content)
	_, err = ca.Msg().SendMsg(ctx, msg)
	require.NoError(t, err, "online send")

	got := mb.recv(t, msg.ClientMsgID, eventTimeout) // OnNewMessage 已触发
	assert.Equal(t, content, got.Content)

	// 推送实时落库：handleMessagePush 先 persistPushMsg 再发布事件，收到事件即已落库
	msgs, err := storB.ListMsgsByConversation(u2, convID)
	require.NoError(t, err, "list msgs after push")
	var found *model.MsgData
	for _, m := range msgs {
		if m.ClientMsgID == msg.ClientMsgID {
			found = m
			break
		}
	}
	require.NotNil(t, found, "pushed msg should be persisted in sqlite immediately")
	assert.Equal(t, content, found.Content)
	assert.Positive(t, found.Seq)

	// 游标未被推送推进（推送可能丢失/乱序，仅 gap 拉取推进）
	afterSeqs, _, err := storB.GetMaxSeqs(u2)
	require.NoError(t, err, "get maxSeqs after push")
	assert.Equal(t, baseSeq, afterSeqs[convID], "cursor must not advance on push")
}
