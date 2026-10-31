package integration

// E2 消息链路（docs/INTEGRATION_TEST_PLAN.md §3 E2，核心）
// 覆盖：E2.1 单聊收发 / E2.2 群聊收发 / E2.3 离线 SeqSync 补齐 / E2.4 NewestSeq / E2.5 PullMsgBySeq
// （E2.6 本地落库归属 E6.1 重启恢复场景深入验证；E2.7 由 E2.1 在线 + E2.3 离线两条路径共同覆盖）

import (
	"context"
	"strconv"
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
}

func newMsgBox(c client.Client) *msgBox {
	mb := &msgBox{ch: make(chan *model.MsgData, 64)}
	c.OnNewMessage(func(msg *model.MsgData) {
		select {
		case mb.ch <- msg:
		default: // 通道满丢弃计数场景仅存在于极端压测，集成测试 buffer 足够
		}
	})
	return mb
}

// recv 等待收到指定 clientMsgID 的消息（超时 Fatal）
func (mb *msgBox) recv(t *testing.T, clientMsgID string, timeout time.Duration) *model.MsgData {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case msg := <-mb.ch:
			if clientMsgID == "" || msg.ClientMsgID == clientMsgID {
				return msg
			}
			// 非目标消息继续等（通道内可能有其他测试流量）
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

	ca, eca := newTestClient(t)
	cb, ecb := newTestClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*loginTimeout)
	defer cancel()

	require.NoError(t, ca.Login(ctx, u1, platformID))
	eca.wait(t, events.EventLoginSuccess, eventTimeout)
	require.NoError(t, cb.Login(ctx, u2, platformID))
	ecb.wait(t, events.EventLoginSuccess, eventTimeout)

	// B 先订阅（防 R6）
	mb := newMsgBox(cb)

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
	t.Logf("sent: clientMsgID=%s serverMsgID=%s sendTime=%d", resp.ClientMsgID, resp.ServerMsgID, resp.SendTime)

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

	ca, eca := newTestClient(t)
	cb, ecb := newTestClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*loginTimeout)
	defer cancel()

	require.NoError(t, ca.Login(ctx, u1, platformID))
	eca.wait(t, events.EventLoginSuccess, eventTimeout)
	require.NoError(t, cb.Login(ctx, u2, platformID))
	ecb.wait(t, events.EventLoginSuccess, eventTimeout)

	mb := newMsgBox(cb)

	// A 建群（owner=A，成员 B）
	group, err := ca.Group().CreateGroup(ctx,
		&model.GroupInfo{GroupName: "e2-group-" + time.Now().Format("150405")},
		[]string{u2}, nil, u1, false)
	require.NoError(t, err, "create group")
	require.NotEmpty(t, group.GroupID)
	t.Logf("group created: %s", group.GroupID)

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

	ca, eca := newTestClient(t)
	cb, ecb := newTestClient(t)
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
	t.Logf("baseline: conv=%s baseSeq=%d", convID, baseSeq)

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
		Begin:          strconv.FormatInt(baseSeq+1, 10),
		End:            strconv.FormatInt(baseSeq+10, 10),
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
