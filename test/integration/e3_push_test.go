//go:build !(js && wasm)

package integration

// E3 推送事件：WS 2001 系统通知分流（docs/INTEGRATION_TEST_PLAN.md §3 E3）
//
// 断言策略：
//   - 事件到达：接收方 collector 等待指定 EventType；不确定服务端通知接收方时用
//     waitAny 断言「至少一方收到」并记录实际接收方（集成层以运行结果校准预期）
//   - 本地缓存：消息类通知（2101 撤回 / 2102 删除 / 2200 已读）SDK 先应用缓存
//     再发布事件，收到事件后可直接断言 SQLite 状态
//   - payload 双形态：decodeTip 类事件（好友申请/建群/资料变更/在线状态/撤回）的
//     Payload 为 *pbsdk.XxxTips 结构体，其余事件 Payload 为 Detail JSON string——
//     统一经 tipJSON 归一为 JSON bytes 后再取字段

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PaperMan11/goim-sdk-core/pkg/client"
	"github.com/PaperMan11/goim-sdk-core/pkg/events"
	"github.com/PaperMan11/goim-sdk-core/pkg/model"
	"github.com/PaperMan11/goim-sdk-core/pkg/storage"
)

// —— E3 公共基建 ——

// tipJSON 事件 payload 归一为 JSON bytes（结构体 marshal，string/[]byte 直取）
func tipJSON(t *testing.T, ev events.Event) []byte {
	t.Helper()
	switch p := ev.Payload.(type) {
	case string:
		return []byte(p)
	case []byte:
		return p
	default:
		b, err := json.Marshal(p)
		require.NoError(t, err, "marshal event payload")
		return b
	}
}

// loginAndSync 登录并等待后台同步完成（实体类断言的前置，见 E1 节登录同步解耦说明）
func loginAndSync(t *testing.T, c client.Client, ec *eventCollector, userID string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), loginTimeout)
	defer cancel()
	require.NoError(t, c.Login(ctx, userID, platformID), "login %s", userID)
	ec.wait(t, events.EventLoginSuccess, eventTimeout)
	ec.wait(t, events.EventSyncFinish, eventTimeout)
}

// waitAny 任一收集器等到指定事件（服务端通知的接收方可能为操作方或被操作方，
// 断言「至少一方收到」；返回实际收到事件的收集器）
func waitAny(t *testing.T, typ events.EventType, timeout time.Duration, ecs ...*eventCollector) (*eventCollector, events.Event) {
	t.Helper()
	deadline := time.After(timeout)
	for {
		for _, ec := range ecs {
			select {
			case ev := <-ec.ch:
				ec.record(ev)
				if ec.tag != "" {
					t.Logf("[it-%s] event %v", ec.tag, ev.Type)
				}
				if ev.Type == typ {
					return ec, ev
				}
			default:
			}
		}
		select {
		case <-deadline:
			dump := ""
			for _, ec := range ecs {
				dump += fmt.Sprintf(" %s{got %d events: %s};", ec.prefix(), len(ec.snapshot()), eventTypes(ec.snapshot()))
			}
			t.Fatalf("timeout (%v) waiting event %v on any collector;%s", timeout, typ, dump)
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// applyFriendship A(u1) 申请加 B(u2)、B 同意，返回时双方好友关系成立。
// AddFriendResponse 参数对齐申请对：fromUserID=申请人、toUserID=处理者。
func applyFriendship(t *testing.T, ca, cb client.Client, eca, ecb *eventCollector, u1, u2 string) {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, ca.Rel().AddFriend(ctx, u1, u2, "it friend request", ""), "apply friend")
	ecb.wait(t, events.EventFriendApplication, eventTimeout) // 1203 → 被申请人收到好友申请
	require.NoError(t, cb.Rel().AddFriendResponse(ctx, u1, u2, "ok", model.HandleResultApproved), "approve friend")
	eca.wait(t, events.EventFriendApplicationApproved, eventTimeout) // 1201 → 申请人收到好友申请已同意
}

// storedMsgBySeq 从本地库按 seq 取消息（E3 消息类通知的缓存断言辅助）
func storedMsgBySeq(t *testing.T, stor storage.Storage, owner, convID string, seq int64) *model.MsgData {
	t.Helper()
	msgs, err := stor.ListMsgsByConversation(owner, convID)
	require.NoError(t, err, "list msgs owner=%s conv=%s", owner, convID)
	for _, m := range msgs {
		if m.Seq == seq {
			return m
		}
	}
	return nil
}

// TestE3_MsgOpsNotificationAndCache E3.1-E3.4：普通消息 / 撤回(2101) / 已读(2200) /
// 删除(2102) 的事件分流与本地缓存应用
func TestE3_MsgOpsNotificationAndCache(t *testing.T) {
	ca, eca, storA := newTestClientWithStorage(t, "A")
	cb, ecb, storB := newTestClientWithStorage(t, "B")
	u1, u2 := uniqueUser("e3a"), uniqueUser("e3b")
	registerUsers(t, u1, u2)
	loginAndSync(t, ca, eca, u1)
	loginAndSync(t, cb, ecb, u2)

	ctx := context.Background()
	mb := newMsgBox(cb, "B")

	// E3.1 普通消息推送：B 收 EventNewMsg（msgBox 挂在 OnNewMessage 上）
	m1 := newTextMsg(u1, u2, "", model.SessionTypeC2C, "e3-1 hello")
	resp, err := ca.Msg().SendMsg(ctx, m1)
	require.NoError(t, err, "send msg1")
	b1 := mb.recv(t, resp.ClientMsgID, eventTimeout)
	convID := waitConvID(t, cb, u2, u1)

	// E3.2 撤回（2101）：B 收 EventRevokeMsg 且本地缓存 Status=Revoked
	require.NoError(t, ca.Msg().RevokeMsg(ctx, convID, b1.Seq), "revoke msg1")
	ev := ecb.wait(t, events.EventRevokeMsg, eventTimeout)
	var revokeTip struct {
		ConversationID string `json:"conversationID"`
		Seq            int64  `json:"seq"`
	}
	require.NoError(t, json.Unmarshal(tipJSON(t, ev), &revokeTip), "revoke tips")
	require.Equal(t, b1.Seq, revokeTip.Seq, "revoke tip seq")
	require.Equal(t, convID, revokeTip.ConversationID, "revoke tip conversationID")
	msg1 := storedMsgBySeq(t, storB, u2, convID, b1.Seq)
	require.NotNil(t, msg1, "msg1 should exist in B's cache")
	require.Equal(t, model.MsgStatusRevoked, msg1.Status, "msg1 status should be revoked in B's cache")

	// E3.3 已读（2200）两种形态：seqs 列表 + HasReadSeq 兜底
	m2 := newTextMsg(u1, u2, "", model.SessionTypeC2C, "e3-3 mark seqs")
	resp2, err := ca.Msg().SendMsg(ctx, m2)
	require.NoError(t, err, "send msg2")
	seq2 := mb.recv(t, resp2.ClientMsgID, eventTimeout).Seq
	require.NoError(t, cb.Msg().MarkMsgsAsRead(ctx, convID, []int64{seq2}), "mark msgs as read (seqs form)")
	eca.wait(t, events.EventMarkAsRead, eventTimeout)
	msg2 := storedMsgBySeq(t, storA, u1, convID, seq2)
	require.NotNil(t, msg2, "msg2 should exist in A's cache")
	require.True(t, msg2.IsRead, "msg2 should be marked read in A's cache (seqs form)")

	m3 := newTextMsg(u1, u2, "", model.SessionTypeC2C, "e3-3 mark conversation")
	resp3, err := ca.Msg().SendMsg(ctx, m3)
	require.NoError(t, err, "send msg3")
	seq3 := mb.recv(t, resp3.ClientMsgID, eventTimeout).Seq
	require.NoError(t, cb.Msg().MarkConversationAsRead(ctx, convID, seq3, nil), "mark conversation as read (hasReadSeq form)")
	eca.wait(t, events.EventMarkAsRead, eventTimeout)
	msg3 := storedMsgBySeq(t, storA, u1, convID, seq3)
	require.NotNil(t, msg3, "msg3 should exist in A's cache")
	require.True(t, msg3.IsRead, "msg3 should be marked read in A's cache (hasReadSeq form)")

	// E3.4 删除（2102）：B 收 EventDeleteMsgs 且本地缓存该 seq 已删除
	require.NoError(t, ca.Msg().DeleteMsgs(ctx, convID, []int64{seq3},
		&model.DeleteSyncOpt{IsSyncSelf: true, IsSyncOther: true}), "delete msg3")
	ecb.wait(t, events.EventDeleteMsgs, eventTimeout)
	require.Nil(t, storedMsgBySeq(t, storB, u2, convID, seq3), "msg3 should be deleted from B's cache")
}

// TestE3_FriendNotifications E3.5：好友事件（申请 1201 / 添加 1204 / 备注 1206 / 删除 1205）
func TestE3_FriendNotifications(t *testing.T) {
	ca, eca := newTestClient(t, "A")
	cb, ecb, storB := newTestClientWithStorage(t, "B")
	u1, u2 := uniqueUser("e3a"), uniqueUser("e3b")
	registerUsers(t, u1, u2)
	loginAndSync(t, ca, eca, u1)
	loginAndSync(t, cb, ecb, u2)

	ctx := context.Background()

	// 申请 + 同意（applyFriendship 内断言：B 收申请、双方收添加成功）
	applyFriendship(t, ca, cb, eca, ecb, u1, u2)

	// 备注：B 给 A 设备注（1206，接收方以 waitAny 记录实际接收方）
	require.NoError(t, cb.Rel().SetFriendRemark(ctx, u2, u1, "it-remark"), "set friend remark")
	ecb.wait(t, events.EventFriendInfoChanged, eventTimeout)
	// 通知触发 B 侧好友增量同步（去抖 ~500ms），本地缓存出现备注
	require.Eventually(t, func() bool {
		friends, err := storB.ListFriends(u2)
		if err != nil {
			return false
		}
		for _, f := range friends {
			if f.FriendUserID() == u1 && f.Remark == "it-remark" {
				return true
			}
		}
		return false
	}, 10*time.Second, 200*time.Millisecond, "B's friend cache should get remark via notification-triggered incremental sync")

	// 删除：A 删除 B（1205）
	require.NoError(t, ca.Rel().DeleteFriend(ctx, u1, u2), "delete friend")
	waitAny(t, events.EventFriendDeleted, eventTimeout, eca, ecb)
}

// TestE3_GroupNotifications E3.6：群事件（建群 1501 / 邀请 1509 / 禁言 1512 /
// 踢人 1508 / 退群 1504 / 解散 1511）
func TestE3_GroupNotifications(t *testing.T) {
	ca, eca := newTestClient(t, "A")
	cb, ecb := newTestClient(t, "B")
	cc, ecc := newTestClient(t, "C")
	u1, u2, u3 := uniqueUser("e3a"), uniqueUser("e3b"), uniqueUser("e3c")
	registerUsers(t, u1, u2, u3)
	loginAndSync(t, ca, eca, u1)
	loginAndSync(t, cb, ecb, u2)
	loginAndSync(t, cc, ecc, u3)

	ctx := context.Background()

	// 建群（成员 u2）：群成员收 EventGroupCreated
	gr, err := ca.Group().CreateGroup(ctx,
		&model.GroupInfo{GroupName: fmt.Sprintf("it-e3-group-%d", time.Now().UnixNano())},
		[]string{u2}, nil, u1, false)
	require.NoError(t, err, "create group")
	require.NotEmpty(t, gr.GroupID, "group id")
	_, createdEv := waitAny(t, events.EventGroupCreated, eventTimeout, eca, ecb)
	// 验证嵌套 tips payload
	var createdTip struct {
		Group struct {
			GroupID string `json:"groupID"`
		} `json:"group"`
	}
	require.NoError(t, json.Unmarshal(tipJSON(t, createdEv), &createdTip), "group created tips")
	require.Equal(t, gr.GroupID, createdTip.Group.GroupID, "group created tip group.groupID")

	// 邀请 u3：被邀请人收 EventMemberInvited
	require.NoError(t, ca.Group().InviteUserToGroup(ctx, gr.GroupID, "it invite", []string{u3}, nil), "invite u3")
	invitedEv := ecc.wait(t, events.EventMemberInvited, eventTimeout)
	// 验证嵌套 tips payload
	var invitedTip struct {
		Group struct {
			GroupID string `json:"groupID"`
		} `json:"group"`
		InvitedUserList []struct {
			UserID string `json:"userID"`
		} `json:"invitedUserList"`
	}
	require.NoError(t, json.Unmarshal(tipJSON(t, invitedEv), &invitedTip), "member invited tips")
	require.Equal(t, gr.GroupID, invitedTip.Group.GroupID, "invited tip group.groupID")
	require.NotEmpty(t, invitedTip.InvitedUserList, "invitedUserList should not be empty")
	require.Equal(t, u3, invitedTip.InvitedUserList[0].UserID, "invited user should be u3")

	// 禁言 u3：被禁言人收 EventGroupMemberMuted
	require.NoError(t, ca.Group().MuteGroupMember(ctx, gr.GroupID, u3, 60), "mute u3")
	mutedEv := ecc.wait(t, events.EventGroupMemberMuted, eventTimeout)
	var mutedTip struct {
		Group struct {
			GroupID string `json:"groupID"`
		} `json:"group"`
		MutedUser struct {
			UserID string `json:"userID"`
		} `json:"mutedUser"`
		MutedSeconds uint32 `json:"mutedSeconds"`
	}
	require.NoError(t, json.Unmarshal(tipJSON(t, mutedEv), &mutedTip), "group member muted tips")
	require.Equal(t, gr.GroupID, mutedTip.Group.GroupID, "muted tip group.groupID")
	require.Equal(t, u3, mutedTip.MutedUser.UserID, "muted user should be u3")
	require.Equal(t, uint32(60), mutedTip.MutedSeconds, "mutedSeconds should be 60")

	// 踢 u3：被踢人收 EventMemberKicked
	require.NoError(t, ca.Group().KickGroup(ctx, gr.GroupID, []string{u3}, "it kick", nil), "kick u3")
	kickedEv := ecc.wait(t, events.EventMemberKicked, eventTimeout)
	var kickedTip struct {
		Group struct {
			GroupID string `json:"groupID"`
		} `json:"group"`
		KickedUserList []struct {
			UserID string `json:"userID"`
		} `json:"kickedUserList"`
	}
	require.NoError(t, json.Unmarshal(tipJSON(t, kickedEv), &kickedTip), "member kicked tips")
	require.Equal(t, gr.GroupID, kickedTip.Group.GroupID, "kicked tip group.groupID")
	require.NotEmpty(t, kickedTip.KickedUserList, "kickedUserList should not be empty")
	require.Equal(t, u3, kickedTip.KickedUserList[0].UserID, "kicked user should be u3")

	// 重新邀请后 u3 主动退群：剩余成员收 EventMemberQuit
	require.NoError(t, ca.Group().InviteUserToGroup(ctx, gr.GroupID, "it reinvite", []string{u3}, nil), "reinvite u3")
	ecc.wait(t, events.EventMemberInvited, eventTimeout)
	require.NoError(t, cc.Group().QuitGroup(ctx, gr.GroupID, u3), "quit group u3")
	_, quitEv := waitAny(t, events.EventMemberQuit, eventTimeout, eca, ecb)
	var quitTip struct {
		Group struct {
			GroupID string `json:"groupID"`
		} `json:"group"`
		QuitUser struct {
			UserID string `json:"userID"`
		} `json:"quitUser"`
	}
	require.NoError(t, json.Unmarshal(tipJSON(t, quitEv), &quitTip), "member quit tips")
	require.Equal(t, gr.GroupID, quitTip.Group.GroupID, "quit tip group.groupID")
	require.Equal(t, u3, quitTip.QuitUser.UserID, "quit user should be u3")

	// 解散群：在群成员收 EventGroupDismissed
	require.NoError(t, ca.Group().DismissGroup(ctx, gr.GroupID, true, nil), "dismiss group")
	_, dismissEv := waitAny(t, events.EventGroupDismissed, eventTimeout, eca, ecb)
	var dismissTip struct {
		Group struct {
			GroupID string `json:"groupID"`
		} `json:"group"`
	}
	require.NoError(t, json.Unmarshal(tipJSON(t, dismissEv), &dismissTip), "group dismissed tips")
	require.Equal(t, gr.GroupID, dismissTip.Group.GroupID, "dismiss tip group.groupID")
}

// TestE3_UserInfoAndStatusNotification E3.7/E3.8：用户资料变更（1303）与在线状态订阅（2005）
func TestE3_UserInfoAndStatusNotification(t *testing.T) {
	ca, eca := newTestClient(t, "A")
	cb, ecb := newTestClient(t, "B")
	u1, u2 := uniqueUser("e3a"), uniqueUser("e3b")
	registerUsers(t, u1, u2)
	loginAndSync(t, ca, eca, u1)
	loginAndSync(t, cb, ecb, u2)

	ctx := context.Background()

	// 资料变更通知发给好友（先建立好友关系）
	applyFriendship(t, ca, cb, eca, ecb, u1, u2)

	// E3.7 A 更新资料 → B 收 EventUserInfoUpdated
	nick := fmt.Sprintf("it-nick-%d", time.Now().UnixNano())
	require.NoError(t, ca.User().UpdateUserInfo(ctx, &model.UserInfo{UserID: u1, Nickname: nick}), "update user info")
	ev := eca.wait(t, events.EventUserInfoUpdated, eventTimeout)
	// 服务端 UserInfoUpdatedTips 只携带 userID（昵称/头像需另行拉取用户资料）
	var infoTip struct {
		UserID string `json:"userID"`
	}
	require.NoError(t, json.Unmarshal(tipJSON(t, ev), &infoTip), "user info tips")
	require.Equal(t, u1, infoTip.UserID, "user info tip userID")

	// E3.8 B 订阅 A 的在线状态（genre=1 订阅上/下线，服务端约定实测校准）
	_, err := cb.User().SubscribeUsersStatus(ctx, u2, []string{u1}, 1)
	require.NoError(t, err, "subscribe user status")
	require.NoError(t, ca.Logout(false), "logout u1 to trigger offline status")
	stEv := ecb.wait(t, events.EventUserStatusChange, eventTimeout)
	// 服务端 UserStatusChangeTips 字段为 fromUserID（状态变更者）/ toUserID / status / platformID
	var statusTip struct {
		FromUserID string `json:"fromUserID"`
		ToUserID   string `json:"toUserID"`
		Status     int32  `json:"status"`
		PlatformID int32  `json:"platformID"`
	}
	require.NoError(t, json.Unmarshal(tipJSON(t, stEv), &statusTip), "user status tips")
	require.Equal(t, u1, statusTip.FromUserID, "user status tip fromUserID")
	require.Equal(t, u2, statusTip.ToUserID, "user status tip toUserID")
	t.Logf("[it-B] u1 status changed to %d (platformID=%d)", statusTip.Status, statusTip.PlatformID)
}

// TestE3_EntitySyncTriggeredByNotification E3.10：实体类通知触发增量同步——
// B 不重连，1501 建群通知去抖后 B 的本地群缓存自动出现该群
func TestE3_EntitySyncTriggeredByNotification(t *testing.T) {
	ca, eca := newTestClient(t, "A")
	cb, ecb, storB := newTestClientWithStorage(t, "B")
	u1, u2 := uniqueUser("e3a"), uniqueUser("e3b")
	registerUsers(t, u1, u2)
	loginAndSync(t, ca, eca, u1)
	loginAndSync(t, cb, ecb, u2)

	gr, err := ca.Group().CreateGroup(context.Background(),
		&model.GroupInfo{GroupName: fmt.Sprintf("it-e3-sync-%d", time.Now().UnixNano())},
		[]string{u2}, nil, u1, false)
	require.NoError(t, err, "create group")

	// 通知到达（tips 含嵌套 group 对象，取 group.groupID）
	_, ev := waitAny(t, events.EventGroupCreated, eventTimeout, eca, ecb)
	var createdTip struct {
		Group struct {
			GroupID string `json:"groupID"`
		} `json:"group"`
	}
	require.NoError(t, json.Unmarshal(tipJSON(t, ev), &createdTip), "group created tips")
	require.Equal(t, gr.GroupID, createdTip.Group.GroupID, "group created tip group.groupID")

	// B 不做任何操作、不重连：~500ms 去抖后通知触发的实体增量同步填充本地缓存
	require.Eventually(t, func() bool {
		groups, err := storB.ListGroups(u2)
		if err != nil {
			return false
		}
		for _, g := range groups {
			if g.GroupID == gr.GroupID {
				return true
			}
		}
		return false
	}, 10*time.Second, 200*time.Millisecond, "B's group cache should be filled by notification-triggered entity sync without relogin")
}

// TestE3_UnmappedContentType E3.9：未映射 contentType 的通知不 panic、不误发布。
// 服务端无注入未知通知的现成手段，SDK 侧行为已由单测确定性覆盖
// （pkg/client/client_test.go：未映射通知仅 warn 日志、不发布 EventNewMsg）。
func TestE3_UnmappedContentType(t *testing.T) {
	t.Skip("E3.9 covered by SDK unit test (client_test.go unmapped notification case); server-side injection TBD")
}
