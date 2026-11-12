//go:build !(js && wasm)

package integration

// E4 五类实体增量同步（docs/INTEGRATION_TEST_PLAN.md §3 E4）
//
// 每类实体走「登录后台首拉 → 对端操作产生增量 → 通知触发/重登补齐」验证：
//   - 断言目标为本地 SQLite 缓存（newTestClientWithStorage 注入的同一 Storage 句柄）
//   - 通知触发的定向增量同步有约 500ms 去抖（client_impl.go triggerEntitySync），
//     事件到达 ≠ 缓存已更新，缓存断言统一用 require.Eventually 收敛（同 E3.5/E3.10 模式）
//   - 1205 FriendDeleted 不触发自动同步（dispatchNotification 未挂 triggerEntitySync），
//     好友删除断言走 Logout(false) 重登的游标增量补齐（计划三步法第三步：重登增量补齐）

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PaperMan11/goim-sdk-core/pkg/client"
	"github.com/PaperMan11/goim-sdk-core/pkg/events"
	"github.com/PaperMan11/goim-sdk-core/pkg/model"
	"github.com/PaperMan11/goim-sdk-core/pkg/storage"
)

const storeWaitTimeout = 15 * time.Second

// reloginAndSync 登出后重登并等后台同步完成（clearData=false 走游标增量补齐）
func reloginAndSync(t *testing.T, c client.Client, ec *eventCollector, userID string, clearData bool) {
	t.Helper()
	require.NoError(t, c.Logout(clearData), "logout %s (clearData=%v)", userID, clearData)
	loginAndSync(t, c, ec, userID)
}

// —— 本地缓存谓词（require.Eventually 轮询用）——

func hasFriend(stor storage.Storage, owner, friendID string) bool {
	friends, err := stor.ListFriends(owner)
	if err != nil {
		return false
	}
	for _, f := range friends {
		if f.FriendUserID() == friendID {
			return true
		}
	}
	return false
}

func hasBlack(stor storage.Storage, owner, blackID string) bool {
	blacks, err := stor.ListBlacks(owner)
	if err != nil {
		return false
	}
	for _, b := range blacks {
		if b.BlackUserID() == blackID {
			return true
		}
	}
	return false
}

func findGroup(stor storage.Storage, owner, groupID string) *model.GroupInfo {
	groups, err := stor.ListGroups(owner)
	if err != nil {
		return nil
	}
	for _, g := range groups {
		if g.GroupID == groupID {
			return g
		}
	}
	return nil
}

func hasGroup(stor storage.Storage, owner, groupID string) bool {
	return findGroup(stor, owner, groupID) != nil
}

func hasMember(stor storage.Storage, owner, groupID, userID string) bool {
	members, err := stor.ListGroupMembers(owner, groupID)
	if err != nil {
		return false
	}
	for _, m := range members {
		if m.UserID == userID {
			return true
		}
	}
	return false
}

// memberRole 成员角色（成员不存在返回 -1）
func memberRole(stor storage.Storage, owner, groupID, userID string) int32 {
	members, err := stor.ListGroupMembers(owner, groupID)
	if err != nil {
		return -1
	}
	for _, m := range members {
		if m.UserID == userID {
			return m.RoleLevel
		}
	}
	return -1
}

func findConv(stor storage.Storage, owner, conversationID string) *model.Conversation {
	convs, err := stor.ListConversations(owner)
	if err != nil {
		return nil
	}
	for _, c := range convs {
		if c.ConversationID == conversationID {
			return c
		}
	}
	return nil
}

// TestE4_FriendIncrementalSync E4.1 好友：首拉 → 通知触发增量 insert → 删除重登补齐 delete
func TestE4_FriendIncrementalSync(t *testing.T) {
	ca, eca := newTestClient(t, "A")
	cb, ecb, storB := newTestClientWithStorage(t, "B")
	cc, ecc := newTestClient(t, "C")
	u1, u2, u3 := uniqueUser("e4a"), uniqueUser("e4b"), uniqueUser("e4c")
	registerUsers(t, u1, u2, u3)
	loginAndSync(t, ca, eca, u1)
	loginAndSync(t, cb, ecb, u2)
	loginAndSync(t, cc, ecc, u3)

	require.False(t, hasFriend(storB, u2, u1), "baseline: B has no friend")

	// 增量 insert #1：u1 加 u2、u2 同意 → B 收 1204 触发好友增量同步
	applyFriendship(t, ca, cb, eca, ecb, u1, u2)
	require.Eventually(t, func() bool { return hasFriend(storB, u2, u1) },
		storeWaitTimeout, 200*time.Millisecond, "B friend cache should insert u1 via notification-triggered sync")

	// 增量 insert #2：u3 加 u2、u2 同意 → 同游标再增量
	applyFriendship(t, cc, cb, ecc, ecb, u3, u2)
	require.Eventually(t, func() bool { return hasFriend(storB, u2, u3) },
		storeWaitTimeout, 200*time.Millisecond, "B friend cache should insert u3")

	// 增量 delete：u1 删除 u2（1205 不触发自动同步）→ B 重登走游标增量补齐
	require.NoError(t, ca.Rel().DeleteFriend(context.Background(), u1, u2), "delete friend")
	waitAny(t, events.EventFriendDeleted, eventTimeout, eca, ecb)
	reloginAndSync(t, cb, ecb, u2, false)
	require.False(t, hasFriend(storB, u2, u1), "u1 should be deleted from B friend cache after relogin incremental")
	require.True(t, hasFriend(storB, u2, u3), "u3 should remain in B friend cache")
}

// TestE4_BlackIncrementalSync E4.2 黑名单：拉黑/移除增量（full=true 清表重拉由 E4.6 覆盖）
func TestE4_BlackIncrementalSync(t *testing.T) {
	ca, eca, storA := newTestClientWithStorage(t, "A")
	u1, u2 := uniqueUser("e4a"), uniqueUser("e4b")
	registerUsers(t, u1, u2)
	loginAndSync(t, ca, eca, u1)

	ctx := context.Background()
	require.False(t, hasBlack(storA, u1, u2), "baseline: A black list empty")

	// 拉黑 → 1207 触发黑名单增量 insert
	require.NoError(t, ca.Rel().AddBlack(ctx, u1, u2, ""), "add black")
	eca.wait(t, events.EventBlackAdded, eventTimeout)
	require.Eventually(t, func() bool { return hasBlack(storA, u1, u2) },
		storeWaitTimeout, 200*time.Millisecond, "A black cache should insert u2 via notification-triggered sync")

	// 移除 → 1208 触发增量 delete
	require.NoError(t, ca.Rel().RemoveBlack(ctx, u1, u2), "remove black")
	eca.wait(t, events.EventBlackDeleted, eventTimeout)
	require.Eventually(t, func() bool { return !hasBlack(storA, u1, u2) },
		storeWaitTimeout, 200*time.Millisecond, "A black cache should delete u2 via notification-triggered sync")
}

// TestE4_GroupIncrementalSync E4.3 群组：建群 insert / 邀请+转让 update / 解散 delete
func TestE4_GroupIncrementalSync(t *testing.T) {
	ca, eca, storA := newTestClientWithStorage(t, "A")
	cb, ecb, storB := newTestClientWithStorage(t, "B")
	cc, ecc, storC := newTestClientWithStorage(t, "C")
	u1, u2, u3 := uniqueUser("e4a"), uniqueUser("e4b"), uniqueUser("e4c")
	registerUsers(t, u1, u2, u3)
	loginAndSync(t, ca, eca, u1)
	loginAndSync(t, cb, ecb, u2)
	loginAndSync(t, cc, ecc, u3)

	ctx := context.Background()

	// insert：A 建群（成员 u2）→ 两端 joinGroups insert
	gr, err := ca.Group().CreateGroup(ctx,
		&model.GroupInfo{GroupName: fmt.Sprintf("it-e4-group-%d", time.Now().UnixNano())},
		[]string{u2}, nil, u1, false)
	require.NoError(t, err, "create group")
	gid := gr.GroupID
	require.NotEmpty(t, gid, "group id")
	waitAny(t, events.EventGroupCreated, eventTimeout, eca, ecb)
	require.Eventually(t, func() bool {
		return hasGroup(storA, u1, gid) && hasGroup(storB, u2, gid)
	}, storeWaitTimeout, 200*time.Millisecond, "A/B group cache should insert new group")
	require.Equal(t, u1, findGroup(storB, u2, gid).OwnerUserID, "group owner should be u1")

	// update：邀请 u3（memberCount 变化 → 存量成员 joinGroups update；u3 insert）
	require.NoError(t, ca.Group().InviteUserToGroup(ctx, gid, "it invite", []string{u3}, nil), "invite u3")
	ecc.wait(t, events.EventMemberInvited, eventTimeout)
	require.Eventually(t, func() bool { return hasGroup(storC, u3, gid) },
		storeWaitTimeout, 200*time.Millisecond, "C group cache should insert group on invited")

	// update：转让群主 u1 → u2 → 三端 OwnerUserID 更新
	require.NoError(t, ca.Group().TransferGroup(ctx, gid, u1, u2), "transfer group to u2")
	waitAny(t, events.EventGroupOwnerTransferred, eventTimeout, eca, ecb, ecc)
	require.Eventually(t, func() bool {
		return findGroup(storA, u1, gid).OwnerUserID == u2 &&
			findGroup(storB, u2, gid).OwnerUserID == u2 &&
			findGroup(storC, u3, gid).OwnerUserID == u2
	}, storeWaitTimeout, 200*time.Millisecond, "group owner should become u2 on all members' cache")

	// delete：新群主 u2 解散 → 成员 joinGroups delete
	require.NoError(t, cb.Group().DismissGroup(ctx, gid, true, nil), "dismiss group")
	waitAny(t, events.EventGroupDismissed, eventTimeout, eca, ecb, ecc)
	require.Eventually(t, func() bool {
		return !hasGroup(storA, u1, gid) && !hasGroup(storC, u3, gid)
	}, storeWaitTimeout, 200*time.Millisecond, "A/C group cache should delete dismissed group")
}

// TestE4_GroupMemberIncrementalSync E4.4 群成员：邀请 insert / 踢人 delete / 退群 delete
func TestE4_GroupMemberIncrementalSync(t *testing.T) {
	ca, eca, storA := newTestClientWithStorage(t, "A")
	cb, ecb, storB := newTestClientWithStorage(t, "B")
	cc, ecc, storC := newTestClientWithStorage(t, "C")
	u1, u2, u3 := uniqueUser("e4a"), uniqueUser("e4b"), uniqueUser("e4c")
	registerUsers(t, u1, u2, u3)
	loginAndSync(t, ca, eca, u1)
	loginAndSync(t, cb, ecb, u2)
	loginAndSync(t, cc, ecc, u3)

	ctx := context.Background()

	// A 建群（成员 u2）→ 两端成员全量落库（新群成员同步走 full 拉取）
	gr, err := ca.Group().CreateGroup(ctx,
		&model.GroupInfo{GroupName: fmt.Sprintf("it-e4-members-%d", time.Now().UnixNano())},
		[]string{u2}, nil, u1, false)
	require.NoError(t, err, "create group")
	gid := gr.GroupID
	waitAny(t, events.EventGroupCreated, eventTimeout, eca, ecb)
	require.Eventually(t, func() bool {
		return hasMember(storA, u1, gid, u1) && hasMember(storA, u1, gid, u2) &&
			hasMember(storB, u2, gid, u1) && hasMember(storB, u2, gid, u2)
	}, storeWaitTimeout, 200*time.Millisecond, "A/B member cache should be fully populated")
	require.Equal(t, int32(model.RoleOwner), memberRole(storB, u2, gid, u1), "u1 should be group owner")
	require.Equal(t, int32(model.RoleMember), memberRole(storB, u2, gid, u2), "u2 should be ordinary member")

	// 邀请 u3 → A/B 成员缓存增量 insert u3；u3 本地成员全量
	require.NoError(t, ca.Group().InviteUserToGroup(ctx, gid, "it invite", []string{u3}, nil), "invite u3")
	ecc.wait(t, events.EventMemberInvited, eventTimeout)
	require.Eventually(t, func() bool {
		return hasMember(storA, u1, gid, u3) && hasMember(storB, u2, gid, u3) &&
			hasMember(storC, u3, gid, u1) && hasMember(storC, u3, gid, u2) && hasMember(storC, u3, gid, u3)
	}, storeWaitTimeout, 200*time.Millisecond, "member caches should insert u3 (C fully populated)")

	// 踢 u3 → A/B 成员缓存增量 delete u3；C 丢失群（成员缓存随群级联清理）
	require.NoError(t, ca.Group().KickGroup(ctx, gid, []string{u3}, "it kick", nil), "kick u3")
	ecc.wait(t, events.EventMemberKicked, eventTimeout)
	require.Eventually(t, func() bool {
		return !hasMember(storA, u1, gid, u3) && !hasMember(storB, u2, gid, u3) &&
			!hasGroup(storC, u3, gid)
	}, storeWaitTimeout, 200*time.Millisecond, "A/B member cache should delete kicked u3; C should lose group")

	// u2 退群 → A 成员缓存增量 delete u2（群对 A 保留）；B 重登后群消失
	require.NoError(t, cb.Group().QuitGroup(ctx, gid, u2), "u2 quit group")
	eca.wait(t, events.EventMemberQuit, eventTimeout)
	require.Eventually(t, func() bool { return !hasMember(storA, u1, gid, u2) },
		storeWaitTimeout, 200*time.Millisecond, "A member cache should delete quitted u2")
	require.True(t, hasGroup(storA, u1, gid), "group should remain for A")
	reloginAndSync(t, cb, ecb, u2, false)
	require.False(t, hasGroup(storB, u2, gid), "B should lose group after quit + relogin incremental")
}

// TestE4_ConversationIncrementalSync E4.5 会话：收发消息 / 置顶 / 已读 → conversations 增量
// + GetSortedConversationList 比对（置顶位、ID 集合、pin 状态、已读清零）
func TestE4_ConversationIncrementalSync(t *testing.T) {
	ca, eca := newTestClient(t, "A")
	cb, ecb, storB := newTestClientWithStorage(t, "B")
	cc, ecc := newTestClient(t, "C")
	u1, u2, u3 := uniqueUser("e4a"), uniqueUser("e4b"), uniqueUser("e4c")
	registerUsers(t, u1, u2, u3)
	loginAndSync(t, ca, eca, u1)
	loginAndSync(t, cb, ecb, u2)
	loginAndSync(t, cc, ecc, u3)

	ctx := context.Background()
	mb := newMsgBox(cb, "B")

	// u1/u3 各发一条给 u2：B 侧生成两个会话（服务端）
	m1 := newTextMsg(u1, u2, "", model.SessionTypeC2C, "e4-5 from u1")
	r1, err := ca.Msg().SendMsg(ctx, m1)
	require.NoError(t, err, "send msg u1->u2")
	b1 := mb.recv(t, r1.ClientMsgID, eventTimeout)
	convID := waitConvID(t, cb, u2, u1)

	m2 := newTextMsg(u3, u2, "", model.SessionTypeC2C, "e4-5 from u3")
	r2, err := cc.Msg().SendMsg(ctx, m2)
	require.NoError(t, err, "send msg u3->u2")
	mb.recv(t, r2.ClientMsgID, eventTimeout)
	convID2 := waitConvID(t, cb, u2, u3)

	// 置顶 u1 会话：用户态 set_conversation（owner=u2）→ 服务端推 1300 → B 会话增量同步落库。
	// 服务端单会话接口是完整 upsert，先取回当前会话再改 isPinned，避免未传字段被清零。
	convs, err := cb.Conv().GetConversations(ctx, u2, []string{convID})
	require.NoError(t, err, "get conversation before set")
	require.Len(t, convs, 1, "target conversation should exist")
	pinned := convs[0]
	pinned.IsPinned = true
	require.NoError(t, cb.Conv().SetConversation(ctx, u2, pinned), "user set_conversation")
	ecb.wait(t, events.EventConversationUpdate, eventTimeout)
	require.Eventually(t, func() bool {
		c1 := findConv(storB, u2, convID)
		c2 := findConv(storB, u2, convID2)
		return c1 != nil && c2 != nil && c1.IsPinned && !c2.IsPinned
	}, storeWaitTimeout, 200*time.Millisecond, "B conversation cache should sync both convs with pin applied")

	// 已读：B 标记 u1 会话已读 → A 收 2200
	require.NoError(t, cb.Msg().MarkConversationAsRead(ctx, convID, b1.Seq, nil), "mark conversation as read")
	eca.wait(t, events.EventMarkAsRead, eventTimeout)

	// GetSortedConversationList 比对：置顶会话排首位，ID 集合与 pin 状态同本地缓存一致
	sorted, err := cb.Conv().GetSortedConversationList(ctx, u2, nil,
		&model.RequestPagination{PageNumber: 1, ShowNumber: 50})
	require.NoError(t, err, "get sorted conversation list")
	require.Len(t, sorted.ConversationElems, 2, "B should have exactly 2 conversations")
	require.Equal(t, convID, sorted.ConversationElems[0].ConversationID, "pinned conversation should sort first")
	require.True(t, sorted.ConversationElems[0].IsPinned, "first elem should be pinned")

	local, err := storB.ListConversations(u2)
	require.NoError(t, err, "list local conversations")
	require.Len(t, local, len(sorted.ConversationElems), "local conversation count should match server")
	for _, e := range sorted.ConversationElems {
		lc := findConv(storB, u2, e.ConversationID)
		require.NotNil(t, lc, "local cache should contain server conversation %s", e.ConversationID)
		require.Equal(t, e.IsPinned, lc.IsPinned, "pin state mismatch for %s", e.ConversationID)
		if e.ConversationID == convID {
			require.Zero(t, e.UnreadCount, "u1 conversation unread should be cleared after mark-as-read")
		}
	}
}

// TestE4_FullResyncAfterClearData E4.6：Logout(true) 清游标 → 重登触发 full 全量回退
// （本地空集 IDHash ≠ 服务端 → 清表重拉分支），数据最终一致
func TestE4_FullResyncAfterClearData(t *testing.T) {
	ca, eca := newTestClient(t, "A")
	cb, ecb, storB := newTestClientWithStorage(t, "B")
	u1, u2, u3 := uniqueUser("e4a"), uniqueUser("e4b"), uniqueUser("e4c")
	registerUsers(t, u1, u2, u3)
	loginAndSync(t, ca, eca, u1)
	loginAndSync(t, cb, ecb, u2)

	ctx := context.Background()

	// 构造数据：好友 u1 / 群（含成员）/ 会话 / 黑名单 u3
	applyFriendship(t, ca, cb, eca, ecb, u1, u2)
	gr, err := ca.Group().CreateGroup(ctx,
		&model.GroupInfo{GroupName: fmt.Sprintf("it-e4-full-%d", time.Now().UnixNano())},
		[]string{u2}, nil, u1, false)
	require.NoError(t, err, "create group")
	gid := gr.GroupID
	m := newTextMsg(u1, u2, "", model.SessionTypeC2C, "e4-6 msg")
	_, err = ca.Msg().SendMsg(ctx, m)
	require.NoError(t, err, "send msg u1->u2")
	convID := waitConvID(t, cb, u2, u1)
	require.NoError(t, cb.Rel().AddBlack(ctx, u2, u3, ""), "B add black u3")

	// 数据就绪（通知触发同步收敛后再清库，保证重登后比对有意义）
	require.Eventually(t, func() bool {
		return hasFriend(storB, u2, u1) && hasGroup(storB, u2, gid) &&
			hasBlack(storB, u2, u3) && findConv(storB, u2, convID) != nil
	}, storeWaitTimeout, 200*time.Millisecond, "B caches should converge before clear")

	// Logout(true)：清数据 + 清游标
	require.NoError(t, cb.Logout(true), "logout clearData=true")
	require.False(t, hasFriend(storB, u2, u1), "friends cleared")
	require.False(t, hasBlack(storB, u2, u3), "blacks cleared")
	require.False(t, hasGroup(storB, u2, gid), "groups cleared")
	require.Nil(t, findConv(storB, u2, convID), "conversations cleared")
	vID, v, err := storB.GetVersion(u2, storage.EntityFriends)
	require.NoError(t, err)
	require.Empty(t, vID, "friend cursor cleared")
	require.Zero(t, v, "friend cursor version cleared")

	// 重登：游标为空 → 服务端 full=true → IDHash 校验（本地空集不等）→ 全量重拉
	loginAndSync(t, cb, ecb, u2)
	require.True(t, hasFriend(storB, u2, u1), "friend re-pulled via full resync")
	require.True(t, hasBlack(storB, u2, u3), "black re-pulled via full resync")
	require.True(t, hasGroup(storB, u2, gid), "group re-pulled via full resync")
	require.True(t, hasMember(storB, u2, gid, u1) && hasMember(storB, u2, gid, u2),
		"group members re-pulled via full resync")
	require.NotNil(t, findConv(storB, u2, convID), "conversation re-pulled via full resync")

	// 游标重建
	vID2, v2, err := storB.GetVersion(u2, storage.EntityFriends)
	require.NoError(t, err)
	require.True(t, vID2 != "" || v2 > 0, "friend cursor should be rebuilt after full resync")
}

// TestE4_CursorMonotonicAndServerConsistent E4.7 游标推进：versionID/version 与
// 服务端响应一致且单调递增（以好友实体为例）
func TestE4_CursorMonotonicAndServerConsistent(t *testing.T) {
	ca, eca := newTestClient(t, "A")
	cb, ecb, storB := newTestClientWithStorage(t, "B")
	cc, ecc := newTestClient(t, "C")
	u1, u2, u3 := uniqueUser("e4a"), uniqueUser("e4b"), uniqueUser("e4c")
	registerUsers(t, u1, u2, u3)
	loginAndSync(t, ca, eca, u1)
	loginAndSync(t, cb, ecb, u2)
	loginAndSync(t, cc, ecc, u3)

	ctx := context.Background()

	// 首登后游标已建立（空集走 IDHash equal 对齐分支）
	_, v0, err := storB.GetVersion(u2, storage.EntityFriends)
	require.NoError(t, err, "baseline friend cursor")

	// 第一次增量：u1-u2 好友 → version 单调递增
	applyFriendship(t, ca, cb, eca, ecb, u1, u2)
	require.Eventually(t, func() bool { return hasFriend(storB, u2, u1) },
		storeWaitTimeout, 200*time.Millisecond, "friend u1 synced")
	vID1, v1, err := storB.GetVersion(u2, storage.EntityFriends)
	require.NoError(t, err)
	require.Greater(t, v1, v0, "version should increase after friend insert")

	// 与服务端一致：以本地游标再拉增量。服务端协议为「游标在头部（无新增变更日志）
	// → full=true + 全量快照」（SDK 收到后走 IDHash 校验，相等则空转），
	// 此处校验游标值与快照内容均与服务端一致
	inc, err := cb.Rel().GetIncrementalFriends(ctx, u2, vID1, v1)
	require.NoError(t, err, "incremental friends with local cursor")
	require.True(t, inc.Full, "cursor at head: server returns full snapshot")
	require.Equal(t, vID1, inc.VersionID, "versionID consistent with server")
	require.Equal(t, v1, inc.Version, "version consistent with server")
	require.Len(t, inc.Insert, 1, "full snapshot should contain exactly one friend")
	require.Equal(t, u1, inc.Insert[0].FriendUserID(), "snapshot friend should be u1")
	require.Empty(t, inc.Delete, "no deletes at cursor head")

	// 第二次增量：u3-u2 好友 → 继续单调递增
	applyFriendship(t, cc, cb, ecc, ecb, u3, u2)
	require.Eventually(t, func() bool { return hasFriend(storB, u2, u3) },
		storeWaitTimeout, 200*time.Millisecond, "friend u3 synced")
	_, v2, err := storB.GetVersion(u2, storage.EntityFriends)
	require.NoError(t, err)
	require.Greater(t, v2, v1, "version should keep increasing")
}
