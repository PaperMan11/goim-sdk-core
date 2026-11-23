//go:build !(js && wasm)

package storage

// Local Data API（P1）存储层单测：分页 / 排序 / 单条直读在 Memory 与 SQLite 上行为一致。

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PaperMan11/goim-sdk-core/pkg/model"
)

func newLocalStorages(t *testing.T) map[string]Storage {
	t.Helper()
	return map[string]Storage{
		"memory": NewMemoryStorage(),
		"sqlite": newSQLiteForTest(t),
	}
}

func convIDs(items []*model.Conversation) []string {
	out := make([]string, 0, len(items))
	for _, c := range items {
		out = append(out, c.ConversationID)
	}
	return out
}

func TestStorageLocalConversations(t *testing.T) {
	for name, stor := range newLocalStorages(t) {
		t.Run(name, func(t *testing.T) {
			const owner = "u1"
			require.NoError(t, stor.UpsertConversations(owner, []*model.Conversation{
				{ConversationID: "c1", LatestMsgSendTime: 100},
				{ConversationID: "c2", IsPinned: true, LatestMsgSendTime: 50},
				{ConversationID: "c3", LatestMsgSendTime: 200},
			}))

			// 置顶优先 + LatestMsgSendTime 倒序
			page, err := stor.ListConversationsPage(owner, 0, 2)
			require.NoError(t, err)
			require.Equal(t, []string{"c2", "c3"}, convIDs(page.Items))
			require.Equal(t, int64(3), page.Total)
			require.True(t, page.HasMore)

			page, err = stor.ListConversationsPage(owner, 2, 2)
			require.NoError(t, err)
			require.Equal(t, []string{"c1"}, convIDs(page.Items))
			require.False(t, page.HasMore)

			// offset 越界：空页
			page, err = stor.ListConversationsPage(owner, 10, 2)
			require.NoError(t, err)
			require.Empty(t, page.Items)
			require.Equal(t, int64(3), page.Total)
			require.False(t, page.HasMore)

			// 批量直读：顺序与入参一致，缺失跳过
			batch, err := stor.GetConversations(owner, []string{"c3", "missing", "c1"})
			require.NoError(t, err)
			require.Equal(t, []string{"c3", "c1"}, convIDs(batch))

			// owner 隔离
			page, err = stor.ListConversationsPage("u2", 0, 2)
			require.NoError(t, err)
			require.Empty(t, page.Items)
			require.Zero(t, page.Total)
		})
	}
}

func TestStorageLocalMsgs(t *testing.T) {
	for name, stor := range newLocalStorages(t) {
		t.Run(name, func(t *testing.T) {
			const owner, conv = "u1", "si_c1"
			require.NoError(t, stor.UpsertMsgs(owner, []*model.MsgData{
				{ClientMsgID: "m1", ConversationID: conv, Seq: 1},
				{ClientMsgID: "m2", ConversationID: conv, Seq: 2},
				{ClientMsgID: "m3", ConversationID: conv, Seq: 3},
				{ClientMsgID: "m4", ConversationID: conv, Seq: 4},
				{ClientMsgID: "m5", ConversationID: conv, Seq: 5},
				{ClientMsgID: "other", ConversationID: "si_c2", Seq: 6},
			}))

			// 降序：beforeSeq<=0 取最新
			page, err := stor.ListMsgsPage(owner, conv, 0, 2, true)
			require.NoError(t, err)
			require.Equal(t, []int64{5, 4}, msgSeqs(page.Items))
			require.Equal(t, int64(4), page.OldestSeq)
			require.Equal(t, int64(5), page.NewestSeq)
			require.True(t, page.HasMore)

			// 降序闭区间：seq<=4
			page, err = stor.ListMsgsPage(owner, conv, 4, 2, true)
			require.NoError(t, err)
			require.Equal(t, []int64{4, 3}, msgSeqs(page.Items))
			require.True(t, page.HasMore)

			// 降序到尾
			page, err = stor.ListMsgsPage(owner, conv, 1, 2, true)
			require.NoError(t, err)
			require.Equal(t, []int64{1}, msgSeqs(page.Items))
			require.False(t, page.HasMore)

			// 升序：afterSeq<=0 从头
			page, err = stor.ListMsgsPage(owner, conv, 0, 2, false)
			require.NoError(t, err)
			require.Equal(t, []int64{1, 2}, msgSeqs(page.Items))
			require.True(t, page.HasMore)

			// 升序开区间：seq>2
			page, err = stor.ListMsgsPage(owner, conv, 2, 2, false)
			require.NoError(t, err)
			require.Equal(t, []int64{3, 4}, msgSeqs(page.Items))

			// 单条直读
			m, err := stor.GetMsg(owner, conv, 3)
			require.NoError(t, err)
			require.NotNil(t, m)
			require.Equal(t, "m3", m.ClientMsgID)

			m, err = stor.GetMsg(owner, conv, 99)
			require.NoError(t, err)
			require.Nil(t, m)

			m, err = stor.GetMsgByClientID(owner, conv, "m2")
			require.NoError(t, err)
			require.NotNil(t, m)
			require.Equal(t, int64(2), m.Seq)

			m, err = stor.GetMsgByClientID(owner, conv, "missing")
			require.NoError(t, err)
			require.Nil(t, m)

			// owner 隔离
			m, err = stor.GetMsg("u2", conv, 1)
			require.NoError(t, err)
			require.Nil(t, m)
		})
	}
}

func msgSeqs(items []*model.MsgData) []int64 {
	out := make([]int64, 0, len(items))
	for _, m := range items {
		out = append(out, m.Seq)
	}
	return out
}

func TestStorageLocalFriendsAndBlacks(t *testing.T) {
	for name, stor := range newLocalStorages(t) {
		t.Run(name, func(t *testing.T) {
			const owner = "u1"
			require.NoError(t, stor.UpsertFriends(owner, []*model.FriendInfo{
				{FriendUser: &model.FriendUserInfo{UserID: "u3"}, IsPinned: true},
				{FriendUser: &model.FriendUserInfo{UserID: "u1"}},
				{FriendUser: &model.FriendUserInfo{UserID: "u2"}},
			}))

			// 置顶优先 + userID 升序
			page, err := stor.ListFriendsPage(owner, 0, 2)
			require.NoError(t, err)
			require.Len(t, page.Items, 2)
			require.Equal(t, "u3", page.Items[0].FriendUserID())
			require.Equal(t, "u1", page.Items[1].FriendUserID())
			require.Equal(t, int64(3), page.Total)
			require.True(t, page.HasMore)

			page, err = stor.ListFriendsPage(owner, 2, 2)
			require.NoError(t, err)
			require.Equal(t, []string{"u2"}, friendIDs(page.Items))
			require.False(t, page.HasMore)

			f, err := stor.GetFriend(owner, "u1")
			require.NoError(t, err)
			require.NotNil(t, f)
			f, err = stor.GetFriend(owner, "missing")
			require.NoError(t, err)
			require.Nil(t, f)

			// 黑名单：blackUserID 升序
			require.NoError(t, stor.UpsertBlacks(owner, []*model.BlackInfo{
				{BlackUserInfo: &model.PublicUserInfo{UserID: "b3"}},
				{BlackUserInfo: &model.PublicUserInfo{UserID: "b1"}},
				{BlackUserInfo: &model.PublicUserInfo{UserID: "b2"}},
			}))
			bpage, err := stor.ListBlacksPage(owner, 2, 1)
			require.NoError(t, err)
			require.Equal(t, []string{"b3"}, blackIDs(bpage.Items))
			require.Equal(t, int64(3), bpage.Total)
			require.False(t, bpage.HasMore)

			b, err := stor.GetBlack(owner, "b2")
			require.NoError(t, err)
			require.NotNil(t, b)
			b, err = stor.GetBlack(owner, "missing")
			require.NoError(t, err)
			require.Nil(t, b)
		})
	}
}

func friendIDs(items []*model.FriendInfo) []string {
	out := make([]string, 0, len(items))
	for _, f := range items {
		out = append(out, f.FriendUserID())
	}
	return out
}

func blackIDs(items []*model.BlackInfo) []string {
	out := make([]string, 0, len(items))
	for _, b := range items {
		out = append(out, b.BlackUserID())
	}
	return out
}

func TestStorageLocalGroupsAndMembers(t *testing.T) {
	for name, stor := range newLocalStorages(t) {
		t.Run(name, func(t *testing.T) {
			const owner, groupID = "u1", "g1"
			require.NoError(t, stor.UpsertGroups(owner, []*model.GroupInfo{
				{GroupID: "g3", GroupName: "c"},
				{GroupID: "g1", GroupName: "a"},
				{GroupID: "g2", GroupName: "b"},
			}))

			// 群名升序
			page, err := stor.ListGroupsPage(owner, 0, 2)
			require.NoError(t, err)
			require.Equal(t, []string{"g1", "g2"}, groupIDs(page.Items))
			require.Equal(t, int64(3), page.Total)
			require.True(t, page.HasMore)

			g, err := stor.GetGroup(owner, "g2")
			require.NoError(t, err)
			require.NotNil(t, g)
			require.Equal(t, "b", g.GroupName)
			g, err = stor.GetGroup(owner, "missing")
			require.NoError(t, err)
			require.Nil(t, g)

			// 群成员：角色高优先 → JoinTime 升序 → userID
			require.NoError(t, stor.UpsertGroupMembers(owner, groupID, []*model.GroupMemberFullInfo{
				{GroupID: groupID, UserID: "owner", RoleLevel: 3, JoinTime: 50},
				{GroupID: groupID, UserID: "m2", RoleLevel: 1, JoinTime: 20},
				{GroupID: groupID, UserID: "m1", RoleLevel: 1, JoinTime: 10},
				{GroupID: groupID, UserID: "admin", RoleLevel: 2, JoinTime: 90},
			}))
			require.NoError(t, stor.UpsertGroupMembers(owner, "g2", []*model.GroupMemberFullInfo{
				{GroupID: "g2", UserID: "other", RoleLevel: 3},
			}))

			mpage, err := stor.ListGroupMembersPage(owner, groupID, 0, 2)
			require.NoError(t, err)
			require.Equal(t, []string{"owner", "admin"}, memberIDs(mpage.Items))
			require.Equal(t, int64(4), mpage.Total)
			require.True(t, mpage.HasMore)

			mpage, err = stor.ListGroupMembersPage(owner, groupID, 2, 2)
			require.NoError(t, err)
			require.Equal(t, []string{"m1", "m2"}, memberIDs(mpage.Items))
			require.False(t, mpage.HasMore)

			m, err := stor.GetGroupMember(owner, groupID, "admin")
			require.NoError(t, err)
			require.NotNil(t, m)
			require.Equal(t, int32(2), m.RoleLevel)
			m, err = stor.GetGroupMember(owner, groupID, "missing")
			require.NoError(t, err)
			require.Nil(t, m)
		})
	}
}

func groupIDs(items []*model.GroupInfo) []string {
	out := make([]string, 0, len(items))
	for _, g := range items {
		out = append(out, g.GroupID)
	}
	return out
}

func memberIDs(items []*model.GroupMemberFullInfo) []string {
	out := make([]string, 0, len(items))
	for _, m := range items {
		out = append(out, m.UserID)
	}
	return out
}
