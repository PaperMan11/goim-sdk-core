package manager

// Local Data API（P1）门面单测：分页默认值 / 消息游标语义 / seq 快照 / 未登录与 owner 隔离。

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PaperMan11/goim-sdk-core/pkg/model"
	"github.com/PaperMan11/goim-sdk-core/pkg/storage"
)

func TestLocalDataMgr_NotLoggedIn(t *testing.T) {
	stor := storage.NewMemoryStorage()

	m := NewLocalDataMgr(stor, func() string { return "" })
	_, err := m.ListConversations(0, 10)
	require.ErrorIs(t, err, ErrNotLoggedIn)

	m = NewLocalDataMgr(stor, nil)
	_, err = m.UnreadTotal()
	require.ErrorIs(t, err, ErrNotLoggedIn)
	_, err = m.ListMsgs("si_c1", 0, 10)
	require.ErrorIs(t, err, ErrNotLoggedIn)
}

func TestLocalDataMgr_PageDefaults(t *testing.T) {
	const owner = "u1"
	stor := storage.NewMemoryStorage()
	convs := make([]*model.Conversation, 0, 60)
	for i := 0; i < 60; i++ {
		convs = append(convs, &model.Conversation{
			ConversationID:    fmt.Sprintf("c%02d", i),
			LatestMsgSendTime: int64(1000 - i),
		})
	}
	require.NoError(t, stor.UpsertConversations(owner, convs))

	m := NewLocalDataMgr(stor, func() string { return owner })

	// limit<=0 → 默认 50；offset<0 → 0
	page, err := m.ListConversations(-1, 0)
	require.NoError(t, err)
	require.Len(t, page.Items, int(DefaultLocalPageSize))
	require.Equal(t, int64(60), page.Total)
	require.True(t, page.HasMore)

	// limit 超过上限 → 200（数据不足时不截断可见）
	page, err = m.ListConversations(0, MaxLocalPageSize+100)
	require.NoError(t, err)
	require.Len(t, page.Items, 60)
	require.False(t, page.HasMore)
}

func TestLocalDataMgr_Msgs(t *testing.T) {
	const owner, conv = "u1", "si_c1"
	stor := storage.NewMemoryStorage()
	require.NoError(t, stor.UpsertMsgs(owner, []*model.MsgData{
		{ClientMsgID: "m1", ConversationID: conv, Seq: 1},
		{ClientMsgID: "m2", ConversationID: conv, Seq: 2},
		{ClientMsgID: "m3", ConversationID: conv, Seq: 3},
		{ClientMsgID: "m4", ConversationID: conv, Seq: 4},
		{ClientMsgID: "m5", ConversationID: conv, Seq: 5},
	}))

	m := NewLocalDataMgr(stor, func() string { return owner })

	// 默认降序：limit<=0 → 默认 30
	page, err := m.ListMsgs(conv, 0, 0)
	require.NoError(t, err)
	require.Equal(t, []int64{5, 4, 3, 2, 1}, localSeqs(page.Items))
	require.False(t, page.HasMore)

	// 继续向更早翻页：传 page.OldestSeq-1
	page, err = m.ListMsgs(conv, 0, 2)
	require.NoError(t, err)
	require.Equal(t, []int64{5, 4}, localSeqs(page.Items))
	require.True(t, page.HasMore)
	page, err = m.ListMsgs(conv, page.OldestSeq-1, 2)
	require.NoError(t, err)
	require.Equal(t, []int64{3, 2}, localSeqs(page.Items))
	require.True(t, page.HasMore)

	// 升序
	page, err = m.ListMsgsAfter(conv, 0, 2)
	require.NoError(t, err)
	require.Equal(t, []int64{1, 2}, localSeqs(page.Items))
	page, err = m.ListMsgsAfter(conv, page.NewestSeq, 2)
	require.NoError(t, err)
	require.Equal(t, []int64{3, 4}, localSeqs(page.Items))

	// 单条直读：不存在 (nil, nil)
	msg, err := m.GetMsg(conv, 3)
	require.NoError(t, err)
	require.NotNil(t, msg)
	msg, err = m.GetMsg(conv, 99)
	require.NoError(t, err)
	require.Nil(t, msg)
	msg, err = m.GetMsgByClientID(conv, "m2")
	require.NoError(t, err)
	require.NotNil(t, msg)
	msg, err = m.GetMsgByClientID(conv, "missing")
	require.NoError(t, err)
	require.Nil(t, msg)

	// 非法参数
	_, err = m.ListMsgs("", 0, 10)
	require.Error(t, err)
	_, err = m.GetMsg(conv, 0)
	require.Error(t, err)
}

func localSeqs(items []*model.MsgData) []int64 {
	out := make([]int64, 0, len(items))
	for _, m := range items {
		out = append(out, m.Seq)
	}
	return out
}

func TestLocalDataMgr_ConversationSeqs(t *testing.T) {
	const owner, conv = "u1", "si_c1"
	stor := storage.NewMemoryStorage()
	require.NoError(t, stor.SetMaxSeqs(owner, map[string]int64{conv: 10}, map[string]int64{conv: 1}))
	require.NoError(t, stor.UpsertMsgs(owner, []*model.MsgData{
		{ClientMsgID: "m2", ConversationID: conv, Seq: 2},
		{ClientMsgID: "m3", ConversationID: conv, Seq: 3, IsRead: true},
		{ClientMsgID: "m4", ConversationID: conv, Seq: 4},
		{ClientMsgID: "m5", ConversationID: conv, Seq: 5},
	}))

	m := NewLocalDataMgr(stor, func() string { return owner })

	seqs, err := m.GetConversationSeqs(conv)
	require.NoError(t, err)
	require.Equal(t, conv, seqs.ConversationID)
	require.Equal(t, int64(2), seqs.LocalMinSeq)
	require.Equal(t, int64(5), seqs.LocalMaxSeq)
	require.Equal(t, int64(3), seqs.HasReadSeq)
	require.Equal(t, int64(1), seqs.ServerMinSeq)
	require.Equal(t, int64(10), seqs.ServerMaxSeq)

	// 无缓存会话：全零
	seqs, err = m.GetConversationSeqs("si_missing")
	require.NoError(t, err)
	require.Zero(t, seqs.LocalMaxSeq)
	require.Zero(t, seqs.ServerMaxSeq)
}

func TestLocalDataMgr_UnreadAndRelations(t *testing.T) {
	const owner = "u1"
	stor := storage.NewMemoryStorage()
	require.NoError(t, stor.UpsertConversations(owner, []*model.Conversation{
		{ConversationID: "si_c1"},
		{ConversationID: "si_c2", UnreadCount: 5},
		{ConversationID: "n_a_b", UnreadCount: 1},
	}))
	require.NoError(t, stor.UpsertMsgs(owner, []*model.MsgData{
		{ClientMsgID: "m1", ConversationID: "si_c1", Seq: 1, IsRead: true},
		{ClientMsgID: "m2", ConversationID: "si_c1", Seq: 2},
		{ClientMsgID: "m3", ConversationID: "si_c1", Seq: 3},
		{ClientMsgID: "m4", ConversationID: "si_c1", Seq: 4, Status: model.MsgStatusRevoked},
	}))
	require.NoError(t, stor.UpsertFriends(owner, []*model.FriendInfo{
		{FriendUser: &model.FriendUserInfo{UserID: "u2"}},
	}))
	require.NoError(t, stor.UpsertBlacks(owner, []*model.BlackInfo{
		{BlackUserInfo: &model.PublicUserInfo{UserID: "bad"}},
	}))
	require.NoError(t, stor.UpsertGroups(owner, []*model.GroupInfo{{GroupID: "g1", GroupName: "g"}}))

	m := NewLocalDataMgr(stor, func() string { return owner })

	n, err := m.UnreadCount("si_c1")
	require.NoError(t, err)
	require.Equal(t, int64(2), n)
	n, err = m.UnreadCount("si_c2")
	require.NoError(t, err)
	require.Equal(t, int64(5), n)
	total, err := m.UnreadTotal()
	require.NoError(t, err)
	require.Equal(t, int64(7), total)

	blocked, err := m.IsBlocked("bad")
	require.NoError(t, err)
	require.True(t, blocked)
	blocked, err = m.IsBlocked("u2")
	require.NoError(t, err)
	require.False(t, blocked)

	f, err := m.GetFriend("u2")
	require.NoError(t, err)
	require.NotNil(t, f)
	f, err = m.GetFriend("missing")
	require.NoError(t, err)
	require.Nil(t, f)

	g, err := m.GetGroup("g1")
	require.NoError(t, err)
	require.NotNil(t, g)
	g, err = m.GetGroup("missing")
	require.NoError(t, err)
	require.Nil(t, g)
}

// TestLocalDataMgr_UnreadUsesServerReadSeq 未读以服务端已读游标为基准：
// 换机/重装后本地没有 IsRead 标记也能算出正确未读
func TestLocalDataMgr_UnreadUsesServerReadSeq(t *testing.T) {
	const owner = "u1"
	stor := storage.NewMemoryStorage()
	require.NoError(t, stor.UpsertMsgs(owner, []*model.MsgData{
		{ClientMsgID: "m1", ConversationID: "si_c1", Seq: 1, SendID: "u2"},
		{ClientMsgID: "m2", ConversationID: "si_c1", Seq: 2, SendID: "u2"},
		{ClientMsgID: "m3", ConversationID: "si_c1", Seq: 3, SendID: "u2"},
	}))

	m := NewLocalDataMgr(stor, func() string { return owner })
	n, err := m.UnreadCount("si_c1")
	require.NoError(t, err)
	require.Equal(t, int64(3), n)

	// 服务端记录已读至 seq2（如在另一台设备读过），本地消息全部为未读
	require.NoError(t, stor.SetVersion(owner, fmt.Sprintf(storage.EntityReadSeqFmt, "si_c1"), "", 2))
	n, err = m.UnreadCount("si_c1")
	require.NoError(t, err)
	require.Equal(t, int64(1), n)
}

func TestLocalDataMgr_SearchAndStats(t *testing.T) {
	const owner, conv = "u1", "si_c1"
	stor := storage.NewMemoryStorage()
	require.NoError(t, stor.UpsertConversations(owner, []*model.Conversation{
		{ConversationID: conv, LatestMsgSendTime: 100, MsgInfo: &model.MsgInfo{Content: "hello"}},
		{ConversationID: "si_c2", LatestMsgSendTime: 50},
	}))
	require.NoError(t, stor.UpsertMsgs(owner, []*model.MsgData{
		{ClientMsgID: "m1", ConversationID: conv, Seq: 1, Content: "hello"},
		{ClientMsgID: "m2", ConversationID: conv, Seq: 2, Content: "hello world"},
		{ClientMsgID: "m3", ConversationID: conv, Seq: 3, Content: "other"},
		{ClientMsgID: "m4", ConversationID: "si_c2", Seq: 4, Content: "hello"},
	}))
	require.NoError(t, stor.UpsertFriends(owner, []*model.FriendInfo{
		{FriendUser: &model.FriendUserInfo{UserID: "f1", Nickname: "Alice"}},
	}))
	require.NoError(t, stor.UpsertBlacks(owner, []*model.BlackInfo{
		{BlackUserInfo: &model.PublicUserInfo{UserID: "bad"}},
	}))
	require.NoError(t, stor.UpsertGroups(owner, []*model.GroupInfo{{GroupID: "g1", GroupName: "Alpha"}}))
	require.NoError(t, stor.UpsertGroupMembers(owner, "g1", []*model.GroupMemberFullInfo{
		{GroupID: "g1", UserID: "m1"},
		{GroupID: "g1", UserID: "m2"},
	}))

	m := NewLocalDataMgr(stor, func() string { return owner })

	// 空关键字 → 参数错误
	_, err := m.SearchConversations("  ", 10)
	require.Error(t, err)
	_, err = m.SearchMsgs(conv, "", 10)
	require.Error(t, err)
	_, err = m.SearchMsgs("", "hello", 10)
	require.Error(t, err)
	_, err = m.SearchFriends("", 10)
	require.Error(t, err)
	_, err = m.SearchGroups(" ", 10)
	require.Error(t, err)

	// 搜索：limit<=0 走默认值（实体 50 / 消息 30），结果正确
	cpage, err := m.SearchConversations("hello", 0)
	require.NoError(t, err)
	require.Len(t, cpage.Items, 1)
	require.Equal(t, conv, cpage.Items[0].ConversationID)

	mpage, err := m.SearchMsgs(conv, "hello", 0)
	require.NoError(t, err)
	require.Equal(t, []int64{2, 1}, localSeqs(mpage.Items))
	require.False(t, mpage.HasMore)

	fpage, err := m.SearchFriends("alice", 0)
	require.NoError(t, err)
	require.Len(t, fpage.Items, 1)
	gpage, err := m.SearchGroups("alp", 0)
	require.NoError(t, err)
	require.Len(t, gpage.Items, 1)

	// Stats：消息/群成员为全量口径
	st, err := m.Stats()
	require.NoError(t, err)
	require.Equal(t, int64(2), st.Conversations)
	require.Equal(t, int64(4), st.Messages)
	require.Equal(t, int64(1), st.Friends)
	require.Equal(t, int64(1), st.Blacks)
	require.Equal(t, int64(1), st.Groups)
	require.Equal(t, int64(2), st.GroupMembers)

	// 未登录 → ErrNotLoggedIn
	m2 := NewLocalDataMgr(stor, func() string { return "" })
	_, err = m2.SearchConversations("hello", 10)
	require.ErrorIs(t, err, ErrNotLoggedIn)
	_, err = m2.Stats()
	require.ErrorIs(t, err, ErrNotLoggedIn)
}

func TestLocalDataMgr_PurgeAndClear(t *testing.T) {
	const owner, conv1, conv2 = "u1", "si_c1", "si_c2"
	stor := storage.NewMemoryStorage()
	require.NoError(t, stor.SetMaxSeqs(owner, map[string]int64{conv1: 10}, map[string]int64{conv1: 1}))
	require.NoError(t, stor.UpsertConversations(owner, []*model.Conversation{
		{ConversationID: conv1}, {ConversationID: conv2},
	}))
	require.NoError(t, stor.UpsertMsgs(owner, []*model.MsgData{
		{ClientMsgID: "m1", ConversationID: conv1, Seq: 1},
		{ClientMsgID: "m2", ConversationID: conv1, Seq: 2},
		{ClientMsgID: "m3", ConversationID: conv1, Seq: 3},
		{ClientMsgID: "m4", ConversationID: conv1, Seq: 4},
		{ClientMsgID: "m5", ConversationID: conv1, Seq: 5},
		{ClientMsgID: "m6", ConversationID: conv1, Seq: 6},
		{ClientMsgID: "m7", ConversationID: conv1, Seq: 7},
		{ClientMsgID: "m8", ConversationID: conv1, Seq: 8},
		{ClientMsgID: "m9", ConversationID: conv1, Seq: 9},
		{ClientMsgID: "m10", ConversationID: conv1, Seq: 10},
		{ClientMsgID: "m11", ConversationID: conv1, Seq: 11},
		{ClientMsgID: "m12", ConversationID: conv1, Seq: 12},
		{ClientMsgID: "c2m1", ConversationID: conv2, Seq: 1},
		{ClientMsgID: "c2m2", ConversationID: conv2, Seq: 2},
		{ClientMsgID: "c2m3", ConversationID: conv2, Seq: 3},
	}))
	require.NoError(t, stor.UpsertFriends(owner, []*model.FriendInfo{{FriendUser: &model.FriendUserInfo{UserID: "f1"}}}))
	require.NoError(t, stor.UpsertBlacks(owner, []*model.BlackInfo{{BlackUserInfo: &model.PublicUserInfo{UserID: "bad"}}}))
	require.NoError(t, stor.UpsertGroups(owner, []*model.GroupInfo{{GroupID: "g1"}}))
	require.NoError(t, stor.UpsertGroupMembers(owner, "g1", []*model.GroupMemberFullInfo{{GroupID: "g1", UserID: "m1"}}))
	require.NoError(t, stor.SetMeta("device", "d1"))

	m := NewLocalDataMgr(stor, func() string { return owner })

	// 参数校验 / 未登录
	_, err := m.PurgeMsgs("", 5)
	require.Error(t, err)
	_, err = m.PurgeMsgs(conv1, 0)
	require.Error(t, err)
	_, err = m.PurgeMsgs(conv1, -1)
	require.Error(t, err)
	nm := NewLocalDataMgr(stor, nil)
	_, err = nm.PurgeMsgs(conv1, 5)
	require.ErrorIs(t, err, ErrNotLoggedIn)
	require.ErrorIs(t, nm.ClearLocalData(), ErrNotLoggedIn)

	// 删除 seq<=5 → 5 条；floor=5
	n, err := m.PurgeMsgs(conv1, 5)
	require.NoError(t, err)
	require.Equal(t, int64(5), n)
	floor, err := stor.GetMsgFloor(owner, conv1)
	require.NoError(t, err)
	require.Equal(t, int64(5), floor)

	// floor 只增不减：再次清到 3 不删消息、floor 保持 5
	n, err = m.PurgeMsgs(conv1, 3)
	require.NoError(t, err)
	require.Zero(t, n)
	floor, err = stor.GetMsgFloor(owner, conv1)
	require.NoError(t, err)
	require.Equal(t, int64(5), floor)

	// 继续清到 10：删除 6..10（5 条），floor=10
	n, err = m.PurgeMsgs(conv1, 10)
	require.NoError(t, err)
	require.Equal(t, int64(5), n)
	floor, _ = stor.GetMsgFloor(owner, conv1)
	require.Equal(t, int64(10), floor)

	// 清空全部：floor 取本地最新 seq（conv1=12，conv2=3），实体与消息归零，SDK meta 保留
	require.NoError(t, m.ClearLocalData())

	st, err := m.Stats()
	require.NoError(t, err)
	require.Equal(t, &model.LocalDataStats{}, st)

	floor, err = stor.GetMsgFloor(owner, conv1)
	require.NoError(t, err)
	require.Equal(t, int64(12), floor)
	floor, err = stor.GetMsgFloor(owner, conv2)
	require.NoError(t, err)
	require.Equal(t, int64(3), floor)

	meta, err := stor.GetMeta("device")
	require.NoError(t, err)
	require.Equal(t, "d1", meta)

	// owner 隔离：其他账号数据未受影响
	require.NoError(t, stor.UpsertConversations("u2", []*model.Conversation{{ConversationID: "c9"}}))
	require.NoError(t, stor.UpsertMsgs("u2", []*model.MsgData{{ClientMsgID: "x", ConversationID: "c9", Seq: 1}}))
	require.NoError(t, m.ClearLocalData())
	convs, err := stor.ListConversations("u2")
	require.NoError(t, err)
	require.Len(t, convs, 1)
	require.Equal(t, int64(1), mustCountMsgs(t, stor, "u2", "c9"))
}

func mustCountMsgs(t *testing.T, stor storage.Storage, owner, conv string) int64 {
	t.Helper()
	n, err := stor.CountMsgs(owner, conv)
	require.NoError(t, err)
	return n
}

func TestLocalDataMgr_OwnerIsolation(t *testing.T) {
	stor := storage.NewMemoryStorage()
	require.NoError(t, stor.UpsertConversations("u1", []*model.Conversation{
		{ConversationID: "c1", LatestMsgSendTime: 100},
	}))

	owner := "u1"
	m := NewLocalDataMgr(stor, func() string { return owner })

	page, err := m.ListConversations(0, 10)
	require.NoError(t, err)
	require.Len(t, page.Items, 1)

	owner = "u2"
	page, err = m.ListConversations(0, 10)
	require.NoError(t, err)
	require.Empty(t, page.Items)

	conv, err := m.GetConversation("c1")
	require.NoError(t, err)
	require.Nil(t, conv)
}
