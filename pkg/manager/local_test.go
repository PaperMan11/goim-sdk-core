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
