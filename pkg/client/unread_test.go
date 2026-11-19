package client

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PaperMan11/goim-sdk-core/pkg/logger"
	"github.com/PaperMan11/goim-sdk-core/pkg/model"
	"github.com/PaperMan11/goim-sdk-core/pkg/storage"
)

// TestLocalUnread 本地未读数推导：hasReadSeq（已读最大 seq）+ 未撤回消息计数，
// 消息缓存为空时回落到会话 unreadCount；UnreadTotal 剔除 n_ 通知会话。
func TestLocalUnread(t *testing.T) {
	const owner = "u1"
	stor := storage.NewMemoryStorage()

	require.NoError(t, stor.UpsertConversations(owner, []*model.Conversation{
		{ConversationID: "si_c1", ConversationType: model.SessionTypeC2C},
		{ConversationID: "si_c2", ConversationType: model.SessionTypeC2C, UnreadCount: 5},
		{ConversationID: "n_a_b", ConversationType: model.SessionTypeC2C, UnreadCount: 1},
	}))
	require.NoError(t, stor.UpsertMsgs(owner, []*model.MsgData{
		{ClientMsgID: "m1", ConversationID: "si_c1", Seq: 1, IsRead: true},
		{ClientMsgID: "m2", ConversationID: "si_c1", Seq: 2},
		{ClientMsgID: "m3", ConversationID: "si_c1", Seq: 3},
		{ClientMsgID: "m4", ConversationID: "si_c1", Seq: 4, Status: model.MsgStatusRevoked},
		{ClientMsgID: "m5", ConversationID: "n_a_b", Seq: 1},
	}))

	c := &DefaultClient{storage: stor, userID: owner, logger: &logger.NopLogger{}}

	// si_c1：seq 2/3 未读，seq 1 已读，seq 4 已撤回不计
	n, err := c.UnreadCount("si_c1")
	require.NoError(t, err)
	require.Equal(t, int64(2), n)

	// si_c2：无消息缓存 → 回落到会话 unreadCount
	n, err = c.UnreadCount("si_c2")
	require.NoError(t, err)
	require.Equal(t, int64(5), n)

	// 通知会话 n_a_b 不参与业务未读
	total, err := c.UnreadTotal()
	require.NoError(t, err)
	require.Equal(t, int64(7), total)

	// 非法参数 / 未登录
	_, err = c.UnreadCount("")
	require.Error(t, err)
	_, err = (&DefaultClient{storage: stor}).UnreadTotal()
	require.Error(t, err)
}
