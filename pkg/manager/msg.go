package manager

import (
	"context"

	"github.com/PaperMan11/goim-sdk-core/pkg/model"
)

// MsgMgr 消息发送 / 拉取 / 撤回 / 已读 / 搜索
//
// 实现要点:
//   - SendMsg / SendSimpleMsg / BatchSendMsg 走 WS (WSSendMsg) + 同步等待 OperationID 匹配
//   - PullMsgBySeq / NewestSeq 走 WS (WSPullMsg / WSGetNewestSeq)
//   - RevokeMsg / MarkMsgsAsRead / MarkConversationAsRead 走 HTTP
//   - PushHandler(2001) 注册到 MessageRouter，解析 proto 后派发事件
//   - SeqSyncEngine 需要本地 Storage 配合（Phase 4 实现）
type MsgMgr interface {
	SendMsg(ctx context.Context, msg *model.MsgData) (*model.SendMsgResp, error)
	SendSimpleMsg(ctx context.Context, msg *model.MsgData) (*model.SendMsgResp, error)
	BatchSendMsg(ctx context.Context, recvIDList []string, msg *model.MsgData) (*model.SendMsgResp, error)
	NewestSeq(ctx context.Context) (maxSeqs, minSeqs map[string]int64, err error)
	PullMsgBySeq(ctx context.Context, userID string, seqRanges []*model.SeqRange, order int32) (map[string]*model.PullMsgsResult, error)
	RevokeMsg(ctx context.Context, conversationID string, seq int64) error
	MarkMsgsAsRead(ctx context.Context, conversationID string, seqs []int64) error
	MarkConversationAsRead(ctx context.Context, conversationID string, hasReadSeq int64, seqs []int64) error
	GetConversationsHasReadAndMaxSeq(ctx context.Context, conversationIDs []string, returnPinned bool) (*model.SeqInfo, error)
	ClearConversationMsg(ctx context.Context, conversationIDs []string, syncOpt *model.DeleteSyncOpt) error
	DeleteMsgs(ctx context.Context, conversationID string, seqs []int64, syncOpt *model.DeleteSyncOpt) error
	DeleteMsgPhysicalBySeq(ctx context.Context, conversationID string, seqs []int64) error
	DeleteMsgPhysical(ctx context.Context, conversationIDs []string, timestamp int64) error
	SearchMsg(ctx context.Context, sendID, recvID string, contentType int32, sendTime int64, sessionType int32, pagination *model.RequestPagination) (*model.SearchMsgResult, error)
	CheckMsgIsSendSuccess(ctx context.Context, clientMsgID string) (*model.MsgSendStatus, error)
}
