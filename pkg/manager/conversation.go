package manager

import (
	"context"

	"github.com/PaperMan11/goim-sdk-core/pkg/model"
)

// ConvMgr 会话列表 / 属性 / 增量同步
//
// 实现要点:
//   - 13 个 HTTP 接口
//   - GetIncrementalConversations 需本地 versionID 配合
type ConvMgr interface {
	GetSortedConversationList(ctx context.Context, userID string, conversationIDs []string, pagination *model.RequestPagination) (*model.SortedConversationList, error)
	GetConversation(ctx context.Context, ownerUserID string, conversationID string) (*model.Conversation, error)
	GetConversations(ctx context.Context, ownerUserID string, conversationIDs []string) ([]*model.Conversation, error)
	GetAllConversations(ctx context.Context, ownerUserID string) ([]*model.Conversation, error)
	SetConversations(ctx context.Context, userIDs []string, conversationReq *model.ConversationReq) error
	GetFullConversationIDs(ctx context.Context, userID string, idHash uint64) (*model.FullConversationIDs, error)
	GetIncrementalConversations(ctx context.Context, userID string, versionID string, version uint64) (*model.IncrementalConversations, error)
	DeleteConversations(ctx context.Context, ownerUserID string, needDeleteTime int64, conversationIDs []string) error
	UpdateConversationsByUser(ctx context.Context, userID string, ex string) error
}
