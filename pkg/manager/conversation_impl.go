package manager

import (
	"context"
	"fmt"

	"github.com/PaperMan11/goim-sdk-core/pkg/events"
	"github.com/PaperMan11/goim-sdk-core/pkg/logger"
	"github.com/PaperMan11/goim-sdk-core/pkg/model"
	"github.com/PaperMan11/goim-sdk-core/pkg/transport"
	"github.com/PaperMan11/goim-sdk-core/pkg/utils/eventbus"
	"github.com/PaperMan11/goim-sdk-core/pkg/utils/timex"
)

// DefaultConvMgr ConvMgr 默认实现（纯 HTTP）
type DefaultConvMgr struct {
	http   transport.HTTPClient
	bus    *eventbus.EventBus
	logger logger.Logger
}

// NewConvMgr 构造 ConvMgr
func NewConvMgr(http transport.HTTPClient, bus *eventbus.EventBus, l logger.Logger) *DefaultConvMgr {
	return &DefaultConvMgr{http: http, bus: bus, logger: l}
}

func (m *DefaultConvMgr) GetSortedConversationList(ctx context.Context, userID string, conversationIDs []string, pagination *model.RequestPagination) (*model.SortedConversationList, error) {
	req := struct {
		UserID          string                   `json:"userID"`
		ConversationIDs []string                 `json:"conversationIDs"`
		Pagination      *model.RequestPagination `json:"pagination"`
	}{UserID: userID, ConversationIDs: conversationIDs, Pagination: pagination}
	var resp model.SortedConversationList
	if err := m.http.Post(ctx, "/conversation/get_sorted_conversation_list", req, &resp); err != nil {
		return nil, fmt.Errorf("get sorted conversation list: %w", err)
	}
	return &resp, nil
}

func (m *DefaultConvMgr) GetConversation(ctx context.Context, conversationID string) (*model.Conversation, error) {
	req := struct {
		ConversationID string `json:"conversationID"`
	}{ConversationID: conversationID}
	var resp model.Conversation
	if err := m.http.Post(ctx, "/conversation/get_conversation", req, &resp); err != nil {
		return nil, fmt.Errorf("get conversation: %w", err)
	}
	return &resp, nil
}

func (m *DefaultConvMgr) GetConversations(ctx context.Context, conversationIDs []string) ([]*model.Conversation, error) {
	req := struct {
		ConversationIDs []string `json:"conversationIDs"`
	}{ConversationIDs: conversationIDs}

	var resp struct {
		Conversations []*model.Conversation `json:"conversations"`
	}
	if err := m.http.Post(ctx, "/conversation/get_conversations", req, &resp); err != nil {
		return nil, fmt.Errorf("get conversations: %w", err)
	}
	return resp.Conversations, nil
}

func (m *DefaultConvMgr) GetAllConversations(ctx context.Context, ownerUserID string) ([]*model.Conversation, error) {
	req := struct {
		OwnerUserID string `json:"ownerUserID"`
	}{OwnerUserID: ownerUserID}

	var resp struct {
		Conversations []*model.Conversation `json:"conversations"`
	}
	if err := m.http.Post(ctx, "/conversation/get_all_conversations", req, &resp); err != nil {
		return nil, fmt.Errorf("get all conversations: %w", err)
	}
	return resp.Conversations, nil
}

func (m *DefaultConvMgr) SetConversations(ctx context.Context, userIDs []string, conversationReq *model.ConversationReq) error {
	req := struct {
		UserIDs         []string               `json:"userIDs"`
		ConversationReq *model.ConversationReq `json:"conversationReq"`
	}{UserIDs: userIDs, ConversationReq: conversationReq}
	if err := m.http.Post(ctx, "/conversation/set_conversations", req, nil); err != nil {
		return fmt.Errorf("set conversations: %w", err)
	}
	if m.bus != nil {
		m.bus.Publish(events.EventConversationCreated, events.Event{
			Type:      events.EventConversationCreated,
			Timestamp: timex.UnixMilli(),
		})
	}
	return nil
}

func (m *DefaultConvMgr) GetFullConversationIDs(ctx context.Context, userID string, idHash uint64) (*model.FullConversationIDs, error) {
	req := struct {
		UserID string `json:"userID"`
		IDHash uint64 `json:"idHash"`
	}{UserID: userID, IDHash: idHash}
	var resp model.FullConversationIDs
	if err := m.http.Post(ctx, "/conversation/get_full_conversation_ids", req, &resp); err != nil {
		return nil, fmt.Errorf("get full conversation ids: %w", err)
	}
	return &resp, nil
}

func (m *DefaultConvMgr) GetIncrementalConversations(ctx context.Context, userID string, versionID string, version uint64) (*model.IncrementalConversations, error) {
	req := struct {
		UserID    string `json:"userID"`
		VersionID string `json:"versionID"`
		Version   uint64 `json:"version"`
	}{UserID: userID, VersionID: versionID, Version: version}
	var resp model.IncrementalConversations
	if err := m.http.Post(ctx, "/conversation/get_incremental_conversations", req, &resp); err != nil {
		return nil, fmt.Errorf("get incremental conversations: %w", err)
	}
	return &resp, nil
}

func (m *DefaultConvMgr) DeleteConversations(ctx context.Context, needDeleteTime int64, conversationIDs []string) error {
	req := struct {
		NeedDeleteTime  int64    `json:"needDeleteTime"`
		ConversationIDs []string `json:"conversationIDs"`
	}{NeedDeleteTime: needDeleteTime, ConversationIDs: conversationIDs}
	if err := m.http.Post(ctx, "/conversation/delete_conversations", req, nil); err != nil {
		return fmt.Errorf("delete conversations: %w", err)
	}
	return nil
}

func (m *DefaultConvMgr) UpdateConversationsByUser(ctx context.Context, userID string, ex string) error {
	req := struct {
		UserID string `json:"userID"`
		Ex     string `json:"ex"`
	}{UserID: userID, Ex: ex}
	if err := m.http.Post(ctx, "/conversation/update_conversations_by_user", req, nil); err != nil {
		return fmt.Errorf("update conversations by user: %w", err)
	}
	return nil
}
