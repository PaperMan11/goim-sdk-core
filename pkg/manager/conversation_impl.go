package manager

import (
	"context"
	"fmt"
	"strings"

	"github.com/PaperMan11/goim-sdk-core/pkg/logger"
	"github.com/PaperMan11/goim-sdk-core/pkg/model"
	"github.com/PaperMan11/goim-sdk-core/pkg/transport"
	"github.com/PaperMan11/goim-sdk-core/pkg/utils/eventbus"
)

// NotificationConversationIDPrefix 服务端系统通知（ContentType>=1000）的会话归属前缀：
// 通知消息不进业务会话（si_/群会话），而是归属到 n_<userA>_<userB> 独立通知会话。
// 通知会话不在 SeqSync 游标体系内、对宿主无业务意义（测试计划 R13），
// 列表与同步入口统一过滤，不暴露给宿主；显式点名查询（GetConversation 等）不拦截
const NotificationConversationIDPrefix = "n_"

// IsNotificationConversation 是否通知会话（R13）
func IsNotificationConversation(conversationID string) bool {
	return strings.HasPrefix(conversationID, NotificationConversationIDPrefix)
}

// filterNotificationConvs 剔除通知会话（同步增量/批量查询出口）
func filterNotificationConvs(convs []*model.Conversation) []*model.Conversation {
	filtered := make([]*model.Conversation, 0, len(convs))
	for _, c := range convs {
		if c != nil && !IsNotificationConversation(c.ConversationID) {
			filtered = append(filtered, c)
		}
	}
	return filtered
}

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
	filtered := make([]*model.ConversationElem, 0, len(resp.ConversationElems))
	for _, e := range resp.ConversationElems {
		if e != nil && !IsNotificationConversation(e.ConversationID) {
			filtered = append(filtered, e)
		}
	}
	// 剔除通知会话后同步修正总数（未读汇总不动：通知会话不参与业务未读）
	resp.ConversationTotal -= int64(len(resp.ConversationElems) - len(filtered))
	resp.ConversationElems = filtered
	return &resp, nil
}

func (m *DefaultConvMgr) GetConversation(ctx context.Context, ownerUserID string, conversationID string) (*model.Conversation, error) {
	// 服务端 GetConversationReq{conversationID, ownerUserID}
	req := struct {
		ConversationID string `json:"conversationID"`
		OwnerUserID    string `json:"ownerUserID"`
	}{ConversationID: conversationID, OwnerUserID: ownerUserID}
	// 服务端 GetConversationResp：{conversation: {...}}
	var resp struct {
		Conversation *model.Conversation `json:"conversation"`
	}
	if err := m.http.Post(ctx, "/conversation/get_conversation", req, &resp); err != nil {
		return nil, fmt.Errorf("get conversation: %w", err)
	}
	return resp.Conversation, nil
}

func (m *DefaultConvMgr) GetConversations(ctx context.Context, ownerUserID string, conversationIDs []string) ([]*model.Conversation, error) {
	// 服务端 GetConversationsReq{ownerUserID, conversationIDs}
	req := struct {
		OwnerUserID     string   `json:"ownerUserID"`
		ConversationIDs []string `json:"conversationIDs"`
	}{OwnerUserID: ownerUserID, ConversationIDs: conversationIDs}

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
	return filterNotificationConvs(resp.Conversations), nil
}

func (m *DefaultConvMgr) SetConversations(ctx context.Context, userIDs []string, conversationReq *model.ConversationReq) error {
	// 服务端 SetConversationsReq{userIDs, conversation}
	req := struct {
		UserIDs      []string               `json:"userIDs"`
		Conversation *model.ConversationReq `json:"conversation"`
	}{UserIDs: userIDs, Conversation: conversationReq}
	if err := m.http.Post(ctx, "/conversation/set_conversations", req, nil); err != nil {
		return fmt.Errorf("set conversations: %w", err)
	}
	// 服务端处理 SetConversations 后会推送 ConversationChangeNotification（1300），
	// 由 client 层 WS dispatch 统一 Publish(EventConversationUpdate) 并触发会话增量同步，
	// SDK 侧无需再本地发布。
	// if m.bus != nil {
	// 	m.bus.Publish(events.EventConversationCreated, events.Event{
	// 		Type:      events.EventConversationCreated,
	// 		Timestamp: timex.UnixMilli(),
	// 	})
	// }
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
	// 过滤通知会话；versionID/Version 不受影响（游标按服务端响应推进，被剔除
	// 的通知会话本就不落本地库，剔除后无一致性影响），Delete 列表保留服务端原样
	resp.Insert = filterNotificationConvs(resp.Insert)
	resp.Update = filterNotificationConvs(resp.Update)
	return &resp, nil
}

func (m *DefaultConvMgr) DeleteConversations(ctx context.Context, ownerUserID string, needDeleteTime int64, conversationIDs []string) error {
	// 服务端 DeleteConversationsReq{ownerUserID, needDeleteTime, conversationIDs}
	req := struct {
		OwnerUserID     string   `json:"ownerUserID"`
		NeedDeleteTime  int64    `json:"needDeleteTime"`
		ConversationIDs []string `json:"conversationIDs"`
	}{OwnerUserID: ownerUserID, NeedDeleteTime: needDeleteTime, ConversationIDs: conversationIDs}
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
