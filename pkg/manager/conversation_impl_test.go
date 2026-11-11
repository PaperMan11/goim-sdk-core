package manager

// 通知会话过滤单测（R13）：n_ 前缀通知会话不暴露给宿主

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PaperMan11/goim-sdk-core/pkg/logger"
	"github.com/PaperMan11/goim-sdk-core/pkg/model"
	"github.com/PaperMan11/goim-sdk-core/pkg/transport"
)

// newConvSrv mock 会话接口：path 为业务路径后缀，data 为响应包装内的业务数据
func newConvSrv(t *testing.T, path string, data interface{}) *DefaultConvMgr {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, path) {
			http.NotFound(w, r)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"code": 0, "msg": "", "data": data})
	}))
	t.Cleanup(srv.Close)
	// BaseURL 需含 /api/v1 前缀（R11 约定），SDK 在其上拼接业务路径
	return NewConvMgr(transport.NewHTTPClient(srv.URL+"/api/v1", transport.NewTokenStore(nil), &logger.NopLogger{}),
		nil, &logger.NopLogger{})
}

func TestIsNotificationConversation(t *testing.T) {
	require.True(t, IsNotificationConversation("n_a_b"))
	require.False(t, IsNotificationConversation("si_a_b"))
	require.False(t, IsNotificationConversation("sg_1"))
}

func TestSetConversationFillsOwnerAndPath(t *testing.T) {
	var gotPath string
	var gotBody struct {
		Conversation *model.Conversation `json:"conversation"`
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		require.NoError(t, json.NewDecoder(r.Body).Decode(&gotBody))
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"code": 0, "msg": "", "data": map[string]interface{}{}})
	}))
	t.Cleanup(srv.Close)
	mgr := NewConvMgr(transport.NewHTTPClient(srv.URL+"/api/v1", transport.NewTokenStore(nil), &logger.NopLogger{}),
		nil, &logger.NopLogger{})

	conv := &model.Conversation{ConversationID: "si_a_b", ConversationType: model.SessionTypeC2C, UserID: "b", IsPinned: true}
	require.NoError(t, mgr.SetConversation(context.Background(), "a", conv))
	require.Equal(t, "/api/v1/conversation/set_conversation", gotPath)
	require.NotNil(t, gotBody.Conversation)
	require.Equal(t, "a", gotBody.Conversation.OwnerUserID)
	require.Equal(t, "si_a_b", gotBody.Conversation.ConversationID)
	require.True(t, gotBody.Conversation.IsPinned)
	require.Empty(t, conv.OwnerUserID, "should not mutate caller conversation")

	require.Error(t, mgr.SetConversation(context.Background(), "a", nil))
}

func TestGetAllConversationsFiltersNotificationConv(t *testing.T) {
	mgr := newConvSrv(t, "/conversation/get_all_conversations", map[string]interface{}{
		"conversations": []*model.Conversation{
			{ConversationID: "si_a_b"},
			{ConversationID: "n_a_b"},
			{ConversationID: "sg_1"},
		},
	})
	convs, err := mgr.GetAllConversations(context.Background(), "a")
	require.NoError(t, err)
	ids := make([]string, 0, len(convs))
	for _, c := range convs {
		ids = append(ids, c.ConversationID)
	}
	require.Equal(t, []string{"si_a_b", "sg_1"}, ids)
}

func TestGetSortedConversationListFiltersNotificationConv(t *testing.T) {
	mgr := newConvSrv(t, "/conversation/get_sorted_conversation_list", model.SortedConversationList{
		ConversationTotal: 3,
		UnreadTotal:       7,
		ConversationElems: []*model.ConversationElem{
			{ConversationID: "si_a_b"},
			{ConversationID: "n_a_b"},
			{ConversationID: "sg_1"},
		},
	})
	list, err := mgr.GetSortedConversationList(context.Background(), "a", nil, nil)
	require.NoError(t, err)
	require.Len(t, list.ConversationElems, 2)
	require.Equal(t, int64(2), list.ConversationTotal) // 剔除通知会话后修正
	require.Equal(t, int64(7), list.UnreadTotal)       // 未读汇总不动（通知会话不参与业务未读）
}

func TestGetIncrementalConversationsFiltersNotificationConv(t *testing.T) {
	mgr := newConvSrv(t, "/conversation/get_incremental_conversations", model.IncrementalConversations{
		VersionID: "v2",
		Version:   2,
		Delete:    []string{"si_gone", "n_a_b"},
		Insert: []*model.Conversation{
			{ConversationID: "si_new"},
			{ConversationID: "n_x_y"},
		},
		Update: []*model.Conversation{
			{ConversationID: "si_upd"},
			{ConversationID: "n_u_v"},
		},
	})
	incr, err := mgr.GetIncrementalConversations(context.Background(), "a", "v1", 1)
	require.NoError(t, err)
	require.Equal(t, "v2", incr.VersionID) // 游标按服务端响应推进，不受过滤影响
	require.Len(t, incr.Insert, 1)
	require.Equal(t, "si_new", incr.Insert[0].ConversationID)
	require.Len(t, incr.Update, 1)
	require.Equal(t, "si_upd", incr.Update[0].ConversationID)
	require.Equal(t, []string{"si_gone", "n_a_b"}, incr.Delete) // 服务端删除指令原样保留
}
