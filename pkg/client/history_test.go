package client

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PaperMan11/goim-sdk-core/pkg/logger"
	"github.com/PaperMan11/goim-sdk-core/pkg/manager"
	"github.com/PaperMan11/goim-sdk-core/pkg/storage"
	"github.com/PaperMan11/goim-sdk-core/pkg/transport"
)

// TestGetHistoryMsgsCachesLocally 分页历史拉取后应写入本地消息缓存（重装/冷启动回填）。
func TestGetHistoryMsgsCachesLocally(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		payload := map[string]interface{}{
			"msgs": map[string]interface{}{
				"si_x": map[string]interface{}{
					"Msgs": []map[string]interface{}{
						{"clientMsgID": "c2", "seq": 2, "content": "m2"},
						{"clientMsgID": "c1", "seq": 1, "content": "m1"},
					},
					"isEnd": true,
				},
			},
		}
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"code": 0, "msg": "", "data": payload})
	}))
	defer srv.Close()

	stor := storage.NewMemoryStorage()
	c := &DefaultClient{storage: stor, userID: "u1", logger: &logger.NopLogger{}}
	c.msg = manager.NewMsgMgr(transport.NewHTTPClient(srv.URL+"/api/v1", transport.NewTokenStore(nil), c.logger),
		nil, nil, c.logger)
	c.msg.SetSenderID("u1")

	page, err := c.GetHistoryMsgs(context.Background(), "si_x", 0, 2)
	require.NoError(t, err)
	require.Len(t, page.Msgs, 2)

	msgs, err := stor.ListMsgsByConversation("u1", "si_x")
	require.NoError(t, err)
	require.Len(t, msgs, 2, "拉取结果应写入本地缓存")

	// 未登录时跳过写缓存，但仍应返回服务端结果
	anon := &DefaultClient{storage: stor, logger: &logger.NopLogger{}}
	anon.msg = c.msg
	_, err = anon.GetHistoryMsgs(context.Background(), "si_x", 0, 2)
	require.NoError(t, err)
}
